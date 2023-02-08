use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    rc::{Rc, Weak},
};

use ascii_tree::{write_tree, Tree};

use crate::physical::{
    tabular::operations::{
        triescan_append::AppendInstruction, triescan_join::JoinBinding,
        triescan_select::SelectEqualClasses, ValueAssignment,
    },
    util::Reordering,
};

use super::database::{TableId, TableKeyType};

/// Wraps [`ExecutionNode`] into a `Rc<RefCell<_>>`
#[derive(Debug)]
pub struct ExecutionNodeOwned<TableKey: TableKeyType>(pub Rc<RefCell<ExecutionNode<TableKey>>>);

impl<TableKey: TableKeyType> ExecutionNodeOwned<TableKey> {
    /// Create new [`ExecutionNodeOwned`]
    pub fn new(node: ExecutionNode<TableKey>) -> Self {
        Self(Rc::new(RefCell::new(node)))
    }

    /// Return a [`ExecutionNodeRef`] pointing to this node
    pub fn get_ref(&self) -> ExecutionNodeRef<TableKey> {
        ExecutionNodeRef(Rc::downgrade(&self.0))
    }
}

/// Wraps [`ExecutionNode`] into a `Weak<RefCell<_>>`
#[derive(Debug, Clone)]
pub struct ExecutionNodeRef<TableKey: TableKeyType>(pub Weak<RefCell<ExecutionNode<TableKey>>>);

impl<TableKey: TableKeyType> ExecutionNodeRef<TableKey> {
    /// Add a sub node to a join or union node
    pub fn add_subnode(&mut self, subnode: ExecutionNodeRef<TableKey>) {
        let self_rc = self
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
        let node_ref = &mut *self_rc.as_ref().borrow_mut();

        match node_ref {
            ExecutionNode::Join(subnodes, _) => subnodes.push(subnode),
            ExecutionNode::Union(subnodes) => subnodes.push(subnode),
            ExecutionNode::FetchTable(_) | ExecutionNode::FetchTemp(_) => {
                panic!("Can't add subnode to a leaf node")
            }
            _ => {
                panic!("Can only add subnodes to operations which can have arbitrary many of them")
            }
        }
    }
}

/// Represents a database operation that should be performed
#[derive(Debug, Clone)]
pub enum ExecutionNode<TableKey: TableKeyType> {
    /// Fetch table by key.
    FetchTable(TableKey),
    /// Fetch temporary table with the (temporary) id.
    FetchTemp(TableId),
    /// Join operation.
    Join(Vec<ExecutionNodeRef<TableKey>>, JoinBinding),
    /// Union operation.
    Union(Vec<ExecutionNodeRef<TableKey>>),
    /// Table difference operation.
    Minus(ExecutionNodeRef<TableKey>, ExecutionNodeRef<TableKey>),
    /// Table project operation; can only be applied to a [`FetchTable`] or [`FetchTemp`] node.
    Project(ExecutionNodeRef<TableKey>, Reordering),
    /// Only leave entries in that have a certain value.
    SelectValue(ExecutionNodeRef<TableKey>, Vec<ValueAssignment>),
    /// Only leave entries in that contain equal values in certain columns.
    SelectEqual(ExecutionNodeRef<TableKey>, SelectEqualClasses),
    /// Append certain columns to the trie.
    AppendColumns(ExecutionNodeRef<TableKey>, Vec<Vec<AppendInstruction>>),
    /// Append (the given number of) columns containing fresh nulls.
    AppendNulls(ExecutionNodeRef<TableKey>, usize),
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently.
#[derive(Debug, Clone)]
pub enum ExecutionResult<TableKey: TableKeyType> {
    /// Temporary table with the id.
    Temp(TableId),
    /// Permanent table tih the table key.
    Save(TableKey),
}

/// Represents the plan for calculating a table
pub struct ExecutionTree<TableKey: TableKeyType> {
    /// All the nodes in the tree.
    nodes: Vec<ExecutionNodeOwned<TableKey>>,
    /// Root of the operation tree.
    root: Option<ExecutionNodeRef<TableKey>>,
    /// How to save the resulting table.
    result: ExecutionResult<TableKey>,
    /// Name which identifies this operation for timing.
    name: String,
}

impl<TableKey: TableKeyType> Debug for ExecutionTree<TableKey> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_tree(f, &self.ascii_tree())
    }
}

/// Public interface for [`ExecutionTree`]
impl<TableKey: TableKeyType> ExecutionTree<TableKey> {
    /// Create new [`ExecutionTree`]
    pub fn new(name: String, result: ExecutionResult<TableKey>) -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
            result,
            name,
        }
    }

    /// Return the result of this operation
    pub fn result(&self) -> &ExecutionResult<TableKey> {
        &self.result
    }

    /// Return the name of this tree.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Return the root of the trie.
    pub fn root(&self) -> Option<ExecutionNodeRef<TableKey>> {
        self.root.clone()
    }

    /// Set the root node of the tree.
    pub fn set_root(&mut self, root: ExecutionNodeRef<TableKey>) {
        self.root = Some(root);
    }

    /// Set the result of the computation tree.
    pub fn set_result(&mut self, result: ExecutionResult<TableKey>) {
        self.result = result;
    }

    /// Return an iterator containing a mutable reference to all FetchTable nodes.
    pub fn all_fetched_tables(
        &mut self,
    ) -> impl Iterator<Item = &mut ExecutionNodeOwned<TableKey>> {
        self.nodes
            .iter_mut()
            .filter(|n| matches!(&*n.0.as_ref().borrow(), ExecutionNode::FetchTable(_)))
    }

    /// Return all table keys that have been fetched
    pub fn all_fetched_keys(&self) -> HashSet<TableKey> {
        let mut result = HashSet::<TableKey>::new();

        for node in &self.nodes {
            let node_ref = &*node.0.as_ref().borrow();

            if let ExecutionNode::FetchTable(key) = node_ref {
                result.insert(key.clone());
            }
        }

        result
    }

    /// Push new node to list of all nodes and returns a reference.
    fn push_and_return_ref(&mut self, node: ExecutionNode<TableKey>) -> ExecutionNodeRef<TableKey> {
        self.nodes.push(ExecutionNodeOwned::new(node));
        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table.
    pub fn fetch_table(&mut self, key: TableKey) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::FetchTable(key);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for fetching a temporary table.
    pub fn fetch_temp(&mut self, id: TableId) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::FetchTemp(id);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn join_empty(&mut self, binding: JoinBinding) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Join(Vec::new(), binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn join(
        &mut self,
        subtables: Vec<ExecutionNodeRef<TableKey>>,
        binding: JoinBinding,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Join(subtables, binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for the union of several tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn union_empty(&mut self) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Union(Vec::new());
        self.nodes.push(ExecutionNodeOwned::new(new_node));

        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn union(
        &mut self,
        subtables: Vec<ExecutionNodeRef<TableKey>>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Union(subtables);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for subtracting one table from another.
    pub fn minus(
        &mut self,
        left: ExecutionNodeRef<TableKey>,
        right: ExecutionNodeRef<TableKey>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Minus(left, right);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for applying project to a table.
    pub fn project(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        reorder: Reordering,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Project(subnode, reorder);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricing a column to a certain value.
    pub fn select_value(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        assigments: Vec<ValueAssignment>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::SelectValue(subnode, assigments);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricting a column to values of certain other columns.
    pub fn select_equal(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        eq_classes: SelectEqualClasses,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::SelectEqual(subnode, eq_classes);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for appending columns to a trie.
    pub fn append_columns(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        instructions: Vec<Vec<AppendInstruction>>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::AppendColumns(subnode, instructions);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for appending null-columns to a trie.
    pub fn append_nulls(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        num_nulls: usize,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::AppendNulls(subnode, num_nulls);
        self.push_and_return_ref(new_node)
    }

    fn ascii_tree_recursive(node: ExecutionNodeRef<TableKey>) -> Tree {
        let node_rc = node
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
        let node_ref = &*node_rc.as_ref().borrow();

        match node_ref {
            ExecutionNode::FetchTable(key) => {
                Tree::Leaf(vec![format!("Permanent Table: {:?}", key)])
            }
            ExecutionNode::FetchTemp(id) => Tree::Leaf(vec![format!("Temporary Table: {:}", id)]),
            ExecutionNode::Join(subnodes, bindings) => {
                let subtrees = subnodes
                    .iter()
                    .map(|n| Self::ascii_tree_recursive(n.clone()))
                    .collect();

                Tree::Node(format!("Join {:?}", bindings), subtrees)
            }
            ExecutionNode::Union(subnodes) => {
                let subtrees = subnodes
                    .iter()
                    .map(|n| Self::ascii_tree_recursive(n.clone()))
                    .collect();

                Tree::Node(String::from("Union"), subtrees)
            }
            ExecutionNode::Minus(node_left, node_right) => {
                let subtree_left = Self::ascii_tree_recursive(node_left.clone());
                let subtree_right = Self::ascii_tree_recursive(node_right.clone());

                Tree::Node(String::from("Minus"), vec![subtree_left, subtree_right])
            }
            ExecutionNode::Project(subnode, reorder) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Project {:?}", reorder), vec![subtree])
            }
            ExecutionNode::SelectValue(subnode, assignments) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Value {:?}", assignments), vec![subtree])
            }
            ExecutionNode::SelectEqual(subnode, classes) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Equal {:?}", classes), vec![subtree])
            }
            ExecutionNode::AppendColumns(subnode, instructions) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Columns {:?}", instructions), vec![subtree])
            }
            ExecutionNode::AppendNulls(subnode, num_nulls) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Nulls {}", num_nulls), vec![subtree])
            }
        }
    }

    /// Return an ascii tree representation of the [`ExecutionTree`]
    pub fn ascii_tree(&self) -> Tree {
        if let Some(root) = self.root() {
            Self::ascii_tree_recursive(root)
        } else {
            Tree::Leaf(vec![])
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum RemoveTable<TableKey: TableKeyType> {
    Temp(TableId),
    Permanent(TableKey),
}

/// Functionality for optimizing an [`ExecutionTree`]
impl<TableKey: TableKeyType> ExecutionTree<TableKey> {
    /// Implements the functionalily for `simplify` by recusively traversing the tree.
    fn simplify_recursive(
        new_tree: &mut ExecutionTree<TableKey>,
        node: ExecutionNodeRef<TableKey>,
        removed_tables: &HashSet<RemoveTable<TableKey>>,
    ) -> Option<ExecutionNodeRef<TableKey>> {
        let node_rc = node
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
        let node_ref = &*node_rc.as_ref().borrow();

        match node_ref {
            ExecutionNode::FetchTable(key) => {
                let remove = RemoveTable::Permanent(key.clone());

                if removed_tables.contains(&remove) {
                    None
                } else {
                    Some(new_tree.fetch_table(key.clone()))
                }
            }
            ExecutionNode::FetchTemp(id) => {
                let remove = RemoveTable::Temp(*id);

                if removed_tables.contains(&remove) {
                    None
                } else {
                    Some(new_tree.fetch_temp(*id))
                }
            }
            ExecutionNode::Join(subnodes, binding) => {
                let mut simplified_nodes =
                    Vec::<ExecutionNodeRef<TableKey>>::with_capacity(subnodes.len());
                for subnode in subnodes {
                    let simplified_opt =
                        Self::simplify_recursive(new_tree, subnode.clone(), removed_tables);

                    if let Some(simplified) = simplified_opt {
                        simplified_nodes.push(simplified)
                    } else {
                        // If subtables contain an empty table, then the join is empty
                        return None;
                    }
                }

                if simplified_nodes.len() == 1 {
                    return Some(simplified_nodes.remove(0));
                }

                Some(new_tree.join(simplified_nodes, binding.clone()))
            }
            ExecutionNode::Union(subnodes) => {
                let mut simplified_nodes =
                    Vec::<ExecutionNodeRef<TableKey>>::with_capacity(subnodes.len());
                for subnode in subnodes {
                    let simplified_opt =
                        Self::simplify_recursive(new_tree, subnode.clone(), removed_tables);

                    if let Some(simplified) = simplified_opt {
                        simplified_nodes.push(simplified)
                    }
                }

                if simplified_nodes.is_empty() {
                    return None;
                }

                if simplified_nodes.len() == 1 {
                    return Some(simplified_nodes.remove(0));
                }

                Some(new_tree.union(simplified_nodes))
            }
            ExecutionNode::Minus(left, right) => {
                let simplified_left_opt =
                    Self::simplify_recursive(new_tree, left.clone(), removed_tables);
                let simplified_right_opt =
                    Self::simplify_recursive(new_tree, right.clone(), removed_tables);

                if let Some(simplified_left) = simplified_left_opt {
                    if let Some(simplififed_right) = simplified_right_opt {
                        return Some(new_tree.minus(simplified_left, simplififed_right));
                    } else {
                        return Some(simplified_left);
                    }
                }

                None
            }
            ExecutionNode::Project(subnode, reorder) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if reorder.is_default() {
                    Some(simplified)
                } else {
                    Some(new_tree.project(simplified, reorder.clone()))
                }
            }
            ExecutionNode::SelectValue(subnode, assignments) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if assignments.is_empty() {
                    Some(simplified)
                } else {
                    Some(new_tree.select_value(simplified, assignments.clone()))
                }
            }
            ExecutionNode::SelectEqual(subnode, classes) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if classes.is_empty() {
                    Some(simplified)
                } else {
                    Some(new_tree.select_equal(simplified, classes.clone()))
                }
            }
            ExecutionNode::AppendColumns(subnode, instructions) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if instructions.iter().all(|i| i.is_empty()) {
                    Some(simplified)
                } else {
                    Some(new_tree.append_columns(simplified, instructions.clone()))
                }
            }
            ExecutionNode::AppendNulls(subnode, num_nulls) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if *num_nulls == 0 {
                    Some(simplified)
                } else {
                    Some(new_tree.append_nulls(simplified, *num_nulls))
                }
            }
        }
    }

    /// Builds a new [`ExecutionTree`] which does not include superfluous operations,
    /// like, e.g., performing a join over one subtable.
    /// Will also exclude the supplied set of temporary tables.
    fn simplify(&self, removed_temp_ids: &HashSet<RemoveTable<TableKey>>) -> Self {
        let mut new_tree = ExecutionTree::<TableKey>::new(self.name.clone(), self.result.clone());

        if let Some(old_root) = self.root() {
            let new_root_opt = Self::simplify_recursive(&mut new_tree, old_root, removed_temp_ids);
            if let Some(new_root) = new_root_opt {
                new_tree.set_root(new_root);
            }
        }

        new_tree
    }

    /// Replace temporary ids in the tree as well as in the result with the given mapping.
    pub fn replace_temp_ids(&mut self, map: &HashMap<TableId, ExecutionNode<TableKey>>) {
        if let ExecutionResult::Temp(id_saved) = self.result {
            if let Some(node) = map.get(&id_saved) {
                match node {
                    ExecutionNode::FetchTable(key_loaded) => {
                        self.set_result(ExecutionResult::Save(key_loaded.clone()));
                    }
                    ExecutionNode::FetchTemp(id_loaded) => {
                        self.set_result(ExecutionResult::Temp(*id_loaded));
                    }
                    _ => {}
                }
            }
        }

        for node in &mut self.nodes {
            let node_unpacked = &mut *node.0.as_ref().borrow_mut();
            if let ExecutionNode::FetchTemp(id) = node_unpacked {
                if let Some(new_node) = map.get(id) {
                    *node_unpacked = new_node.clone();
                }
            }
        }
    }
}

/// A series of execution plans
/// Usually contains the information necessary for evaluating one rule
#[derive(Debug, Default)]
pub struct ExecutionPlan<TableKey: TableKeyType> {
    /// The individual steps that will be executed
    /// Each step will result in either a temporary or a permanent table
    pub trees: Vec<ExecutionTree<TableKey>>,
}

impl<TableKey: TableKeyType> ExecutionPlan<TableKey> {
    /// Create new [`ExecutionPlan`].
    pub fn new() -> Self {
        Self { trees: Vec::new() }
    }

    /// Append new [`ExecutionTree`] to the plan.
    pub fn push(&mut self, tree: ExecutionTree<TableKey>) {
        self.trees.push(tree);
    }

    /// Append a list of [`ExecutionTree`] to the plan.
    pub fn append(&mut self, mut trees: Vec<ExecutionTree<TableKey>>) {
        self.trees.append(&mut trees);
    }

    /// Adds a new entry to the given map which indicates that the given [`TableId`] shall be replaced
    /// with the given [`ExecutionNode`].
    /// Renaming of temporary tables to other temporary tables will be applied exhaustively.
    /// I.e. if A -> B and B -> C then A -> C.
    fn update_renaming_map(
        map: &mut HashMap<TableId, ExecutionNode<TableKey>>,
        from: TableId,
        to: ExecutionNode<TableKey>,
    ) {
        let actual_to = if let ExecutionNode::FetchTemp(to_id) = to {
            if let Some(actual_node) = map.get(&to_id) {
                actual_node
            } else {
                &to
            }
        } else {
            &to
        }
        .clone();

        if let Entry::Vacant(vacant) = map.entry(from) {
            vacant.insert(actual_to);
        }
    }

    /// Simplifies the current [`ExecutionPlan`].
    /// This includes:
    ///     * Removing superfluous operations like empty unions or default projects
    ///     * Removing computations that would result in an empty table
    ///     * Removing computations that would result in an unused temporary table
    ///     * Removing temporary tables that are the same as other temporary tables
    pub fn simplify(&mut self) {
        let mut removed_tables = HashSet::<RemoveTable<TableKey>>::new();

        let mut used_temp_ids = HashSet::<TableId>::new();
        let mut used_temp_in = HashMap::<TableId, HashSet<TableId>>::new();

        // First, simplify all the trees individually and remove fetch nodes which load an empty temporary table.
        // We also remember which temporary tables are used to compute permanent tables
        // and also which temporary tables are used to compute which other temporary tables.
        for tree in &mut self.trees {
            *tree = tree.simplify(&removed_tables);

            if tree.root().is_some() {
                for fetch_instruction in &tree.nodes {
                    let node_unpacked = &*fetch_instruction.0.as_ref().borrow_mut();
                    if let ExecutionNode::FetchTemp(used_id) = node_unpacked {
                        match tree.result() {
                            ExecutionResult::Temp(saved_id) => {
                                let used_set =
                                    used_temp_in.entry(*used_id).or_insert(HashSet::new());
                                used_set.insert(*saved_id);
                            }
                            ExecutionResult::Save(_) => {
                                used_temp_ids.insert(*used_id);
                            }
                        }
                    }
                }
            } else {
                let removed_table = match tree.result() {
                    ExecutionResult::Temp(id) => RemoveTable::Temp(*id),
                    ExecutionResult::Save(key) => RemoveTable::Permanent(key.clone()),
                };

                removed_tables.insert(removed_table);
            }
        }

        // Keep only those trees which are not empty
        // and are actually used for computing a permanent table.
        // Also remove temporary tables which are the same as other tables and remember those.
        let mut renamed_temp = HashMap::<TableId, ExecutionNode<TableKey>>::new();
        self.trees.retain(|t| {
            if let Some(root) = t.root() {
                let node = root
                    .0
                    .upgrade()
                    .expect("Referenced execution node has been deleted");
                if let ExecutionResult::Temp(id) = t.result() {
                    let used_directly = used_temp_ids.contains(id);
                    let used_indirectly = if let Some(used_in) = used_temp_in.get(id) {
                        !used_temp_ids.is_disjoint(used_in)
                    } else {
                        false
                    };

                    if !used_directly && !used_indirectly {
                        return false;
                    }
                }

                let node_unpacked = &*node.as_ref().borrow();
                match node_unpacked {
                    ExecutionNode::FetchTemp(id_loaded) => match t.result() {
                        ExecutionResult::Temp(id_saved) => {
                            Self::update_renaming_map(
                                &mut renamed_temp,
                                *id_saved,
                                ExecutionNode::FetchTemp(*id_loaded),
                            );

                            return false;
                        }
                        ExecutionResult::Save(_key_saved) => {
                            return true;
                        }
                    },
                    ExecutionNode::FetchTable(key_loaded) => {
                        if let ExecutionResult::Temp(id_saved) = t.result() {
                            Self::update_renaming_map(
                                &mut renamed_temp,
                                *id_saved,
                                ExecutionNode::FetchTable(key_loaded.clone()),
                            );

                            return false;
                        }
                    }
                    _ => {}
                }

                true
            } else {
                false
            }
        });

        // Apply the above renaming to the remaining trees
        for tree in &mut self.trees {
            tree.replace_temp_ids(&renamed_temp);
        }
    }

    /// Return all table keys that have been fetched
    pub fn all_fetched_keys(&self) -> HashSet<TableKey> {
        let mut result = HashSet::<TableKey>::new();

        for tree in &self.trees {
            let tree_keys = tree.all_fetched_keys();

            result = result.union(&tree_keys).cloned().collect();
        }

        result
    }
}

#[cfg(test)]
mod test {
    use crate::physical::management::database::{TableId, TableKeyType};

    use super::{ExecutionNodeRef, ExecutionPlan, ExecutionResult, ExecutionTree};

    type MyTableKey = usize;
    impl TableKeyType for MyTableKey {}

    #[test]
    fn general_use() {
        let mut body_tree =
            ExecutionTree::<MyTableKey>::new("Test".to_string(), ExecutionResult::Temp(0));

        let mut seminaive_union_node = body_tree.union_empty();
        body_tree.set_root(seminaive_union_node.clone());

        for _body_index in 0..2 {
            let mut join_node = body_tree.join_empty(vec![vec![0, 1], vec![1, 2]]);
            let tables: Vec<Vec<TableId>> = vec![vec![0, 1], vec![0, 4, 7, 12]];
            for table_ids in tables {
                let union_subnodes: Vec<ExecutionNodeRef<MyTableKey>> = table_ids
                    .iter()
                    .map(|id| body_tree.fetch_table(*id))
                    .collect();
                let union_node = body_tree.union(union_subnodes);
                join_node.add_subnode(union_node);
            }

            seminaive_union_node.add_subnode(join_node);
        }

        let mut final_plan = ExecutionPlan::new();
        final_plan.push(body_tree);
    }
}
