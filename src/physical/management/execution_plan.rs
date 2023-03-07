use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::Debug,
    rc::{Rc, Weak},
};

use ascii_tree::{write_tree, Tree};
use linked_hash_map::LinkedHashMap;

use crate::physical::{
    tabular::operations::{
        triescan_append::AppendInstruction, triescan_join::JoinBinding,
        triescan_select::SelectEqualClasses, ValueAssignment,
    },
    util::Reordering,
};

use super::{column_order::ColumnOrder, database::TableId};

/// Wraps [`ExecutionNode`] into a `Rc<RefCell<_>>`
#[derive(Debug)]
pub struct ExecutionNodeOwned(pub Rc<RefCell<ExecutionNode>>);

impl ExecutionNodeOwned {
    /// Create new [`ExecutionNodeOwned`]
    pub fn new(node: ExecutionNode) -> Self {
        Self(Rc::new(RefCell::new(node)))
    }

    /// Return a [`ExecutionNodeRef`] pointing to this node
    pub fn get_ref(&self) -> ExecutionNodeRef {
        ExecutionNodeRef(Rc::downgrade(&self.0))
    }
}

/// Wraps [`ExecutionNode`] into a `Weak<RefCell<_>>`
#[derive(Debug, Clone)]
pub struct ExecutionNodeRef(pub Weak<RefCell<ExecutionNode>>);

impl ExecutionNodeRef {
    /// Return an referenced counted cell of an [`ExecutionNode`].
    pub fn get_rc(&self) -> Rc<RefCell<ExecutionNode>> {
        self.0
            .upgrade()
            .expect("Referenced execution node has been deleted")
    }
}

impl ExecutionNodeRef {
    /// Add a sub node to a join or union node
    pub fn add_subnode(&mut self, subnode: ExecutionNodeRef) {
        let node_rc = subnode.get_rc();
        let node_ref = &mut *node_rc.borrow_mut();

        match node_ref {
            ExecutionNode::Join(subnodes, _) => subnodes.push(subnode),
            ExecutionNode::Union(subnodes) => subnodes.push(subnode),
            ExecutionNode::FetchExisting(_, _) | ExecutionNode::FetchNew(_) => {
                panic!("Can't add subnode to a leaf node.")
            }
            _ => {
                panic!("Can only add subnodes to operations which can have arbitrary many of them.")
            }
        }
    }
}

/// Represents a database operation that should be performed
#[derive(Debug, Clone)]
pub enum ExecutionNode {
    /// Fetch a table that is already present in the database instance.
    FetchExisting(TableId, ColumnOrder),
    /// Fetch a table that is computed as part of the [`ExecutionPlan`] this tree is part of.
    FetchNew(usize),
    /// Join operation.
    Join(Vec<ExecutionNodeRef>, JoinBinding),
    /// Union operation.
    Union(Vec<ExecutionNodeRef>),
    /// Table difference operation.
    Minus(ExecutionNodeRef, ExecutionNodeRef),
    /// Table project operation; can only be applied to a [`FetchTable`] or [`FetchTemp`] node.
    Project(ExecutionNodeRef, Reordering),
    /// Only leave entries in that have a certain value.
    SelectValue(ExecutionNodeRef, Vec<ValueAssignment>),
    /// Only leave entries in that contain equal values in certain columns.
    SelectEqual(ExecutionNodeRef, SelectEqualClasses),
    /// Append certain columns to the trie.
    AppendColumns(ExecutionNodeRef, Vec<Vec<AppendInstruction>>),
    /// Append (the given number of) columns containing fresh nulls.
    AppendNulls(ExecutionNodeRef, usize),
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently.
#[derive(Debug, Clone)]
pub(super) enum ExecutionResult {
    /// Table will be dropped after the [`ExecutionPlan`] is finished.
    Temporary,
    /// Table will be saved permanently in the database instance.
    Permanent(ColumnOrder, String),
}

/// Represents the plan for calculating a table
pub struct ExecutionTree {
    /// All the nodes in the tree.
    nodes: Vec<ExecutionNodeOwned>,
    /// Root of the operation tree.
    root: Option<ExecutionNodeRef>,
    /// How to save the resulting table.
    result: ExecutionResult,
    /// Name which identifies this operation, e.g., for logging and timing.
    name: String,
}

impl Debug for ExecutionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_tree(f, &self.ascii_tree())
    }
}

/// Public interface for [`ExecutionTree`]
impl ExecutionTree {
    /// Create a new [`ExecutionTree`].
    fn new(tree_name: &str, result: ExecutionResult) -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
            result,
            name: String::from(tree_name),
        }
    }

    /// Create a new [`ExecutionTree`] that will result in a temporary table.
    pub fn new_temporary(tree_name: &str) -> Self {
        Self::new(tree_name, ExecutionResult::Temporary)
    }

    /// Crate a new [`ExecutionTree`] that will result in a permanent table.
    pub fn new_permanent(tree_name: &str, table_name: &str) -> Self {
        Self::new_permanent_reordered(tree_name, table_name, ColumnOrder::default())
    }

    /// Create a new [`ExecutionTree`] that will result in a permanent table with an alternative [`ColumnOrder`]
    pub fn new_permanent_reordered(tree_name: &str, table_name: &str, order: ColumnOrder) -> Self {
        Self::new(
            tree_name,
            ExecutionResult::Permanent(order, String::from(table_name)),
        )
    }

    /// Returns the [`ExecutionResult`] of this operation.
    /// It declares what should happen with the output, once computed.
    pub(super) fn result(&self) -> &ExecutionResult {
        &self.result
    }

    /// Return the name of this tree.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the root of the trie.
    pub fn root(&self) -> Option<ExecutionNodeRef> {
        self.root.clone()
    }

    /// Set the root node of the tree.
    pub fn set_root(&mut self, root: ExecutionNodeRef) {
        self.root = Some(root);
    }

    /// Push new node to list of all nodes and returns a reference.
    fn push_and_return_ref(&mut self, node: ExecutionNode) -> ExecutionNodeRef {
        self.nodes.push(ExecutionNodeOwned::new(node));
        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table.
    pub fn fetch_existing(&mut self, id: TableId) -> ExecutionNodeRef {
        let new_node = ExecutionNode::FetchExisting(id, ColumnOrder::default());
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table.
    pub fn fetch_existing_reordered(
        &mut self,
        id: TableId,
        order: ColumnOrder,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::FetchExisting(id, order);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for fetching a temporary table.
    pub fn fetch_new(&mut self, index: usize) -> ExecutionNodeRef {
        let new_node = ExecutionNode::FetchNew(index);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn join_empty(&mut self, binding: JoinBinding) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Join(Vec::new(), binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn join(
        &mut self,
        subtables: Vec<ExecutionNodeRef>,
        binding: JoinBinding,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Join(subtables, binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for the union of several tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn union_empty(&mut self) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Union(Vec::new());
        self.nodes.push(ExecutionNodeOwned::new(new_node));

        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn union(&mut self, subtables: Vec<ExecutionNodeRef>) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Union(subtables);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for subtracting one table from another.
    pub fn minus(&mut self, left: ExecutionNodeRef, right: ExecutionNodeRef) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Minus(left, right);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for applying project to a table.
    pub fn project(&mut self, subnode: ExecutionNodeRef, reorder: Reordering) -> ExecutionNodeRef {
        let new_node = ExecutionNode::Project(subnode, reorder);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricing a column to a certain value.
    pub fn select_value(
        &mut self,
        subnode: ExecutionNodeRef,
        assigments: Vec<ValueAssignment>,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::SelectValue(subnode, assigments);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricting a column to values of certain other columns.
    pub fn select_equal(
        &mut self,
        subnode: ExecutionNodeRef,
        eq_classes: SelectEqualClasses,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::SelectEqual(subnode, eq_classes);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for appending columns to a trie.
    pub fn append_columns(
        &mut self,
        subnode: ExecutionNodeRef,
        instructions: Vec<Vec<AppendInstruction>>,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::AppendColumns(subnode, instructions);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for appending null-columns to a trie.
    pub fn append_nulls(
        &mut self,
        subnode: ExecutionNodeRef,
        num_nulls: usize,
    ) -> ExecutionNodeRef {
        let new_node = ExecutionNode::AppendNulls(subnode, num_nulls);
        self.push_and_return_ref(new_node)
    }

    fn ascii_tree_recursive(node: ExecutionNodeRef) -> Tree {
        let node_rc = node.get_rc();
        let node_ref = &*node_rc.borrow();

        match node_ref {
            ExecutionNode::FetchExisting(id, order) => {
                Tree::Leaf(vec![format!("Permanent Table: {id} ({order:?})")])
            }
            ExecutionNode::FetchNew(index) => Tree::Leaf(vec![format!("Temporary Table: {index}")]),
            ExecutionNode::Join(subnodes, bindings) => {
                let subtrees = subnodes
                    .iter()
                    .map(|n| Self::ascii_tree_recursive(n.clone()))
                    .collect();

                Tree::Node(format!("Join {bindings:?}"), subtrees)
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

                Tree::Node(format!("Project {reorder:?}"), vec![subtree])
            }
            ExecutionNode::SelectValue(subnode, assignments) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Value {assignments:?}"), vec![subtree])
            }
            ExecutionNode::SelectEqual(subnode, classes) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Equal {classes:?}"), vec![subtree])
            }
            ExecutionNode::AppendColumns(subnode, instructions) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Columns {instructions:?}"), vec![subtree])
            }
            ExecutionNode::AppendNulls(subnode, num_nulls) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Nulls {num_nulls}"), vec![subtree])
            }
        }
    }

    /// Return an ascii tree representation of the [`ExecutionTree`]
    pub fn ascii_tree(&self) -> Tree {
        if let Some(root) = self.root() {
            let tree = Self::ascii_tree_recursive(root);
            let top_level_name = match self.result() {
                ExecutionResult::Temporary => format!("{} (Temporary)", self.name()),
                ExecutionResult::Permanent(_, order) => {
                    format!("{} (Permanent {order:?}", self.name())
                }
            };

            Tree::Node(top_level_name, vec![tree])
        } else {
            Tree::Leaf(vec![])
        }
    }
}

/// Functionality for optimizing an [`ExecutionTree`]
impl ExecutionTree {
    /// Implements the functionalily for `simplify` by recusively traversing the tree.
    fn simplify_recursive(
        new_tree: &mut ExecutionTree,
        node: ExecutionNodeRef,
        removed_tables: &HashSet<usize>,
    ) -> Option<ExecutionNodeRef> {
        let node_rc = node.get_rc();
        let node_ref = &*node_rc.borrow();

        match node_ref {
            ExecutionNode::FetchExisting(id, order) => {
                Some(new_tree.fetch_existing_reordered(*id, order.clone()))
            }
            ExecutionNode::FetchNew(index) => {
                if removed_tables.contains(index) {
                    None
                } else {
                    Some(new_tree.fetch_new(*index))
                }
            }
            ExecutionNode::Join(subnodes, binding) => {
                let mut simplified_nodes = Vec::<ExecutionNodeRef>::with_capacity(subnodes.len());
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
                let mut simplified_nodes = Vec::<ExecutionNodeRef>::with_capacity(subnodes.len());
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

                if reorder.is_identity() {
                    Some(simplified)
                } else {
                    if let ExecutionNode::FetchExisting(id, order) = &*simplified.get_rc().borrow()
                    {
                        Some(new_tree.fetch_existing_reordered(*id, order.apply_reorder(reorder)))
                    } else {
                        Some(new_tree.project(simplified, reorder.clone()))
                    }
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
    fn simplify(&self, removed_temp_indices: &HashSet<usize>) -> Self {
        let mut new_tree = ExecutionTree::new(&self.name, self.result.clone());

        if let Some(old_root) = self.root() {
            let new_root_opt =
                Self::simplify_recursive(&mut new_tree, old_root, removed_temp_indices);
            if let Some(new_root) = new_root_opt {
                new_tree.set_root(new_root);
            }
        }

        new_tree
    }

    /// Return a list of fetched tables that are required by this tree.
    pub fn required_tables(&self) -> Vec<(TableId, ColumnOrder)> {
        let mut result = Vec::<(TableId, ColumnOrder)>::new();
        for node in &self.nodes {
            if let ExecutionNode::FetchExisting(id, order) = &*node.0.as_ref().borrow() {
                result.push((*id, order.clone()));
            }
        }

        result
    }
}

/// Functionality for ensuring certain constraints
impl ExecutionTree {
    /// Alters the given [`ExecutionTree`] in such a way as to comply with the constraints of the leapfrog trie join algorithm
    /// Specifically, this will reorder tables if necessary
    pub fn satisfy_leapfrog_triejoin(&mut self, arity: usize) {
        if let Some(root) = self.root() {
            self.satisfy_leapfrog_recurisve(root, Reordering::default(arity));
        }
    }

    /// Implements the functionality of `satisfy_leapfrog_triejoin` by traversing the tree recursively
    fn satisfy_leapfrog_recurisve(&mut self, node: ExecutionNodeRef, reorder: Reordering) {
        let node_rc = node.get_rc();
        let node_ref = &mut *node_rc.borrow_mut();

        match node_ref {
            ExecutionNode::FetchExisting(id, order) => {
                *node_ref = ExecutionNode::FetchExisting(*id, order.apply_reorder(&reorder));
            }
            ExecutionNode::FetchNew(index) => {
                let new_fetch = self.fetch_new(*index);
                *node_ref = ExecutionNode::Project(new_fetch, reorder);
            }
            ExecutionNode::Project(subnode, project_reorder) => {
                let subnode_arity = project_reorder.len_source();

                *project_reorder = project_reorder.chain(&reorder);

                self.satisfy_leapfrog_recurisve(
                    subnode.clone(),
                    Reordering::default(subnode_arity),
                );
            }
            ExecutionNode::Join(subnodes, bindings) => {
                let mut sorted_bindings = Vec::<Vec<usize>>::with_capacity(bindings.len());

                for (subnode, binding) in subnodes.iter().zip(bindings.iter()) {
                    let mut sorted_binding = binding.clone();
                    sorted_binding.sort();

                    let binding_reorder = Reordering::from_transformation(&sorted_binding, binding);

                    self.satisfy_leapfrog_recurisve(
                        subnode.clone(),
                        binding_reorder.chain(&reorder),
                    );

                    sorted_bindings.push(sorted_binding);
                }

                *bindings = sorted_bindings
            }
            ExecutionNode::Union(subnodes) => {
                for subnode in subnodes {
                    self.satisfy_leapfrog_recurisve(subnode.clone(), reorder.clone());
                }
            }
            ExecutionNode::Minus(left, right) => {
                self.satisfy_leapfrog_recurisve(left.clone(), reorder.clone());
                self.satisfy_leapfrog_recurisve(right.clone(), reorder.clone());
            }
            ExecutionNode::SelectValue(subnode, _assignments) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(reorder.is_identity());
                // for assigment in assignments {
                //     assigment.column_idx = reorder.apply_element_reverse(assigment.column_idx);
                // }

                self.satisfy_leapfrog_recurisve(subnode.clone(), reorder);
            }
            ExecutionNode::SelectEqual(subnode, _classes) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(reorder.is_identity());
                self.satisfy_leapfrog_recurisve(subnode.clone(), reorder);
            }
            ExecutionNode::AppendColumns(subnode, _) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(reorder.is_identity());
                self.satisfy_leapfrog_recurisve(subnode.clone(), reorder);
            }
            ExecutionNode::AppendNulls(subnode, _) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(reorder.is_identity());
                self.satisfy_leapfrog_recurisve(subnode.clone(), reorder);
            }
        }
    }
}

/// A series of execution plans
/// Usually contains the information necessary for evaluating one rule
#[derive(Debug, Default)]
pub struct ExecutionPlan {
    trees: LinkedHashMap<usize, ExecutionTree>,
    current_id: usize,
}

impl ExecutionPlan {
    /// Create new [`ExecutionPlan`].
    pub fn new() -> Self {
        Self {
            trees: LinkedHashMap::new(),
            current_id: 0,
        }
    }

    /// Append new [`ExecutionTree`] to the plan.
    /// Return the index assigned to the inserted tree.
    pub fn push(&mut self, tree: ExecutionTree) -> usize {
        let result = self.current_id;

        self.trees.insert(self.current_id, tree);
        self.current_id += 1;

        result
    }

    /// Append a list of [`ExecutionTree`] to the plan.
    pub fn append(&mut self, trees: Vec<ExecutionTree>) {
        for tree in trees {
            self.push(tree);
        }
    }

    /// Remove a [`ExecutionTree`] identified by its index.
    /// This does not check whether the deleted tree is used later in some other tree.
    /// Panics if the given index is not occupied.
    pub fn remove(&mut self, index: usize) {
        if self.trees.remove(&index).is_none() {
            panic!("Tried to remove a non-existing index from an execution plan.");
        }
    }

    /// Return an iterator which traverses the [`ExecutionTree`]s in order.
    pub fn iter(&mut self) -> impl Iterator<Item = (&usize, &mut ExecutionTree)> {
        self.trees.iter_mut()
    }

    /// Simplifies the current [`ExecutionPlan`] by
    /// removing superfluous operations like empty unions or default projects
    pub fn simplify(&mut self) {
        let mut removed_new_tables = HashSet::<usize>::new();

        // Simplify all the trees idividually.
        // If a tree is empty (that is has no root) after simplification
        // then we record this information and use it to simplify further
        for (index, tree) in self.trees.iter_mut() {
            *tree = tree.simplify(&removed_new_tables);

            if tree.root().is_none() {
                removed_new_tables.insert(*index);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{management::database::TableId, util::Reordering};

    use super::{ExecutionPlan, ExecutionTree};

    #[test]
    fn general_use() {
        let mut test_plan = ExecutionPlan::new();
        let mut current_id = TableId::default();

        // Create a tree that will only be available temporarily
        let mut tree_temp = ExecutionTree::new_temporary("Computing Temporary table");

        // Create a union subnode and fill it later with subnodes
        let mut node_union_empty = tree_temp.union_empty();
        for _ in 0..3 {
            let new_node = tree_temp.fetch_existing(current_id.increment());
            node_union_empty.add_subnode(new_node);
        }

        // Create a vector of subnodes and create a union node from that
        let subnode_vec = vec![
            tree_temp.fetch_existing(current_id.increment()),
            tree_temp.fetch_existing(current_id.increment()),
        ];
        let node_union_vec = tree_temp.union(subnode_vec);

        // Join both unions
        let node_join = tree_temp.join(
            vec![node_union_empty, node_union_vec],
            vec![vec![0, 1], vec![1, 0]],
        );

        // At the end, set the root of the tree
        tree_temp.set_root(node_join);

        // Add the finished tree to the plan and remember its id
        let temp_id = test_plan.push(tree_temp);

        // Create tree the computation result of which will stay permanently
        let mut tree_perm = ExecutionTree::new_permanent("Computing Projection", "Final Tree");

        // Load temporary table using the id saved earlier
        let node_fetch_temp = tree_perm.fetch_new(temp_id);

        // Project it
        let node_project = tree_perm.project(node_fetch_temp, Reordering::new(vec![0, 2], 3));

        // Set root and add it to the plan
        tree_perm.set_root(node_project);
        test_plan.push(tree_perm);
    }
}
