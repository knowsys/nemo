use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::Debug,
    rc::{Rc, Weak},
};

use ascii_tree::{write_tree, Tree};

use crate::physical::{
    tabular::operations::{
        triescan_append::AppendInstruction, triescan_join::JoinBindings,
        triescan_project::ProjectReordering, triescan_select::SelectEqualClasses, ValueAssignment,
    },
    util::mapping::{permutation::Permutation, traits::NatMapping},
};

use super::database::{ColumnOrder, TableId};

/// Wraps [`ExecutionNode`] into a `Rc<RefCell<_>>`
#[derive(Debug)]
struct ExecutionNodeOwned(Rc<RefCell<ExecutionNode>>);

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
pub struct ExecutionNodeRef(Weak<RefCell<ExecutionNode>>);

impl ExecutionNodeRef {
    /// Return an referenced counted cell of an [`ExecutionNode`].
    pub fn get_rc(&self) -> Rc<RefCell<ExecutionNode>> {
        self.0
            .upgrade()
            .expect("Referenced execution node has been deleted")
    }

    /// Return the id which identifies the referenced node
    pub fn id(&self) -> usize {
        let node_rc = self.get_rc();
        let node_borrow = node_rc.borrow();

        node_borrow.id
    }
}

impl ExecutionNodeRef {
    /// Add a sub node to a join or union node
    pub fn add_subnode(&mut self, subnode: ExecutionNodeRef) {
        let node_rc = self.get_rc();
        let node_operation = &mut node_rc.borrow_mut().operation;

        match node_operation {
            ExecutionOperation::Join(subnodes, _) => subnodes.push(subnode),
            ExecutionOperation::Union(subnodes) => subnodes.push(subnode),
            ExecutionOperation::FetchExisting(_, _) | ExecutionOperation::FetchNew(_) => {
                panic!("Can't add subnode to a leaf node.")
            }
            _ => {
                panic!("Can only add subnodes to operations which can have arbitrary many of them.")
            }
        }
    }
}

/// Represents a node in a [`ExecutionPlan`]
#[derive(Debug)]
pub struct ExecutionNode {
    /// The operation that should be performed on this node.
    pub operation: ExecutionOperation,
    /// Identifier of the node.
    pub id: usize,
}

#[derive(Debug, Clone)]
/// Represents an operation in [`ExecutionPlan`]
pub enum ExecutionOperation {
    /// Fetch a table that is already present in the database instance.
    FetchExisting(TableId, ColumnOrder),
    /// Fetch a table that is computed as part of the [`ExecutionPlan`] this tree is part of.
    FetchNew(usize),
    /// Join operation.
    Join(Vec<ExecutionNodeRef>, JoinBindings),
    /// Union operation.
    Union(Vec<ExecutionNodeRef>),
    /// Table difference operation.
    Minus(ExecutionNodeRef, ExecutionNodeRef),
    /// Table project operation; can only be applied to a [`FetchTable`] or [`FetchTemp`] node.
    Project(ExecutionNodeRef, ProjectReordering),
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

/// Marker for nodes within a [`ExecutionPlan`] that are materialized into their own tables.
#[derive(Debug)]
struct ExecutionOutNode {
    /// Reference to the execution node that is marked as an "output" node.
    node: ExecutionNodeRef,
    /// How to save the resulting table.
    result: ExecutionResult,
    /// Name which identifies this operation, e.g., for logging and timing.
    name: String,
}

/// A DAG representing instructions for generating new tables.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// All the nodes in the tree.
    nodes: Vec<ExecutionNodeOwned>,
    /// A list that marks all nodes that will be materialized into their own tables.
    out_nodes: Vec<ExecutionOutNode>,
}

impl Default for ExecutionPlan {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            out_nodes: Default::default(),
        }
    }
}

impl ExecutionPlan {
    /// Push new node to list of all nodes and returns a reference.
    fn push_and_return_ref(&mut self, operation: ExecutionOperation) -> ExecutionNodeRef {
        let id = self.nodes.len();
        let node = ExecutionNode { operation, id };

        self.nodes.push(ExecutionNodeOwned::new(node));
        self.nodes
            .last()
            .expect("New node has been added above.")
            .get_ref()
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table.
    pub fn fetch_existing(&mut self, id: TableId) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchExisting(id, ColumnOrder::default());
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table.
    pub fn fetch_existing_reordered(
        &mut self,
        id: TableId,
        order: ColumnOrder,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchExisting(id, order);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for fetching a temporary table.
    pub fn fetch_new(&mut self, index: usize) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchNew(index);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn join_empty(&mut self, bindings: JoinBindings) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(Vec::new(), bindings);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn join(
        &mut self,
        subtables: Vec<ExecutionNodeRef>,
        bindings: JoinBindings,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(subtables, bindings);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for the union of several tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn union_empty(&mut self) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Union(Vec::new());
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for joining tables.
    pub fn union(&mut self, subtables: Vec<ExecutionNodeRef>) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Union(subtables);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for subtracting one table from another.
    pub fn minus(&mut self, left: ExecutionNodeRef, right: ExecutionNodeRef) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Minus(left, right);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for applying project to a table.
    pub fn project(
        &mut self,
        subnode: ExecutionNodeRef,
        project_reordering: ProjectReordering,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Project(subnode, project_reordering);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for restricing a column to a certain value.
    pub fn select_value(
        &mut self,
        subnode: ExecutionNodeRef,
        assigments: Vec<ValueAssignment>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::SelectValue(subnode, assigments);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for restricting a column to values of certain other columns.
    pub fn select_equal(
        &mut self,
        subnode: ExecutionNodeRef,
        eq_classes: SelectEqualClasses,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::SelectEqual(subnode, eq_classes);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for appending columns to a trie.
    pub fn append_columns(
        &mut self,
        subnode: ExecutionNodeRef,
        instructions: Vec<Vec<AppendInstruction>>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::AppendColumns(subnode, instructions);
        self.push_and_return_ref(new_operation)
    }

    /// Return [`ExecutionNodeRef`] for appending null-columns to a trie.
    pub fn append_nulls(
        &mut self,
        subnode: ExecutionNodeRef,
        num_nulls: usize,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::AppendNulls(subnode, num_nulls);
        self.push_and_return_ref(new_operation)
    }

    /// Designate a [`ExecutionNode`] as an "output" node that will result in a materialized table.
    fn push_out_node(
        &mut self,
        node: ExecutionNodeRef,
        result: ExecutionResult,
        name: &str,
    ) -> usize {
        let id = node.id();

        self.out_nodes.push(ExecutionOutNode {
            node,
            result,
            name: String::from(name),
        });

        id
    }

    /// Designate a [`ExecutionNode`] as an "output" node that will produce a temporary table.
    /// Returns an id which will later be associated with the result of the computation.
    pub fn write_temporary(&mut self, node: ExecutionNodeRef, tree_name: &str) -> usize {
        self.push_out_node(node, ExecutionResult::Temporary, tree_name)
    }

    /// Designate a [`ExecutionNode`] as an "output" node that will produce a permament table (in its default order).
    /// Returns an id which will later be associated with the result of the computation.
    pub fn write_permanent(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
    ) -> usize {
        self.write_permanent_reordered(node, tree_name, table_name, ColumnOrder::default())
    }

    /// Designate a [`ExecutionNode`] as an "output" node that will produce a permament table.
    /// Returns an id which will later be associated with the result of the computation.
    pub fn write_permanent_reordered(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
        order: ColumnOrder,
    ) -> usize {
        self.push_out_node(
            node,
            ExecutionResult::Permanent(order, String::from(table_name)),
            tree_name,
        )
    }

    fn copy_subgraph(
        new_plan: &mut ExecutionPlan,
        node: ExecutionNodeRef,
        write_node_ids: &HashSet<usize>,
    ) -> ExecutionNodeRef {
        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;
        let node_id = node_rc.borrow().id;

        if write_node_ids.contains(&node_id) {
            return new_plan.fetch_new(node_id);
        }

        match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                new_plan.fetch_existing_reordered(id.clone(), order.clone())
            }
            ExecutionOperation::FetchNew(index) => new_plan.fetch_new(*index),
            ExecutionOperation::Join(subnodes, bindings) => {
                let new_subnodes = subnodes
                    .iter()
                    .cloned()
                    .map(|n| Self::copy_subgraph(new_plan, n, write_node_ids))
                    .collect();

                new_plan.join(new_subnodes, bindings.clone())
            }
            ExecutionOperation::Union(subnodes) => {
                let new_subnodes = subnodes
                    .iter()
                    .cloned()
                    .map(|n| Self::copy_subgraph(new_plan, n, write_node_ids))
                    .collect();

                new_plan.union(new_subnodes)
            }
            ExecutionOperation::Minus(left, right) => {
                let new_left = Self::copy_subgraph(new_plan, left.clone(), write_node_ids);
                let new_right = Self::copy_subgraph(new_plan, right.clone(), write_node_ids);

                new_plan.minus(new_left, new_right)
            }
            ExecutionOperation::Project(subnode, reorder) => {
                let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
                new_plan.project(new_subnode, reorder.clone())
            }
            ExecutionOperation::SelectValue(subnode, assignments) => {
                let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
                new_plan.select_value(new_subnode, assignments.clone())
            }
            ExecutionOperation::SelectEqual(subnode, classes) => {
                let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
                new_plan.select_equal(new_subnode, classes.clone())
            }
            ExecutionOperation::AppendColumns(subnode, instructions) => {
                let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
                new_plan.append_columns(new_subnode, instructions.clone())
            }
            ExecutionOperation::AppendNulls(subnode, num_null_cols) => {
                let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
                new_plan.append_nulls(new_subnode, *num_null_cols)
            }
        }
    }

    /// Return a list of [`ExecutionTree`] that are derived taking the subgraph at each write node.
    /// Each tree will be associated with an id that corrsponds to the id of
    /// the write node from which the tree is derived.
    pub(super) fn split_at_write_nodes(&self) -> Vec<(usize, ExecutionTree)> {
        let write_node_ids: HashSet<usize> = self.out_nodes.iter().map(|o| o.node.id()).collect();

        let mut result = Vec::new();
        for out_node in &self.out_nodes {
            let id = out_node.node.id();
            let mut subtree = ExecutionPlan::default();

            let mut write_node_ids_without_this = write_node_ids.clone();
            write_node_ids_without_this.remove(&id);

            let root_node = Self::copy_subgraph(
                &mut subtree,
                out_node.node.clone(),
                &write_node_ids_without_this,
            );
            subtree.out_nodes.push(ExecutionOutNode {
                node: root_node,
                result: out_node.result.clone(),
                name: out_node.name.clone(),
            });

            result.push((id, ExecutionTree(subtree)));
        }

        result
    }
}

/// Represents a subtree of a [`ExecutionPlan`]
/// Contains an [`ExecutionPlan`] with a designated root node.
pub(super) struct ExecutionTree(pub ExecutionPlan);

impl Debug for ExecutionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_tree(f, &self.ascii_tree())
    }
}

impl ExecutionTree {
    /// Returns the [`ExecutionResult`] of this tree.
    pub(super) fn result(&self) -> &ExecutionResult {
        &self.0.out_nodes[0].result
    }

    /// Return the name of this tree.
    pub fn name(&self) -> &str {
        &self.0.out_nodes[0].name
    }

    /// Return the root of the trie.
    pub fn root(&self) -> ExecutionNodeRef {
        self.0.out_nodes[0].node.clone()
    }

    fn ascii_tree_recursive(node: ExecutionNodeRef) -> Tree {
        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                Tree::Leaf(vec![format!("Permanent Table: {id} ({order})")])
            }
            ExecutionOperation::FetchNew(index) => {
                Tree::Leaf(vec![format!("Temporary Table: {index}")])
            }
            ExecutionOperation::Join(subnodes, bindings) => {
                let subtrees = subnodes
                    .iter()
                    .map(|n| Self::ascii_tree_recursive(n.clone()))
                    .collect();

                Tree::Node(format!("Join {bindings:?}"), subtrees)
            }
            ExecutionOperation::Union(subnodes) => {
                let subtrees = subnodes
                    .iter()
                    .map(|n| Self::ascii_tree_recursive(n.clone()))
                    .collect();

                Tree::Node(String::from("Union"), subtrees)
            }
            ExecutionOperation::Minus(node_left, node_right) => {
                let subtree_left = Self::ascii_tree_recursive(node_left.clone());
                let subtree_right = Self::ascii_tree_recursive(node_right.clone());

                Tree::Node(String::from("Minus"), vec![subtree_left, subtree_right])
            }
            ExecutionOperation::Project(subnode, reorder) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Project {reorder:?}"), vec![subtree])
            }
            ExecutionOperation::SelectValue(subnode, assignments) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Value {assignments:?}"), vec![subtree])
            }
            ExecutionOperation::SelectEqual(subnode, classes) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Select Equal {classes:?}"), vec![subtree])
            }
            ExecutionOperation::AppendColumns(subnode, instructions) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Columns {instructions:?}"), vec![subtree])
            }
            ExecutionOperation::AppendNulls(subnode, num_nulls) => {
                let subtree = Self::ascii_tree_recursive(subnode.clone());

                Tree::Node(format!("Append Nulls {num_nulls}"), vec![subtree])
            }
        }
    }

    /// Return an ascii tree representation of the [`ExecutionTree`]
    pub fn ascii_tree(&self) -> Tree {
        let tree = Self::ascii_tree_recursive(self.root());
        let top_level_name = match self.result() {
            ExecutionResult::Temporary => format!("{} (Temporary)", self.name()),
            ExecutionResult::Permanent(order, name) => {
                format!("{} (Permanent {order}", name)
            }
        };

        Tree::Node(top_level_name, vec![tree])
    }
}

/// Functionality for optimizing an [`ExecutionTree`]
impl ExecutionTree {
    /// Implements the functionalily for `simplify` by recusively traversing the tree.
    fn simplify_recursive(
        new_tree: &mut ExecutionPlan,
        node: ExecutionNodeRef,
        removed_tables: &HashSet<usize>,
    ) -> Option<ExecutionNodeRef> {
        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                Some(new_tree.fetch_existing_reordered(*id, order.clone()))
            }
            ExecutionOperation::FetchNew(index) => {
                if removed_tables.contains(index) {
                    None
                } else {
                    Some(new_tree.fetch_new(*index))
                }
            }
            ExecutionOperation::Join(subnodes, binding) => {
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
            ExecutionOperation::Union(subnodes) => {
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
            ExecutionOperation::Minus(left, right) => {
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
            ExecutionOperation::Project(subnode, reorder) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if reorder.is_identity() {
                    Some(simplified)
                } else if let ExecutionOperation::FetchExisting(id, order) =
                    &simplified.get_rc().borrow().operation
                {
                    if reorder.is_permutation() {
                        Some(new_tree.fetch_existing_reordered(
                            *id,
                            order.chain_permutation(&reorder.into_permutation()),
                        ))
                    } else {
                        Some(new_tree.project(simplified, reorder.clone()))
                    }
                } else {
                    Some(new_tree.project(simplified, reorder.clone()))
                }
            }
            ExecutionOperation::SelectValue(subnode, assignments) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if assignments.is_empty() {
                    Some(simplified)
                } else {
                    Some(new_tree.select_value(simplified, assignments.clone()))
                }
            }
            ExecutionOperation::SelectEqual(subnode, classes) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if classes.is_empty() {
                    Some(simplified)
                } else {
                    Some(new_tree.select_equal(simplified, classes.clone()))
                }
            }
            ExecutionOperation::AppendColumns(subnode, instructions) => {
                let simplified =
                    Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

                if instructions.iter().all(|i| i.is_empty()) {
                    Some(simplified)
                } else {
                    Some(new_tree.append_columns(simplified, instructions.clone()))
                }
            }
            ExecutionOperation::AppendNulls(subnode, num_nulls) => {
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
    /// Returns `None` if the simplified tree results in a no-op.
    pub fn simplify(&self, removed_temp_indices: &HashSet<usize>) -> Option<Self> {
        let mut simplified_tree = ExecutionPlan::default();
        let new_root =
            Self::simplify_recursive(&mut simplified_tree, self.root(), removed_temp_indices)?;

        simplified_tree.out_nodes.push(ExecutionOutNode {
            node: new_root,
            result: self.result().clone(),
            name: String::from(self.name()),
        });

        Some(ExecutionTree(simplified_tree))
    }

    /// Return a list of fetched tables that are required by this tree.
    pub fn required_tables(&self) -> Vec<(TableId, ColumnOrder)> {
        let mut result = Vec::<(TableId, ColumnOrder)>::new();
        for node in &self.0.nodes {
            if let ExecutionOperation::FetchExisting(id, order) =
                &node.0.as_ref().borrow().operation
            {
                result.push((*id, order.clone()));
            }
        }

        result
    }
}

/// Functionality for ensuring certain constraints
impl ExecutionTree {
    /// Alters the given [`ExecutionTree`] in such a way as to comply with the constraints of the leapfrog trie join algorithm.
    /// Specifically, this will reorder tables if necessary
    pub fn satisfy_leapfrog_triejoin(&mut self) {
        Self::satisfy_leapfrog_recurisve(self.root(), Permutation::default());
    }

    /// Implements the functionality of `satisfy_leapfrog_triejoin` by traversing the tree recursively
    fn satisfy_leapfrog_recurisve(node: ExecutionNodeRef, permutation: Permutation) {
        let node_rc = node.get_rc();
        let node_operation = &mut node_rc.borrow_mut().operation;

        match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                *node_operation =
                    ExecutionOperation::FetchExisting(*id, order.chain_permutation(&permutation));
            }
            ExecutionOperation::FetchNew(_index) => {
                if !permutation.is_identity() {
                    // TODO: One cannot infer a ProjectReordering from a Permutation

                    // let new_fetch = self.fetch_new(*index);
                    // *node_operation = ExecutionOperation::Project(new_fetch, permutation);

                    panic!("Automatic reordering of new tables currently not supported");
                }
            }
            ExecutionOperation::Project(subnode, project_reorder) => {
                *project_reorder = project_reorder.chain_permutation(&permutation);

                Self::satisfy_leapfrog_recurisve(subnode.clone(), Permutation::default());
            }
            ExecutionOperation::Join(subnodes, bindings) => {
                bindings.apply_permutation(&permutation);
                let subpermutations = bindings.comply_with_leapfrog();

                for (subnode, subpermutation) in subnodes.iter().zip(subpermutations.into_iter()) {
                    Self::satisfy_leapfrog_recurisve(subnode.clone(), subpermutation)
                }
            }
            ExecutionOperation::Union(subnodes) => {
                for subnode in subnodes {
                    Self::satisfy_leapfrog_recurisve(subnode.clone(), permutation.clone());
                }
            }
            ExecutionOperation::Minus(left, right) => {
                Self::satisfy_leapfrog_recurisve(left.clone(), permutation.clone());
                Self::satisfy_leapfrog_recurisve(right.clone(), permutation.clone());
            }
            ExecutionOperation::SelectValue(subnode, _assignments) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(permutation.is_identity());
                // for assigment in assignments {
                //     assigment.column_idx = reorder.apply_element_reverse(assigment.column_idx);
                // }

                Self::satisfy_leapfrog_recurisve(subnode.clone(), permutation);
            }
            ExecutionOperation::SelectEqual(subnode, _classes) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(permutation.is_identity());
                Self::satisfy_leapfrog_recurisve(subnode.clone(), permutation);
            }
            ExecutionOperation::AppendColumns(subnode, _instructions) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(permutation.is_identity());

                Self::satisfy_leapfrog_recurisve(subnode.clone(), Permutation::default());
            }
            ExecutionOperation::AppendNulls(subnode, _) => {
                // TODO: A few other changes are needed to make this a bit simpler.
                // Will update it then
                assert!(permutation.is_identity());
                Self::satisfy_leapfrog_recurisve(subnode.clone(), permutation.clone());
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        management::database::TableId,
        tabular::operations::{triescan_project::ProjectReordering, JoinBindings},
    };

    use super::ExecutionPlan;

    #[test]
    fn general_use() {
        let mut test_plan = ExecutionPlan::default();
        let mut current_id = TableId::default();

        // Create a union subnode and fill it later with subnodes
        let mut node_union_empty = test_plan.union_empty();
        for _ in 0..3 {
            let new_node = test_plan.fetch_existing(current_id.increment());
            node_union_empty.add_subnode(new_node);
        }

        // Create a vector of subnodes and create a union node from that
        let subnode_vec = vec![
            test_plan.fetch_existing(current_id.increment()),
            test_plan.fetch_existing(current_id.increment()),
        ];
        let node_union_vec = test_plan.union(subnode_vec);

        // Join both unions
        let node_join = test_plan.join(
            vec![node_union_empty, node_union_vec],
            JoinBindings::new(vec![vec![0, 1], vec![1, 0]]),
        );

        // Set the join node as an output node that will produce a temporary table
        test_plan.write_temporary(node_join.clone(), "Computing Join");

        // Project it the result of the join
        let node_project_a = test_plan.project(
            node_join.clone(),
            ProjectReordering::from_vector(vec![0, 2], 3),
        );

        let node_project_b =
            test_plan.project(node_join, ProjectReordering::from_vector(vec![2, 0], 3));

        // Write both projections as permanent tables
        test_plan.write_permanent(node_project_a, "Computing Projection A", "Table X");
        test_plan.write_permanent(node_project_b, "Computing Projection B", "Table X");
    }
}
