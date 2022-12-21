use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use crate::physical::tabular::operations::{
    triescan_join::JoinBinding, triescan_project::ColumnPermutation,
    triescan_select::SelectEqualClasses, ValueAssignment,
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
        if let Some(self_rc) = self.0.upgrade() {
            let node_ref = &mut *self_rc.as_ref().borrow_mut();

            match node_ref {
                ExecutionNode::Join(subnodes, _) => subnodes.push(subnode),
                ExecutionNode::Union(subnodes) => subnodes.push(subnode),
                ExecutionNode::FetchTable(_) | ExecutionNode::FetchTemp(_) => {
                    panic!("Can't add subnode to a leaf node")
                }
                ExecutionNode::Minus(_, _)
                | ExecutionNode::Project(_, _)
                | ExecutionNode::SelectEqual(_, _)
                | ExecutionNode::SelectValue(_, _) => {
                    panic!(
                        "Can only add subnodes to operations which can have arbitrary many of them"
                    )
                }
            }
        } else {
            // This is probably a bug somewhere
            panic!("Adding subnodes to a deleted node.");
        }
    }
}

/// Represents a database operation that should be performed
#[derive(Debug)]
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
    Project(ExecutionNodeRef<TableKey>, ColumnPermutation),
    /// Only leave entries in that have a certain value.
    SelectValue(ExecutionNodeRef<TableKey>, Vec<ValueAssignment>),
    /// Only leave entries in that contain equal values in certain columns.
    SelectEqual(ExecutionNodeRef<TableKey>, SelectEqualClasses),
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently.
#[derive(Debug, Clone)]
pub enum ExecutionResult<TableKey: TableKeyType> {
    /// Temporary table with the id
    Temp(TableId),
    /// Temporary table that only uses a subset of the columns with the id
    TempSubset(TableId, Vec<bool>),
    /// Permanent table with the following identifier, range, column order and priority
    Save(TableKey),
}

/// Represents the plan for calculating a table
#[derive(Debug)]
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

    fn simplify_recursive() {}

    /// Builds a new [`ExecutionTree`] which does not include superfluous operations,
    /// like, e.g., performing a join over one subtable.
    pub fn simplify(&self) -> Option<Self> {
        None
    }

    /// Return the result of this operation
    pub fn result(&self) -> &ExecutionResult<TableKey> {
        &self.result
    }

    /// Return the name of this tree.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Return the root of the trie
    pub fn root(&self) -> Option<ExecutionNodeRef<TableKey>> {
        self.root.clone()
    }

    /// Set the root node of the tree
    pub fn set_root(&mut self, root: ExecutionNodeRef<TableKey>) {
        self.root = Some(root);
    }

    /// Return an iterator containing a mutable reference to all FetchTable nodes.
    pub fn all_fetched_tables(
        &mut self,
    ) -> impl Iterator<Item = &mut ExecutionNodeOwned<TableKey>> {
        self.nodes.iter_mut().filter(|n| {
            if let ExecutionNode::FetchTable(_) = &*n.0.as_ref().borrow() {
                true
            } else {
                false
            }
        })
    }

    /// Pushed new node to list of all nodes and returns a reference
    fn push_and_return_ref(&mut self, node: ExecutionNode<TableKey>) -> ExecutionNodeRef<TableKey> {
        self.nodes.push(ExecutionNodeOwned::new(node));
        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for fetching a permanent table
    pub fn fetch_table(&mut self, key: TableKey) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::FetchTable(key);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for fetching a temporary table
    pub fn fetch_temp(&mut self, id: TableId) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::FetchTemp(id);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables
    /// Starts out empty; add subnodes with `add_subnode`
    pub fn join_empty(&mut self, binding: JoinBinding) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Join(Vec::new(), binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for joining tables
    pub fn join(
        &mut self,
        subtables: Vec<ExecutionNodeRef<TableKey>>,
        binding: JoinBinding,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Join(subtables, binding);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for the union of several tables
    /// Starts out empty; add subnodes with `add_subnode`
    pub fn union_empty(&mut self) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Union(Vec::new());
        self.nodes.push(ExecutionNodeOwned::new(new_node));

        self.nodes.last().unwrap().get_ref()
    }

    /// Return [`ExecutionNodeRef`] for joining tables
    pub fn union(
        &mut self,
        subtables: Vec<ExecutionNodeRef<TableKey>>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Union(subtables);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for subtracting one table from another
    pub fn minus(
        &mut self,
        left: ExecutionNodeRef<TableKey>,
        right: ExecutionNodeRef<TableKey>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Minus(left, right);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for applying project to a table
    pub fn project(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        permutation: ColumnPermutation,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::Project(subnode, permutation);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricing a column to a certain value
    pub fn select_value(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        assigments: Vec<ValueAssignment>,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::SelectValue(subnode, assigments);
        self.push_and_return_ref(new_node)
    }

    /// Return [`ExecutionNodeRef`] for restricing a column to values of certain other columns
    pub fn select_equal(
        &mut self,
        subnode: ExecutionNodeRef<TableKey>,
        eq_classes: SelectEqualClasses,
    ) -> ExecutionNodeRef<TableKey> {
        let new_node = ExecutionNode::SelectEqual(subnode, eq_classes);
        self.push_and_return_ref(new_node)
    }
}

/// A series of execution plans
/// Usually contains the information necessary for evaluating one rule
#[derive(Debug)]
pub struct ExecutionPlan<TableKey: TableKeyType> {
    /// The individual steps that will be executed
    /// Each step will result in either a temporary or a permanent table
    pub trees: Vec<ExecutionTree<TableKey>>,
}

impl<TableKey: TableKeyType> ExecutionPlan<TableKey> {
    /// Create new [`ExecutionPlan`]
    pub fn new() -> Self {
        Self { trees: Vec::new() }
    }

    /// Append new [`ExecutionTree`] to the plan
    pub fn push(&mut self, tree: ExecutionTree<TableKey>) {
        debug_assert!(tree.root.is_some());
        self.trees.push(tree);
    }

    /// Append a list of [`ExecutionTree`] to the plan
    pub fn append(&mut self, mut trees: Vec<ExecutionTree<TableKey>>) {
        debug_assert!(trees.iter().all(|t| t.root.is_some()));
        self.trees.append(&mut trees);
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
