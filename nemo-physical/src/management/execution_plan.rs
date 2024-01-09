//! This module defines [ExecutionPlan],
//! which is used to obtain sequences of database operations.

use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::Debug,
    rc::{Rc, Weak},
};

use crate::{
    tabular::operations::{
        filter::FilterAssignment, function::FunctionAssignment, join::GeneratorJoin,
        OperationGeneratorEnum, OperationTable,
    },
    util::mapping::permutation::Permutation,
};

use super::database::{
    execution_tree::{ExecutionTree, ExecutionTreeNode},
    id::PermanentTableId,
};

/// Type that represents a reordering of the columns of a table.
/// It is given in form of a permutation which encodes the transformation
/// that is needed to get from the original column order to this one.
pub type ColumnOrder = Permutation;

/// Wraps [ExecutionNode] into a `Rc<RefCell<_>>`
#[derive(Debug)]
struct ExecutionNodeOwned(Rc<RefCell<ExecutionNode>>);

impl ExecutionNodeOwned {
    /// Create new [ExecutionNodeOwned]
    pub fn new(node: ExecutionNode) -> Self {
        Self(Rc::new(RefCell::new(node)))
    }

    /// Return a [ExecutionNodeRef] pointing to this node
    pub fn get_ref(&self) -> ExecutionNodeRef {
        ExecutionNodeRef(Rc::downgrade(&self.0))
    }
}

/// Wraps [ExecutionNode] into a `Weak<RefCell<_>>`
#[derive(Debug, Clone, Copy)]
pub struct ExecutionNodeRef(Weak<RefCell<ExecutionNode>>);

impl ExecutionNodeRef {
    /// Return an referenced counted cell of an [ExecutionNode].
    /// # Panics
    /// Throws a panic if the referenced [ExecutionNode] does not exist.
    pub fn get_rc(&self) -> Rc<RefCell<ExecutionNode>> {
        self.0
            .upgrade()
            .expect("Referenced execution node has been deleted")
    }

    /// Return the id which identifies the referenced node
    /// # Panics
    /// Throws a panic if borrowing the current [ExecutionNode] is already mutably borrowed.
    pub fn id(&self) -> usize {
        let node_rc = self.get_rc();
        let node_borrow = node_rc.borrow();

        node_borrow.id
    }

    /// Return [OperationTable],
    /// which marks the colums of the table represented by this node.
    pub fn markers(&self) -> OperationTable {
        let node_rc = self.get_rc();
        let node_borrow = node_rc.borrow();

        node_borrow.marked_columns.clone()
    }
}

impl ExecutionNodeRef {
    /// Add a sub node to a join or union node.
    pub fn add_subnode(&mut self, subnode: ExecutionNodeRef) {
        let node_rc = self.get_rc();
        let node_operation = &mut node_rc.borrow_mut().operation;

        match node_operation {
            ExecutionOperation::Join(subnodes) | ExecutionOperation::Union(subnodes) => {
                subnodes.push(subnode)
            }
            ExecutionOperation::FetchTable(_, _) => {
                panic!("Can't add subnode to a leaf node.")
            }
            _ => {
                panic!("Can only add subnodes to operations which can have arbitrary many of them.")
            }
        }
    }

    /// Return the list of subnodes.
    pub fn subnodes(&self) -> Vec<ExecutionNodeRef> {
        let node_rc = self.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::FetchTable(_, _) => vec![],
            ExecutionOperation::Join(subnodes) => subnodes.clone(),
            ExecutionOperation::Union(subnodes) => subnodes.clone(),
            ExecutionOperation::Subtract(subnode_main, subnodes_subtract) => {
                let mut result = vec![subnode_main.clone()];
                result.extend(subnodes_subtract);

                result
            }
            ExecutionOperation::ProjectReorder(subnode) => vec![subnode.clone()],
            ExecutionOperation::Filter(subnode, _) => vec![subnode.clone()],
            ExecutionOperation::Function(subnode, _) => vec![subnode.clone()],
            ExecutionOperation::Aggregate() => todo!(),
        }
    }
}

/// Identifies a node within an [ExecutionPlan]
pub type ExecutionNodeId = usize;

/// Represents a node in an [ExecutionPlan]
#[derive(Debug)]
pub struct ExecutionNode {
    /// Identifier of the node.
    pub id: ExecutionNodeId,

    /// Operation that should be performed on this node
    pub operation: ExecutionOperation,
    /// Representation of the output table represented by this node
    pub marked_columns: OperationTable,
}

#[derive(Debug, Clone)]
/// Represents an operation in [ExecutionPlan]
pub enum ExecutionOperation {
    /// Fetch a table that is already present in the database instance
    FetchTable(PermanentTableId, ColumnOrder),
    /// Join the tables represented by the subnodes
    Join(Vec<ExecutionNodeRef>),
    /// Union of the tables represented by the subnodes
    Union(Vec<ExecutionNodeRef>),
    /// Subtraction of the tables represented from another table represented by the subnodes
    Subtract(ExecutionNodeRef, Vec<ExecutionNodeRef>),
    /// Reorder or remove columns from the table represented by the subnode
    ProjectReorder(ExecutionNodeRef),
    /// Apply a filter to the table represented by the subnode
    Filter(ExecutionNodeRef, FilterAssignment),
    /// Introduce new columns by applying a function to the columns of the table represented by the subnode
    Function(ExecutionNodeRef, FunctionAssignment),
    /// Aggreagte
    /// TODO: Implement aggregates again
    Aggregate(),
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently
#[derive(Debug, Clone)]
pub(super) enum ExecutionResult {
    /// Table will be dropped after the [ExecutionPlan] is finished.
    Temporary,
    /// Table will be stored permanently in the [DatabaseInstance][super::database::DatabaseInstance]
    /// with the given [ColumnOrder] and the given name.
    Permanent(ColumnOrder, String),
}

/// Marker for nodes within an [ExecutionPlan] that are materialized into their own tables
#[derive(Debug)]
struct ExecutionOutNode {
    /// Reference to the execution node that is marked as an "output" node.
    node: ExecutionNodeRef,
    /// How to save the resulting table.
    result: ExecutionResult,
    /// Name which identifies this operation, e.g., for logging and timing.
    operation_name: String,
}

/// A DAG representing instructions for generating new tables.
#[derive(Debug, Default)]
pub struct ExecutionPlan {
    /// All the nodes in the tree.
    nodes: Vec<ExecutionNodeOwned>,
    /// A list that marks all nodes that will be materialized into their own tables.
    out_nodes: Vec<ExecutionOutNode>,
}

impl ExecutionPlan {
    /// Push new node to list of all nodes and returns a reference.
    fn push_and_return_reference(
        &mut self,
        operation: ExecutionOperation,
        marked_columns: OperationTable,
    ) -> ExecutionNodeRef {
        let id = self.nodes.len();
        let node = ExecutionNode {
            id,
            operation,
            marked_columns,
        };

        self.nodes.push(ExecutionNodeOwned::new(node));
        self.nodes
            .last()
            .expect("New node has been added above.")
            .get_ref()
    }

    /// Return [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::DatabaseInstance].
    pub fn fetch_table(
        &mut self,
        marked_columns: OperationTable,
        id: PermanentTableId,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchTable(id, ColumnOrder::default());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::DatabaseInstance]
    /// in a given [ColumnOrder].
    pub fn fetch_table_reordered(
        &mut self,
        marked_columns: OperationTable,
        id: PermanentTableId,
        order: ColumnOrder,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchTable(id, order);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for joining tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn join_empty(&mut self, marked_columns: OperationTable) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(Vec::new());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for joining tables.
    pub fn join(
        &mut self,
        marked_columns: OperationTable,
        subtables: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(subtables);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for the union of several tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn union_empty(&mut self, marked_columns: OperationTable) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Union(Vec::new());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for joining tables.
    pub fn union(
        &mut self,
        marked_columns: OperationTable,
        subtables: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Union(subtables);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for subtracting one table from another.
    pub fn subtract(
        &mut self,
        marked_columns: OperationTable,
        main: ExecutionNodeRef,
        subtracted: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Subtract(main, subtracted);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for applying project to a table.
    pub fn projectreorder(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Project(subnode);

        // The project/reorder operation requires that its input is a materialized trie.
        // Also, the result of this operation is a materialized trie.
        // Hence, we mark the input of this node as well as this node as "output" nodes.
        let project_node = self.push_and_return_reference(new_operation, marked_columns);
        self.write_temporary(subnode, "Input for Project/Reorder");
        self.write_temporary(project_node, "Project/Reorder");

        project_node
    }

    /// Return [ExecutionNodeRef] for restricing a column to a certain value.
    pub fn filter(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
        filters: FilterAssignment,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Filter(subnode, filters);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for restricting a column to values of certain other columns.
    pub fn function(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
        functions: FunctionAssignment,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::SelectEqual(subnode, functions);
        self.push_and_return_reference(new_operation, marked_columns)
    }
}

impl ExecutionPlan {
    /// Designate a [ExecutionNode] as an "output" node that will result in a materialized table.
    ///
    /// If table has been marked as an "ouput" node already,
    /// this function will overwrite the name and [ExecutionResult].
    fn push_out_node(
        &mut self,
        node: ExecutionNodeRef,
        result: ExecutionResult,
        operation_name: &str,
    ) {
        if let Some(index) = self
            .out_nodes
            .iter()
            .position(|out_node| out_node.node.id() == node.id)
        {
            self.out_nodes[index].operation_name = String::from(operation_name);
            self.out_nodes[index].result = result;
        } else {
            self.out_nodes.push(ExecutionOutNode {
                node,
                result,
                operation_name: String::from(operation_name),
            });
        }
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a temporary table.
    pub fn write_temporary(&mut self, node: ExecutionNodeRef, operation_name: &str) {
        self.push_out_node(node, ExecutionResult::Temporary, operation_name)
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a permanent table (in its default order).
    pub fn write_permanent(&mut self, node: ExecutionNodeRef, tree_name: &str, table_name: &str) {
        self.write_permanent_reordered(node, tree_name, table_name, ColumnOrder::default())
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a permament table.
    pub fn write_permanent_reordered(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
        order: ColumnOrder,
    ) {
        self.push_out_node(
            node,
            ExecutionResult::Permanent(order, String::from(table_name)),
            tree_name,
        )
    }
}

impl ExecutionPlan {
    /// Split the [ExecutionPlan] into a list of [ExecutionTree]s
    /// that will be executed one after another
    /// by the [DatabaseInstance][super::DatabaseInstance].
    pub(crate) fn finalize(self) -> Vec<ExecutionTree> {
        todo!()
    }

    // fn copy_subgraph(
    //     new_plan: &mut ExecutionPlan,
    //     node: ExecutionNodeRef,
    //     write_node_ids: &HashSet<usize>,
    // ) -> ExecutionNodeRef {
    //     let node_rc = node.get_rc();
    //     let node_operation = &node_rc.borrow().operation;
    //     let node_id = node_rc.borrow().id;

    //     if write_node_ids.contains(&node_id) {
    //         return new_plan.fetch_new(node_id);
    //     }

    //     match node_operation {
    //         ExecutionOperation::FetchExisting(id, order) => {
    //             new_plan.fetch_existing_reordered(*id, order.clone())
    //         }
    //         ExecutionOperation::FetchNew(index) => new_plan.fetch_new(*index),
    //         ExecutionOperation::Join(subnodes, bindings) => {
    //             let new_subnodes = subnodes
    //                 .iter()
    //                 .cloned()
    //                 .map(|n| Self::copy_subgraph(new_plan, n, write_node_ids))
    //                 .collect();

    //             new_plan.join(new_subnodes, bindings.clone())
    //         }
    //         ExecutionOperation::Union(subnodes) => {
    //             let new_subnodes = subnodes
    //                 .iter()
    //                 .cloned()
    //                 .map(|n| Self::copy_subgraph(new_plan, n, write_node_ids))
    //                 .collect();

    //             new_plan.union(new_subnodes)
    //         }
    //         ExecutionOperation::Minus(left, right) => {
    //             let new_left = Self::copy_subgraph(new_plan, left.clone(), write_node_ids);
    //             let new_right = Self::copy_subgraph(new_plan, right.clone(), write_node_ids);

    //             new_plan.minus(new_left, new_right)
    //         }
    //         ExecutionOperation::Project(subnode, reorder) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.project(new_subnode, reorder.clone())
    //         }
    //         ExecutionOperation::Filter(subnode, assignments) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.filter_values(new_subnode, assignments.clone())
    //         }
    //         ExecutionOperation::SelectEqual(subnode, classes) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.select_equal(new_subnode, classes.clone())
    //         }
    //         ExecutionOperation::AppendColumns(subnode, instructions) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.append_columns(new_subnode, instructions.clone())
    //         }
    //         ExecutionOperation::AppendNulls(subnode, num_null_cols) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.append_nulls(new_subnode, *num_null_cols)
    //         }
    //         ExecutionOperation::Subtract(subnode_main, subnodes_subtract, subtract_infos) => {
    //             let new_main = Self::copy_subgraph(new_plan, subnode_main.clone(), write_node_ids);
    //             let new_subtract = subnodes_subtract
    //                 .iter()
    //                 .cloned()
    //                 .map(|n| Self::copy_subgraph(new_plan, n, write_node_ids))
    //                 .collect();

    //             new_plan.subtract(new_main, new_subtract, subtract_infos.clone())
    //         }
    //         ExecutionOperation::Aggregate(subnode, instructions) => {
    //             let new_subnode = Self::copy_subgraph(new_plan, subnode.clone(), write_node_ids);
    //             new_plan.aggregate(new_subnode, *instructions)
    //         }
    //     }
    // }

    // /// Deletes all information which marks execution nodes as output.
    // pub fn clear_write_nodes(&mut self) {
    //     self.out_nodes.clear();
    // }

    fn execution_subtree(
        node: ExecutionNodeRef,
        out_node_ids: &HashSet<ExecutionNodeId>,
    ) -> ExecutionTreeNode {
        if out_node_ids.contains(&node.id()) {
            // Do something
        }

        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;
        let node_markers = node_rc.borrow().marked_columns.clone();

        match node_operation {
            ExecutionOperation::FetchTable(_, _) => todo!(),
            ExecutionOperation::Join(subnodes) => {
                let subnode_markers = subnodes.iter().map(|node| node.markers()).collect();
                let subtrees = subnodes
                    .iter()
                    .map(|node| Self::execution_subtree(node, out_node_ids))
                    .collect();

                ExecutionTreeNode::Operation {
                    generator: OperationGeneratorEnum::Join(GeneratorJoin::new(
                        node_markers,
                        subnode_markers,
                    )),
                    subnodes: subtrees,
                }
            }
            ExecutionOperation::Union(_) => todo!(),
            ExecutionOperation::Subtract(_, _) => todo!(),
            ExecutionOperation::ProjectReorder(_) => todo!(),
            ExecutionOperation::Filter(_, _) => todo!(),
            ExecutionOperation::Function(_, _) => todo!(),
            ExecutionOperation::Aggregate() => todo!(),
        }
    }

    /// Return a list of [ExecutionTree] that are derived taking the subgraph at each write node.
    /// Each tree will be associated with an id that corresponds to the id of
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
                cut_bottom_layers: out_node.cut_bottom_layers,
            });

            result.push((id, ExecutionTree::new(subtree)));
        }

        result
    }
}
