//! This module defines [ExecutionPlan],
//! which is used to obtain sequences of database operations.

use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    rc::{Rc, Weak},
};

use crate::{
    columnar::column::Column,
    management::database::execution_series::{
        ExecutionTreeLeaf, ExecutionTreeNode, ExecutionTreeOperation,
    },
    tabular::operations::{
        filter::{Filters, GeneratorFilter},
        function::{FunctionAssignment, GeneratorFunction},
        join::GeneratorJoin,
        projectreorder::GeneratorProjectReorder,
        subtract::GeneratorSubtract,
        union::GeneratorUnion,
        OperationGeneratorEnum, OperationTable,
    },
    util::mapping::permutation::Permutation,
};

use super::database::{
    execution_series::{ComputedTableId, ExecutionSeries, ExecutionTree, LoadedTableId},
    id::{ExecutionId, PermanentTableId, TableId},
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
#[derive(Debug, Clone)]
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

    /// Return the [ExecutionId] which identifies the referenced node
    ///
    /// # Panics
    /// Throws a panic if borrowing the current [ExecutionNode] is already mutably borrowed.
    pub fn id(&self) -> ExecutionId {
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
                result.extend(subnodes_subtract.iter().cloned());

                result
            }
            ExecutionOperation::ProjectReorder(subnode) => vec![subnode.clone()],
            ExecutionOperation::Filter(subnode, _) => vec![subnode.clone()],
            ExecutionOperation::Function(subnode, _) => vec![subnode.clone()],
            ExecutionOperation::Aggregate() => todo!(),
        }
    }
}

/// Represents a node in an [ExecutionPlan]
#[derive(Debug)]
pub struct ExecutionNode {
    /// Identifier of the node.
    pub id: ExecutionId,

    /// Operation that should be performed on this node
    pub operation: ExecutionOperation,
    /// Representation of the output table represented by this node
    pub marked_columns: OperationTable,
}

#[derive(Debug)]
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
    Filter(ExecutionNodeRef, Filters),
    /// Introduce new columns by applying a function to the columns of the table represented by the subnode
    Function(ExecutionNodeRef, FunctionAssignment),
    /// Aggreagte
    /// TODO: Implement aggregates again
    Aggregate(),
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently
#[derive(Debug, Clone)]
pub(crate) enum ExecutionResult {
    /// Table will be dropped after the [ExecutionPlan] is finished.
    Temporary,
    /// Table will be stored permanently in the [DatabaseInstance][super::database::DatabaseInstance]
    /// with the given [ColumnOrder] and the given name.
    Permanent(ColumnOrder, String),
}

impl ExecutionResult {
    /// Return whether the operation will result in a permanent table.
    pub fn is_permanent(&self) -> bool {
        matches!(self, ExecutionResult::Permanent(_, _))
    }
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

    /// The current [ExecutionId]
    current_execution_id: ExecutionId,
}

impl ExecutionPlan {
    /// Push new node to list of all nodes and returns a reference.
    fn push_and_return_reference(
        &mut self,
        operation: ExecutionOperation,
        marked_columns: OperationTable,
    ) -> ExecutionNodeRef {
        let id = self.current_execution_id.increment();
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

    /// Return [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::database::DatabaseInstance].
    pub fn fetch_table(
        &mut self,
        marked_columns: OperationTable,
        id: PermanentTableId,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchTable(id, ColumnOrder::default());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::database::DatabaseInstance]
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
        main: ExecutionNodeRef,
        subtracted: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let marked_columns = main.markers();
        let new_operation = ExecutionOperation::Subtract(main, subtracted);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return [ExecutionNodeRef] for applying project to a table.
    pub fn projectreorder(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::ProjectReorder(subnode.clone());

        // The project/reorder operation requires that its input is a materialized trie.
        // Also, the result of this operation is a materialized trie.
        // Hence, we mark the input of this node as well as this node as "output" nodes.
        let project_node = self.push_and_return_reference(new_operation, marked_columns);
        self.write_temporary(subnode, "Input for Project/Reorder");
        self.write_temporary(project_node.clone(), "Project/Reorder");

        project_node
    }

    /// Return [ExecutionNodeRef] for restricing a column to a certain value.
    pub fn filter(&mut self, subnode: ExecutionNodeRef, filters: Filters) -> ExecutionNodeRef {
        let marked_columns = subnode.markers();
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
        let new_operation = ExecutionOperation::Function(subnode, functions);
        self.push_and_return_reference(new_operation, marked_columns)
    }
}

impl ExecutionPlan {
    /// Designate a [ExecutionNode] as an "output" node that will result in a materialized table.
    ///
    /// If table has been marked as an "ouput" node already,
    /// this function will overwrite the name and [ExecutionResult].
    ///
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluateing this node.
    fn push_out_node(
        &mut self,
        node: ExecutionNodeRef,
        result: ExecutionResult,
        operation_name: &str,
    ) -> ExecutionId {
        let id = node.id();

        if let Some(index) = self
            .out_nodes
            .iter()
            .position(|out_node| out_node.node.id() == node.id())
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

        id
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a temporary table.
    ///
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluateing this node.
    pub fn write_temporary(&mut self, node: ExecutionNodeRef, operation_name: &str) -> ExecutionId {
        self.push_out_node(node, ExecutionResult::Temporary, operation_name)
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a permanent table (in its default order).
    ///
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluateing this node.
    pub fn write_permanent(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
    ) -> ExecutionId {
        self.write_permanent_reordered(node, tree_name, table_name, ColumnOrder::default())
    }

    /// Designate a [ExecutionNode] as an "output" node that will produce a permament table.
    ///  
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluateing this node.
    pub fn write_permanent_reordered(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
        order: ColumnOrder,
    ) -> ExecutionId {
        self.push_out_node(
            node,
            ExecutionResult::Permanent(order, String::from(table_name)),
            tree_name,
        )
    }
}

impl ExecutionPlan {
    /// From a given [ExecutionOutNode], computes its [ExecutionTree]
    /// by depth-first traversing the [ExecutionPlan].
    ///
    /// The newly created [ExecutionTree] is pushed into `computed_trees`.
    /// Returns a [ComputedTableId], which is the index of the newly generated
    /// [ExecutionTree] in the `computed_trees` vector.
    fn execution_tree(
        output_node: &ExecutionOutNode,
        order: ColumnOrder,
        output_nodes: &[ExecutionOutNode],
        computed_trees: &mut Vec<ExecutionTree>,
        computed_trees_map: &mut HashMap<ExecutionId, Vec<(ColumnOrder, ComputedTableId)>>,
        loaded_tables: &mut HashMap<(PermanentTableId, ColumnOrder), LoadedTableId>,
    ) -> ComputedTableId {
        if let Some(id) = computed_trees_map.get(&output_node.node.id()) {
            // Find the closest order and return a reordering if needed

            *id
        } else {
            let id = computed_trees.len();

            let root = Self::execution_node(
                output_node.node.id(),
                output_node.node.clone(),
                order.clone(),
                output_nodes,
                computed_trees,
                computed_trees_map,
                loaded_tables,
            );

            let tree = ExecutionTree {
                root,
                id: output_node.node.id(),
                result: output_node.result.clone(),
                operation_name: output_node.operation_name.clone(),
                cut_layers: 0, // TODO: Compute cut_layers
            };

            let computed_table_id = computed_trees.len();
            computed_trees.push(tree);
            computed_trees_map.insert(output_node.node.id(), vec![(order, computed_table_id)]);

            id
        }
    }

    /// Generate a [ExecutionTreeNode] in an [ExecutionTree]
    /// by a depth-first traversal of the given [ExecutionPlan].
    fn execution_node(
        root_node_id: ExecutionId,
        node: ExecutionNodeRef,
        order: ColumnOrder,
        output_nodes: &[ExecutionOutNode],
        computed_trees: &mut Vec<ExecutionTree>,
        computed_trees_map: &mut HashMap<ExecutionId, Vec<(ColumnOrder, ComputedTableId)>>,
        loaded_tables: &mut HashMap<(PermanentTableId, ColumnOrder), LoadedTableId>,
    ) -> ExecutionTreeNode {
        if node.id() != root_node_id {
            if let Some(output_node) = output_nodes
                .iter()
                .find(|output_node| node.id() == output_node.node.id())
            {
                let computed_id = Self::execution_tree(
                    output_node,
                    order,
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                );

                return ExecutionTreeNode::Operation(ExecutionTreeOperation::Leaf(
                    ExecutionTreeLeaf::FetchComputedTable(computed_id),
                ));
            }
        }

        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;
        let node_markers = node_rc.borrow().marked_columns.clone();

        match node_operation {
            ExecutionOperation::FetchTable(id, order) => {
                let new_loaded_id = loaded_tables.len();
                let id = match loaded_tables.entry((*id, order.clone())) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        entry.insert(new_loaded_id);
                        new_loaded_id
                    }
                };

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Leaf(
                    ExecutionTreeLeaf::LoadTable(id),
                ))
            }
            ExecutionOperation::Join(subnodes) => {
                let subnode_markers = subnodes.iter().map(|node| node.markers()).collect();
                let subtrees = subnodes
                    .iter()
                    .map(|node| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            output_nodes,
                            computed_trees,
                            computed_trees_map,
                            loaded_tables,
                        )
                        .operation()
                        .expect("No sub node should be a project")
                    })
                    .collect::<Vec<_>>();

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Join(GeneratorJoin::new(
                        node_markers,
                        subnode_markers,
                    )),
                    subnodes: subtrees,
                })
            }
            ExecutionOperation::Union(subnodes) => {
                let subtrees = subnodes
                    .iter()
                    .map(|node| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            output_nodes,
                            computed_trees,
                            computed_trees_map,
                            loaded_tables,
                        )
                        .operation()
                        .expect("No sub node should be a project")
                    })
                    .collect::<Vec<_>>();

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Union(GeneratorUnion::new()),
                    subnodes: subtrees,
                })
            }
            ExecutionOperation::Subtract(subnode_main, subnodes_subtract) => {
                let markers_main = subnode_main.markers();
                let markers_subtract = subnodes_subtract
                    .iter()
                    .map(|subnode| subnode.markers())
                    .collect();

                let subtrees = vec![subnode_main]
                    .iter()
                    .cloned()
                    .chain(subnodes_subtract.iter())
                    .map(|node| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            output_nodes,
                            computed_trees,
                            computed_trees_map,
                            loaded_tables,
                        )
                        .operation()
                        .expect("No sub node should be a project")
                    })
                    .collect::<Vec<_>>();

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Subtract(GeneratorSubtract::new(
                        markers_main,
                        markers_subtract,
                    )),
                    subnodes: subtrees,
                })
            }
            ExecutionOperation::Filter(subnode, filters) => {
                let subtree = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Filter(GeneratorFilter::new(
                        node_markers,
                        filters,
                    )),
                    subnodes: vec![subtree],
                })
            }
            ExecutionOperation::Function(subnode, function_assignment) => {
                let subtree = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Function(GeneratorFunction::new(
                        node_markers,
                        function_assignment,
                    )),
                    subnodes: vec![subtree],
                })
            }
            ExecutionOperation::Aggregate() => todo!(),
            ExecutionOperation::ProjectReorder(subnode) => {
                let marker_subnode = subnode.markers();
                let subtree = if let ExecutionTreeOperation::Leaf(leaf) = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project")
                {
                    leaf
                } else {
                    unreachable!("Subnode of a project must be a load instruction");
                };

                ExecutionTreeNode::ProjectReorder {
                    generator: GeneratorProjectReorder::new(node_markers, marker_subnode),
                    subnode: subtree,
                }
            }
        }
    }

    /// Split the [ExecutionPlan] into an [ExecutionSeries]
    /// by splitting it into several [ExecutionTree][super::database::execution_series::ExecutionTree]s
    /// that will be executed one after another
    /// by the [DatabaseInstance][super::database::DatabaseInstance].
    pub(crate) fn finalize(self) -> ExecutionSeries {
        let mut loaded_tables = HashMap::<(PermanentTableId, ColumnOrder), LoadedTableId>::new();
        let mut trees = Vec::<ExecutionTree>::with_capacity(self.out_nodes.len());
        let mut trees_map = HashMap::<ExecutionId, Vec<(ColumnOrder, ComputedTableId)>>::new();

        for output_node in &self.out_nodes {
            if !output_node.result.is_permanent() {
                continue;
            }

            Self::execution_tree(
                output_node,
                ColumnOrder::default(),
                &self.out_nodes,
                &mut trees,
                &mut trees_map,
                &mut loaded_tables,
            );
        }

        let mut loaded_tries = loaded_tables.keys().cloned().collect::<Vec<_>>();
        loaded_tries.sort_by(|left, right| {
            loaded_tables
                .get(left)
                .expect("Keys were generated from the map")
                .cmp(
                    loaded_tables
                        .get(right)
                        .expect("Keys were generated from the map"),
                )
        });

        ExecutionSeries {
            loaded_tries,
            trees,
        }
    }
}
