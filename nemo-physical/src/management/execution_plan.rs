//! This module defines [ExecutionPlan],
//! which is used to obtain sequences of database operations.

use std::{
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    rc::{Rc, Weak},
};

use crate::{
    datasources::table_providers::TableProvider,
    management::database::execution_series::{
        ExecutionTreeLeaf, ExecutionTreeNode, ExecutionTreeOperation,
    },
    tabular::operations::{
        OperationGeneratorEnum, OperationTable,
        aggregate::{AggregateAssignment, GeneratorAggregate},
        filter::{Filters, GeneratorFilter},
        function::{FunctionAssignment, GeneratorFunction},
        incremental_import::GeneratorIncrementalImport,
        join::GeneratorJoin,
        null::GeneratorNull,
        projectreorder::GeneratorProjectReorder,
        single::GeneratorSingle,
        subtract::GeneratorSubtract,
        union::GeneratorUnion,
    },
    util::mapping::{permutation::Permutation, traits::NatMapping},
};

use super::{
    database::{
        execution_series::{ComputedTableId, ExecutionSeries, ExecutionTree, LoadedTableId},
        id::{ExecutionId, PermanentTableId, TableId},
    },
    util::closest_order,
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
    pub(crate) fn new(node: ExecutionNode) -> Self {
        Self(Rc::new(RefCell::new(node)))
    }

    /// Return an [ExecutionNodeRef] pointing to this node
    pub(crate) fn get_ref(&self) -> ExecutionNodeRef {
        ExecutionNodeRef(Rc::downgrade(&self.0))
    }
}

/// Reference to a node in an [ExecutionPlan]
///
/// Internally, uses a `Weak<RefCell<T>>`.
#[derive(Clone)]
pub struct ExecutionNodeRef(Weak<RefCell<ExecutionNode>>);

impl Debug for ExecutionNodeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.upgrade() {
            Some(node) => f
                .debug_tuple("ExecutionNodeRef")
                .field(&node.borrow().id)
                .finish(),
            _ => f
                .debug_tuple("ExecutionNodeRef")
                .field(&"<failed to upgrade weak reference>".to_string())
                .finish(),
        }
    }
}

impl ExecutionNodeRef {
    /// Return an referenced counted cell of an [ExecutionNode].
    /// # Panics
    /// Throws a panic if the referenced [ExecutionNode] does not exist.
    pub(crate) fn get_rc(&self) -> Rc<RefCell<ExecutionNode>> {
        self.0
            .upgrade()
            .expect("Referenced execution node has been deleted")
    }

    /// Return the [ExecutionId] which identifies the referenced node
    ///
    /// # Panics
    /// Panics if the underlying node does not exist or is alreary mutably borrowed.
    pub fn id(&self) -> ExecutionId {
        let node_rc = self.get_rc();
        let node_borrow = node_rc.borrow();

        node_borrow.id
    }

    /// Return [OperationTable],
    /// which marks the columns of the table represented by this node.
    pub fn markers_cloned(&self) -> OperationTable {
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
            ExecutionOperation::Join(subnodes) => subnodes.push(subnode),
            ExecutionOperation::Union(subnodes) => {
                debug_assert!(subnodes.iter().all(|node| node.markers_cloned().is_empty()
                    || node.markers_cloned().len() == subnode.markers_cloned().len()));

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
    #[allow(dead_code)]
    pub(crate) fn subnodes(&self) -> Vec<ExecutionNodeRef> {
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
            ExecutionOperation::ProjectReorder(subnode)
            | ExecutionOperation::Single(subnode, _)
            | ExecutionOperation::Filter(subnode, _)
            | ExecutionOperation::Function(subnode, _)
            | ExecutionOperation::Null(subnode)
            | ExecutionOperation::Aggregate(subnode, _)
            | ExecutionOperation::IncrementalImport(subnode, _) => vec![subnode.clone()],
        }
    }
}

/// Represents a node in an [ExecutionPlan]
#[derive(Debug)]
pub(crate) struct ExecutionNode {
    /// Identifier of the node.
    pub id: ExecutionId,

    /// Operation that should be performed on this node
    pub operation: ExecutionOperation,
    /// Representation of the output table represented by this node
    pub marked_columns: OperationTable,
}

#[derive(Debug)]
/// Represents an operation in [ExecutionPlan]
pub(crate) enum ExecutionOperation {
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
    /// Retain only single elements from certain columns
    Single(ExecutionNodeRef, OperationTable),
    /// Apply a filter to the table represented by the subnode
    Filter(ExecutionNodeRef, Filters),
    /// Introduce new columns by applying a function to the columns of the table represented by the subnode
    Function(ExecutionNodeRef, FunctionAssignment),
    /// Append columns with fresh nulls to the table represented by the subnode
    Null(ExecutionNodeRef),
    /// Perform aggregate operation
    Aggregate(ExecutionNodeRef, AggregateAssignment),
    /// Perform an incremental import
    IncrementalImport(ExecutionNodeRef, Rc<Box<dyn TableProvider>>),
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
    pub(crate) fn is_permanent(&self) -> bool {
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

    /// Return an [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::database::DatabaseInstance].
    pub fn fetch_table(
        &mut self,
        marked_columns: OperationTable,
        id: PermanentTableId,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::FetchTable(id, ColumnOrder::default());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for fetching a table from the [DatabaseInstance][super::database::DatabaseInstance]
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

    /// Return an [ExecutionNodeRef] for joining tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn join_empty(&mut self, marked_columns: OperationTable) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(Vec::new());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for joining tables.
    pub fn join(
        &mut self,
        marked_columns: OperationTable,
        subtables: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Join(subtables);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for the union of several tables.
    /// Starts out empty; add subnodes with `add_subnode`.
    pub fn union_empty(&mut self, marked_columns: OperationTable) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Union(Vec::new());
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for joining tables.
    pub fn union(
        &mut self,
        marked_columns: OperationTable,
        subtables: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        debug_assert!(subtables.iter().all(|node| node.markers_cloned().is_empty()
            || node.markers_cloned().len() == marked_columns.len()));

        let new_operation = ExecutionOperation::Union(subtables);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for subtracting one table from another.
    pub fn subtract(
        &mut self,
        main: ExecutionNodeRef,
        subtracted: Vec<ExecutionNodeRef>,
    ) -> ExecutionNodeRef {
        let marked_columns = main.markers_cloned();
        let new_operation = ExecutionOperation::Subtract(main, subtracted);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for applying project to a table.
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

    /// Return an [ExecutionNodeRef]
    pub fn single(
        &mut self,
        subnode: ExecutionNodeRef,
        single_columns: OperationTable,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Single(subnode.clone(), single_columns);

        let single_node = self.push_and_return_reference(new_operation, subnode.markers_cloned());
        self.write_temporary(single_node.clone(), "Single");

        single_node
    }

    /// Return an [ExecutionNodeRef] for restricing a column to a certain value.
    pub fn filter(&mut self, subnode: ExecutionNodeRef, filters: Filters) -> ExecutionNodeRef {
        let marked_columns = subnode.markers_cloned();
        let new_operation = ExecutionOperation::Filter(subnode, filters);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for restricting a column to values of certain other columns.
    pub fn function(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
        functions: FunctionAssignment,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Function(subnode, functions);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for computing an aggregate.
    pub fn aggregate(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
        aggregate_assignment: AggregateAssignment,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Aggregate(subnode, aggregate_assignment);
        let aggregate_node = self.push_and_return_reference(new_operation, marked_columns);

        self.write_temporary(aggregate_node.clone(), "Aggregate output");

        aggregate_node
    }

    /// Return an [ExecutionNodeRef] for appending columns with fresh null.
    pub fn null(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
    ) -> ExecutionNodeRef {
        let new_operation = ExecutionOperation::Null(subnode);
        self.push_and_return_reference(new_operation, marked_columns)
    }

    /// Return an [ExecutionNodeRef] for performing an import.
    pub fn import(
        &mut self,
        marked_columns: OperationTable,
        subnode: ExecutionNodeRef,
        provider: Box<dyn TableProvider>,
    ) -> ExecutionNodeRef {
        let new_operation =
            ExecutionOperation::IncrementalImport(subnode.clone(), Rc::new(provider));
        let import_node = self.push_and_return_reference(new_operation, marked_columns);

        self.write_temporary(subnode, "Input for Import");
        self.write_temporary(import_node.clone(), "Import");

        import_node
    }
}

impl ExecutionPlan {
    /// Designate a [ExecutionNode] as an "output" node that will result in a materialized table.
    ///
    /// If table has been marked as an "output" node already,
    /// this function will not overwrite the existing name
    /// but will upgrade the [ExecutionResult] to permanent if required.
    ///
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluating this node.
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
            if !matches!(result, ExecutionResult::Temporary) {
                self.out_nodes[index].result = result;
            }
        } else {
            self.out_nodes.push(ExecutionOutNode {
                node,
                result,
                operation_name: String::from(operation_name),
            });
        }

        id
    }

    /// Designate a node in this [ExecutionPlan] as an "output" node that will produce a temporary table.
    ///
    /// Returns an [ExecutionId] which can be linked to the output table
    /// that was computed by evaluateing this node.
    pub fn write_temporary(&mut self, node: ExecutionNodeRef, operation_name: &str) -> ExecutionId {
        self.push_out_node(node, ExecutionResult::Temporary, operation_name)
    }

    /// Designate a node in this [ExecutionPlan] as an "output" node that will produce a permanent table (in its default order).
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

    /// Designate  node in this [ExecutionPlan] as an "output" node that will produce a permament table.
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
        // The computed tree will only be stored permanently in its original order
        let execution_result = if order.is_identity() {
            output_node.result.clone()
        } else {
            ExecutionResult::Temporary
        };

        let new_tree = if let Some(tables) = computed_trees_map.get(&output_node.node.id()) {
            let (closest_index, closest_order) =
                closest_order(tables.iter().map(|(order, _id)| order), &order)
                    .expect("Trie should exist at least in one order.");
            let closest_computed_id = tables[closest_index].1;
            let arity = output_node.node.markers_cloned().arity();
            let generator = GeneratorProjectReorder::from_reordering(
                closest_order.clone(),
                order.clone(),
                arity,
            );

            computed_trees[closest_computed_id].used += 1;

            if generator.is_noop() {
                return closest_computed_id;
            }

            let root = ExecutionTreeNode::ProjectReorder {
                generator,
                subnode: ExecutionTreeLeaf::FetchComputedTable(closest_computed_id),
            };

            ExecutionTree {
                root,
                id: output_node.node.id(),
                result: execution_result,
                operation_name: String::from("Reordering intermediate table"),
                cut_layers: 0, // TODO: Compute cut_layers
                used: 1,
                dependents: Vec::new(),
            }
        } else {
            let root = Self::execution_node(
                output_node.node.id(),
                output_node.node.clone(),
                order.clone(),
                output_nodes,
                computed_trees,
                computed_trees_map,
                loaded_tables,
            );

            let computed_table_id = computed_trees.len();

            if let ExecutionTreeNode::ProjectReorder {
                generator,
                subnode: ExecutionTreeLeaf::FetchComputedTable(computed),
            } = &root
                && !&computed_trees[*computed].result.is_permanent()
            {
                computed_trees[*computed]
                    .dependents
                    .push((computed_table_id, generator.projectreordering()));
                computed_trees[*computed].used -= 1;
            }

            ExecutionTree {
                root,
                id: output_node.node.id(),
                result: execution_result,
                operation_name: output_node.operation_name.clone(),
                cut_layers: 0, // TODO: Compute cut_layers
                used: 1,
                dependents: Vec::new(),
            }
        };

        let computed_table_id = computed_trees.len();

        computed_trees.push(new_tree);
        computed_trees_map.insert(output_node.node.id(), vec![(order, computed_table_id)]);

        computed_table_id
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
        if node.id() != root_node_id
            && let Some(output_node) = output_nodes
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

        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;
        let mut node_markers = node_rc.borrow().marked_columns.clone();
        if !node_markers.is_empty() {
            node_markers = node_markers.apply_permutation(&order);
        }

        match node_operation {
            ExecutionOperation::FetchTable(id, fetch_order) => {
                let new_loaded_order = fetch_order.chain_permutation(&order);
                let new_loaded_id = loaded_tables.len();
                let id = match loaded_tables.entry((*id, new_loaded_order)) {
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
                let (subnode_markers, subnode_reorderings): (
                    Vec<OperationTable>,
                    Vec<Permutation>,
                ) = subnodes
                    .iter()
                    .map(|node| node.markers_cloned().align(&node_markers))
                    .unzip();

                let subtrees = subnodes
                    .iter()
                    .zip(subnode_reorderings)
                    .map(|(node, reorder)| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            reorder,
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
            ExecutionOperation::Aggregate(subnode, aggregate_assignment) => {
                let input = subnode.markers_cloned();
                let generator_aggregate = GeneratorAggregate::new(&input, aggregate_assignment);

                let subtree = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    order.clone(),
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                // Add aggregate operation
                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Aggregate(generator_aggregate),
                    subnodes: vec![subtree],
                })
            }
            ExecutionOperation::Union(subnodes) => {
                let subtrees = subnodes
                    .iter()
                    .map(|node| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            order.clone(),
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
                let markers_main = node_markers;
                let (markers_subtract, subtract_reorderings): (
                    Vec<OperationTable>,
                    Vec<Permutation>,
                ) = subnodes_subtract
                    .iter()
                    .map(|subnode| subnode.markers_cloned().align(&markers_main))
                    .unzip();

                let subtrees = vec![(subnode_main, order.clone())]
                    .into_iter()
                    .chain(subnodes_subtract.iter().zip(subtract_reorderings))
                    .map(|(node, reorder)| {
                        Self::execution_node(
                            root_node_id,
                            node.clone(),
                            reorder,
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
                    order.clone(),
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
                    order,
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                // TODO: A reordering may be required if the computed columns
                // appear earlier than the argument columns they are using.

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Function(GeneratorFunction::new(
                        node_markers,
                        function_assignment,
                    )),
                    subnodes: vec![subtree],
                })
            }
            ExecutionOperation::ProjectReorder(subnode) => {
                let marker_subnode = subnode.markers_cloned();
                let subtree: ExecutionTreeLeaf = if let ExecutionTreeOperation::Leaf(leaf) =
                    Self::execution_node(
                        root_node_id,
                        subnode.clone(),
                        ColumnOrder::default(),
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
            ExecutionOperation::Null(subnode) => {
                let subtree = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    order,
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                ExecutionTreeNode::Operation(ExecutionTreeOperation::Node {
                    generator: OperationGeneratorEnum::Null(GeneratorNull::new(
                        node_markers,
                        subnode.markers_cloned(),
                    )),
                    subnodes: vec![subtree],
                })
            }
            ExecutionOperation::Single(subnode, single) => {
                let marker_subnode = subnode.markers_cloned();
                let subtree = Self::execution_node(
                    root_node_id,
                    subnode.clone(),
                    order,
                    output_nodes,
                    computed_trees,
                    computed_trees_map,
                    loaded_tables,
                )
                .operation()
                .expect("No sub node should be a project");

                ExecutionTreeNode::Single {
                    generator: GeneratorSingle::new(marker_subnode, single.clone()),
                    subnode: subtree,
                }
            }
            ExecutionOperation::IncrementalImport(subnode, table_provider) => {
                let marker_subnode = subnode.markers_cloned();
                let subtree: ExecutionTreeLeaf = if let ExecutionTreeOperation::Leaf(leaf) =
                    Self::execution_node(
                        root_node_id,
                        subnode.clone(),
                        ColumnOrder::default(),
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

                ExecutionTreeNode::IncrementalImport {
                    generator: GeneratorIncrementalImport::new(
                        node_markers,
                        marker_subnode,
                        table_provider.clone(),
                    ),
                    subnode: subtree,
                }
            }
        }
    }

    /// Split the [ExecutionPlan] into an [ExecutionSeries]
    /// by splitting it into several [ExecutionTree]s
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

        let mut loaded_tries =
            vec![(PermanentTableId::default(), ColumnOrder::default()); loaded_tables.len()];

        for (key, index) in loaded_tables.into_iter() {
            loaded_tries[index] = key;
        }

        ExecutionSeries {
            loaded_tries,
            trees,
        }
    }
}
