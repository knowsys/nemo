//! This module defines [ExecutionSeries],
//! which defines a series of database operations
//! that can be executed by the [DatabaseInstance][super::DatabaseInstance].

use std::fmt::Debug;

use crate::{
    management::execution_plan::{ColumnOrder, ExecutionResult},
    tabular::operations::{projectreorder::GeneratorProjectReorder, OperationGeneratorEnum},
};

use super::id::{ExecutionId, PermanentTableId};

pub(crate) type LoadedTableId = usize;
pub(crate) type ComputedTableId = usize;

#[derive(Debug)]
pub(crate) enum ExecutionTreeLeaf {
    /// Table was already in the data base
    LoadTable(LoadedTableId),
    /// Table was computed during the execution of the execution plan
    FetchComputedTable(ComputedTableId),
}

#[derive(Debug)]
pub(crate) enum ExecutionTreeOperation {
    Leaf(ExecutionTreeLeaf),
    Node {
        generator: OperationGeneratorEnum,
        subnodes: Vec<ExecutionTreeOperation>,
    },
}

/// A node in the [ExecutionTree]
#[derive(Debug)]
pub(crate) enum ExecutionTreeRoot {
    Operation(ExecutionTreeOperation),
    ProjectReorder {
        generator: GeneratorProjectReorder,
        subnode: ExecutionTreeLeaf,
    },
}

impl ExecutionTreeRoot {
    /// Returns [ExecutionTreeOperation] if this is not a project node.
    ///
    /// Returns `None` otherwise.
    pub fn operation(self) -> Option<ExecutionTreeOperation> {
        if let Self::Operation(result) = self {
            Some(result)
        } else {
            None
        }
    }
}

/// A tree representation of of database operations
/// that can be executed by the [DatabaseInstance][super::DatabaseInstance]
pub(crate) struct ExecutionTree {
    /// Root node of the tree
    pub root: ExecutionTreeRoot,
    /// How the result of this operation will be stored
    pub result: ExecutionResult,
    /// [ExecutionId] to associate this tree to its original node in the plan
    pub id: ExecutionId,
    /// Name of the of tree, e.g. for debugging purposes
    pub operation_name: String,

    /// Starting from the bottom most layer,
    /// how many layers are not used in the computation
    /// and therefore not need to be computed fully
    pub cut_layers: usize,
}

impl Debug for ExecutionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ascii_tree::write_tree(f, &self.ascii_tree())
    }
}

impl ExecutionTree {
    fn ascii_tree_recursive(node: &ExecutionTreeOperation) -> ascii_tree::Tree {
        match node {
            ExecutionTreeOperation::Leaf(leaf) => match leaf {
                ExecutionTreeLeaf::LoadTable(_) => {
                    ascii_tree::Tree::Leaf(vec![format!("Permanent Table")])
                }
                ExecutionTreeLeaf::FetchComputedTable(_) => {
                    ascii_tree::Tree::Leaf(vec![format!("New Table")])
                }
            },
            ExecutionTreeOperation::Node {
                generator,
                subnodes,
            } => {
                let subtrees = subnodes
                    .iter()
                    .map(|node| Self::ascii_tree_recursive(node))
                    .collect();

                ascii_tree::Tree::Node(format!("{generator:?}"), subtrees)
            }
        }
    }

    /// Return an ascii tree representation of the [ExecutionTree].
    pub fn ascii_tree(&self) -> ascii_tree::Tree {
        let tree = match &self.root {
            ExecutionTreeRoot::Operation(operation_tree) => {
                Self::ascii_tree_recursive(operation_tree)
            }
            ExecutionTreeRoot::ProjectReorder { generator, subnode } => {
                let subnode_tree = match subnode {
                    ExecutionTreeLeaf::LoadTable(_) => {
                        ascii_tree::Tree::Leaf(vec![format!("Permanent Table")])
                    }
                    ExecutionTreeLeaf::FetchComputedTable(_) => {
                        ascii_tree::Tree::Leaf(vec![format!("New Table")])
                    }
                };

                ascii_tree::Tree::Node(format!("{generator:?}"), vec![subnode_tree])
            }
        };

        let top_level_name = match &self.result {
            ExecutionResult::Temporary => format!("{} (Temporary)", self.operation_name),
            ExecutionResult::Permanent(order, name) => {
                format!("{name} (Permanent {order}")
            }
        };

        ascii_tree::Tree::Node(top_level_name, vec![tree])
    }
}

/// Represents a
#[derive(Debug)]
pub(crate) struct ExecutionSeries {
    /// Tables that need to be present in memory before executing this series
    pub loaded_tries: Vec<(PermanentTableId, ColumnOrder)>,
    /// List of execution trees that makes up this series
    pub trees: Vec<ExecutionTree>,
}

// impl ExecutionTree {
//     /// Create a [ExecutionTree] from a [ExecutionPlan] with one write node
//     pub fn new(plan: ExecutionPlan) -> Self {
//         debug_assert!(plan.out_nodes.len() == 1);

//         ExecutionTree(plan)
//     }

//     /// Returns the [ExecutionResult] of this tree.
//     pub(super) fn result(&self) -> &ExecutionResult {
//         &self.0.out_nodes[0].result
//     }

//     /// Return the name of this tree.
//     pub fn name(&self) -> &str {
//         &self.0.out_nodes[0].name
//     }

//     /// Return the root of the trie.
//     pub fn root(&self) -> ExecutionNodeRef {
//         self.0.out_nodes[0].node.clone()
//     }

//     /// How many of the final layers will be dropped in the end result
//     pub fn cut_bottom(&self) -> usize {
//         self.0.out_nodes[0].cut_bottom_layers
//     }

// }

// /// Functionality for optimizing an [ExecutionTree]
// impl ExecutionTree {
//     /// Implements the functionalily for simplify by recusively traversing the tree.
//     fn simplify_recursive(
//         new_tree: &mut ExecutionPlan,
//         node: ExecutionNodeRef,
//         removed_tables: &HashSet<usize>,
//     ) -> Option<ExecutionNodeRef> {
//         let node_rc = node.get_rc();
//         let node_operation = &node_rc.borrow().operation;

//         match node_operation {
//             ExecutionOperation::FetchExisting(id, order) => {
//                 Some(new_tree.fetch_existing_reordered(*id, order.clone()))
//             }
//             ExecutionOperation::FetchNew(index) => {
//                 if removed_tables.contains(index) {
//                     None
//                 } else {
//                     Some(new_tree.fetch_new(*index))
//                 }
//             }
//             ExecutionOperation::Join(subnodes, binding) => {
//                 let mut simplified_nodes = Vec::<ExecutionNodeRef>::with_capacity(subnodes.len());
//                 for subnode in subnodes {
//                     let simplified_opt =
//                         Self::simplify_recursive(new_tree, subnode.clone(), removed_tables);

//                     if let Some(simplified) = simplified_opt {
//                         simplified_nodes.push(simplified)
//                     } else {
//                         // If subtables contain an empty table, then the join is empty
//                         return None;
//                     }
//                 }

//                 if simplified_nodes.len() == 1 {
//                     return Some(simplified_nodes.remove(0));
//                 }

//                 Some(new_tree.join(simplified_nodes, binding.clone()))
//             }
//             ExecutionOperation::Union(subnodes) => {
//                 let mut simplified_nodes = Vec::<ExecutionNodeRef>::with_capacity(subnodes.len());
//                 for subnode in subnodes {
//                     let simplified_opt =
//                         Self::simplify_recursive(new_tree, subnode.clone(), removed_tables);

//                     if let Some(simplified) = simplified_opt {
//                         simplified_nodes.push(simplified)
//                     }
//                 }

//                 if simplified_nodes.is_empty() {
//                     return None;
//                 }

//                 if simplified_nodes.len() == 1 {
//                     return Some(simplified_nodes.remove(0));
//                 }

//                 Some(new_tree.union(simplified_nodes))
//             }
//             ExecutionOperation::Minus(left, right) => {
//                 let simplified_left_opt =
//                     Self::simplify_recursive(new_tree, left.clone(), removed_tables);
//                 let simplified_right_opt =
//                     Self::simplify_recursive(new_tree, right.clone(), removed_tables);

//                 if let Some(simplified_left) = simplified_left_opt {
//                     if let Some(simplififed_right) = simplified_right_opt {
//                         return Some(new_tree.minus(simplified_left, simplififed_right));
//                     } else {
//                         return Some(simplified_left);
//                     }
//                 }

//                 None
//             }
//             ExecutionOperation::Project(subnode, reorder) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 if reorder.is_identity() {
//                     Some(simplified)
//                 } else if let ExecutionOperation::FetchExisting(id, order) =
//                     &simplified.get_rc().borrow().operation
//                 {
//                     if reorder.is_permutation() {
//                         Some(new_tree.fetch_existing_reordered(
//                             *id,
//                             order.chain_permutation(&reorder.into_permutation()),
//                         ))
//                     } else {
//                         Some(new_tree.project(simplified, reorder.clone()))
//                     }
//                 } else {
//                     Some(new_tree.project(simplified, reorder.clone()))
//                 }
//             }
//             ExecutionOperation::Filter(subnode, conditions) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 if conditions.is_empty() {
//                     Some(simplified)
//                 } else {
//                     Some(new_tree.filter_values(simplified, conditions.clone()))
//                 }
//             }
//             ExecutionOperation::SelectEqual(subnode, classes) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 if classes.is_empty() {
//                     Some(simplified)
//                 } else {
//                     Some(new_tree.select_equal(simplified, classes.clone()))
//                 }
//             }
//             ExecutionOperation::AppendColumns(subnode, instructions) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 if instructions.iter().all(|i| i.is_empty()) {
//                     Some(simplified)
//                 } else {
//                     Some(new_tree.append_columns(simplified, instructions.clone()))
//                 }
//             }
//             ExecutionOperation::AppendNulls(subnode, num_nulls) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 if *num_nulls == 0 {
//                     Some(simplified)
//                 } else {
//                     Some(new_tree.append_nulls(simplified, *num_nulls))
//                 }
//             }
//             ExecutionOperation::Subtract(subnode_main, subnodes_subtract, subtract_infos) => {
//                 let simplified_main =
//                     Self::simplify_recursive(new_tree, subnode_main.clone(), removed_tables)?;

//                 let mut simplified_infos =
//                     Vec::<SubtractInfo>::with_capacity(subnodes_subtract.len());
//                 let mut simplified_subtract_nodes =
//                     Vec::<ExecutionNodeRef>::with_capacity(subnodes_subtract.len());

//                 for (subnode, info) in subnodes_subtract.iter().zip(subtract_infos.iter()) {
//                     let simplified_opt =
//                         Self::simplify_recursive(new_tree, subnode.clone(), removed_tables);

//                     if let Some(simplified) = simplified_opt {
//                         simplified_subtract_nodes.push(simplified);
//                         simplified_infos.push(info.clone());
//                     }
//                 }

//                 if simplified_subtract_nodes.is_empty() {
//                     return Some(simplified_main);
//                 }

//                 Some(new_tree.subtract(
//                     simplified_main,
//                     simplified_subtract_nodes,
//                     simplified_infos,
//                 ))
//             }
//             ExecutionOperation::Aggregate(subnode, instructions) => {
//                 let simplified =
//                     Self::simplify_recursive(new_tree, subnode.clone(), removed_tables)?;

//                 Some(new_tree.aggregate(simplified, *instructions))
//             }
//         }
//     }

//     /// Builds a new [ExecutionTree] which does not include superfluous operations,
//     /// like, e.g., performing a join over one subtable.
//     /// Will also exclude the supplied set of temporary tables.
//     /// Returns None if the simplified tree results in a no-op.
//     pub fn simplify(&self, removed_temp_indices: &HashSet<usize>) -> Option<Self> {
//         let mut simplified_tree = ExecutionPlan::default();
//         let new_root =
//             Self::simplify_recursive(&mut simplified_tree, self.root(), removed_temp_indices)?;

//         simplified_tree.out_nodes.push(ExecutionOutNode {
//             node: new_root,
//             result: self.result().clone(),
//             name: String::from(self.name()),
//             cut_bottom_layers: self.cut_bottom(),
//         });

//         Some(ExecutionTree::new(simplified_tree))
//     }

//     /// Return a list of fetched tables that are required by this tree.
//     pub fn required_tables(&self) -> Vec<(TableId, ColumnOrder)> {
//         let mut result = Vec::<(TableId, ColumnOrder)>::new();
//         for node in &self.0.nodes {
//             if let ExecutionOperation::FetchExisting(id, order) =
//                 &node.0.as_ref().borrow().operation
//             {
//                 result.push((*id, order.clone()));
//             }
//         }

//         result
//     }
// }

// /// Functionality for ensuring certain constraints
// impl ExecutionTree {
//     /// Alters the given [ExecutionTree] in such a way as to comply with the constraints of the leapfrog trie join algorithm.
//     /// Specifically, this will reorder tables if necessary
//     pub fn satisfy_leapfrog_triejoin(&mut self) {
//         Self::satisfy_leapfrog_recursive(self.root(), Permutation::default());
//     }

//     /// Implements the functionality of satisfy_leapfrog_triejoin by traversing the tree recursively
//     fn satisfy_leapfrog_recursive(node: ExecutionNodeRef, permutation: Permutation) {
//         let node_rc = node.get_rc();
//         let node_operation = &mut node_rc.borrow_mut().operation;

//         match node_operation {
//             ExecutionOperation::FetchExisting(_id, order) => {
//                 *order = order.chain_permutation(&permutation);
//             }
//             ExecutionOperation::FetchNew(_index) => {
//                 if !permutation.is_identity() {
//                     // TODO: One cannot infer a ProjectReordering from a Permutation

//                     // let new_fetch = self.fetch_new(*index);
//                     // *node_operation = ExecutionOperation::Project(new_fetch, permutation);

//                     panic!("Automatic reordering of new tables currently not supported");
//                 }
//             }
//             ExecutionOperation::Project(subnode, project_reorder) => {
//                 *project_reorder = project_reorder.chain_permutation(&permutation);

//                 Self::satisfy_leapfrog_recursive(subnode.clone(), Permutation::default());
//             }
//             ExecutionOperation::Join(subnodes, bindings) => {
//                 bindings.apply_permutation(&permutation);

//                 let subpermutations = bindings.comply_with_leapfrog();

//                 for (subnode, subpermutation) in subnodes.iter().zip(subpermutations) {
//                     Self::satisfy_leapfrog_recursive(subnode.clone(), subpermutation)
//                 }
//             }
//             ExecutionOperation::Union(subnodes) => {
//                 for subnode in subnodes {
//                     Self::satisfy_leapfrog_recursive(subnode.clone(), permutation.clone());
//                 }
//             }
//             ExecutionOperation::Minus(left, right) => {
//                 Self::satisfy_leapfrog_recursive(left.clone(), permutation.clone());
//                 Self::satisfy_leapfrog_recursive(right.clone(), permutation);
//             }
//             ExecutionOperation::Filter(subnode, _conditions) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());

//                 Self::satisfy_leapfrog_recursive(subnode.clone(), permutation);
//             }
//             ExecutionOperation::SelectEqual(subnode, _classes) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());
//                 Self::satisfy_leapfrog_recursive(subnode.clone(), permutation);
//             }
//             ExecutionOperation::AppendColumns(subnode, _instructions) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());

//                 Self::satisfy_leapfrog_recursive(subnode.clone(), Permutation::default());
//             }
//             ExecutionOperation::AppendNulls(subnode, _) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());
//                 Self::satisfy_leapfrog_recursive(subnode.clone(), permutation.clone());
//             }
//             ExecutionOperation::Subtract(subnode_main, subnodes_subtract, _) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());

//                 Self::satisfy_leapfrog_recursive(subnode_main.clone(), permutation.clone());
//                 for subnode in subnodes_subtract {
//                     Self::satisfy_leapfrog_recursive(subnode.clone(), permutation.clone());
//                 }
//             }
//             ExecutionOperation::Aggregate(subnode, _instructions) => {
//                 // TODO: A few other changes are needed to make this a bit simpler.
//                 // Will update it then
//                 assert!(permutation.is_identity());

//                 Self::satisfy_leapfrog_recursive(subnode.clone(), Permutation::default());
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod test {
//     use std::collections::HashMap;

//     use crate::{
//         management::database::{ColumnOrder, TableId},
//         tabular::operations::{triescan_project::ProjectReordering, JoinBindings},
//     };

//     use super::{ExecutionOperation, ExecutionPlan, ExecutionTree};

//     #[test]
//     fn general_use() {
//         let mut test_plan = ExecutionPlan::default();
//         let mut current_id = TableId::default();

//         // Create a union subnode and fill it later with subnodes
//         let mut node_union_empty = test_plan.union_empty();
//         for _ in 0..3 {
//             let new_node = test_plan.fetch_existing(current_id.increment());
//             node_union_empty.add_subnode(new_node);
//         }

//         // Create a vector of subnodes and create a union node from that
//         let subnode_vec = vec![
//             test_plan.fetch_existing(current_id.increment()),
//             test_plan.fetch_existing(current_id.increment()),
//         ];
//         let node_union_vec = test_plan.union(subnode_vec);

//         // Join both unions
//         let node_join = test_plan.join(
//             vec![node_union_empty, node_union_vec],
//             JoinBindings::new(vec![vec![0, 1], vec![1, 0]]),
//         );

//         // Set the join node as an output node that will produce a temporary table
//         test_plan.write_temporary(node_join.clone(), "Computing Join");

//         // Project it the result of the join
//         let node_project_a = test_plan.project(
//             node_join.clone(),
//             ProjectReordering::from_vector(vec![0, 2], 3),
//         );

//         let node_project_b =
//             test_plan.project(node_join, ProjectReordering::from_vector(vec![2, 0], 3));

//         // Write both projections as permanent tables
//         test_plan.write_permanent(node_project_a, "Computing Projection A", "Table X");
//         test_plan.write_permanent(node_project_b, "Computing Projection B", "Table X");
//     }

//     fn compare_leapfrog_trees(
//         tree: &ExecutionTree,
//         expected_orders: &HashMap<TableId, ColumnOrder>,
//         expected_bindings: &HashMap<usize, JoinBindings>,
//     ) {
//         for (node_index, node) in tree.0.nodes.iter().enumerate() {
//             let operation = &node.0.as_ref().borrow().operation;

//             match operation {
//                 ExecutionOperation::FetchExisting(id, order) => {
//                     assert_eq!(order, expected_orders.get(id).unwrap());
//                 }
//                 ExecutionOperation::Join(_, bindings) => {
//                     assert_eq!(bindings, expected_bindings.get(&node_index).unwrap());
//                 }
//                 _ => {}
//             }
//         }
//     }

//     #[test]
//     fn test_satisfy_leapfrog_1() {
//         let mut test_plan = ExecutionPlan::default();
//         let mut current_id = TableId::default();

//         let subnode_vec = vec![
//             test_plan.fetch_existing(current_id.increment()), // X Z Y
//             test_plan.fetch_existing(current_id.increment()), // W Y Z
//         ];

//         // Order: X Y Z W
//         let join_bindings = JoinBindings::new(vec![vec![0, 2, 1], vec![3, 1, 2]]);

//         let node_join = test_plan.join(subnode_vec, join_bindings);
//         test_plan.write_temporary(node_join, "Test");

//         let mut test_tree = ExecutionTree::new(test_plan);
//         test_tree.satisfy_leapfrog_triejoin();

//         let mut current_id = TableId::default();
//         let mut expected_orders = HashMap::<TableId, ColumnOrder>::new();
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![0, 2, 1]),
//         );
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![1, 2, 0]),
//         );
//         let mut expected_bindings = HashMap::<usize, JoinBindings>::new();
//         expected_bindings.insert(2, JoinBindings::new(vec![vec![0, 1, 2], vec![1, 2, 3]]));

//         compare_leapfrog_trees(&test_tree, &expected_orders, &expected_bindings);
//     }

//     #[test]
//     fn test_satisfy_leapfrog_2() {
//         let mut test_plan = ExecutionPlan::default();
//         let mut current_id = TableId::default();

//         let subnodes_union = vec![
//             test_plan.fetch_existing_reordered(
//                 current_id.increment(),
//                 ColumnOrder::from_vector(vec![2, 0, 3, 1]),
//             ),
//             test_plan.fetch_existing_reordered(
//                 current_id.increment(),
//                 ColumnOrder::from_vector(vec![1, 3, 2, 0]),
//             ),
//         ];
//         let node_union = test_plan.union(subnodes_union);

//         let node_fetch = test_plan.fetch_existing(current_id.increment());

//         let join_bindings = JoinBindings::new(vec![vec![3, 1, 0, 2], vec![0, 2, 1]]);
//         let node_join = test_plan.join(vec![node_union, node_fetch], join_bindings);
//         test_plan.write_temporary(node_join, "Test");

//         let mut test_tree = ExecutionTree::new(test_plan);
//         test_tree.satisfy_leapfrog_triejoin();

//         let mut current_id = TableId::default();
//         let mut expected_orders = HashMap::<TableId, ColumnOrder>::new();
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![3, 0, 1, 2]),
//         );
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![2, 3, 0, 1]),
//         );
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![0, 2, 1]),
//         );
//         let mut expected_bindings = HashMap::<usize, JoinBindings>::new();
//         expected_bindings.insert(4, JoinBindings::new(vec![vec![0, 1, 2, 3], vec![0, 1, 2]]));

//         compare_leapfrog_trees(&test_tree, &expected_orders, &expected_bindings);
//     }

//     #[test]
//     fn test_satisfy_leapfrog_3() {
//         let mut test_plan = ExecutionPlan::default();
//         let mut current_id = TableId::default();

//         let node_fetch_a = test_plan.fetch_existing_reordered(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![2, 3, 0, 1]),
//         );

//         let node_fetch_b = test_plan.fetch_existing_reordered(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![2, 0, 1]),
//         );

//         let node_fetch_c = test_plan.fetch_existing_reordered(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![2, 3, 4, 0, 1]),
//         );

//         let join_bindings_ab = JoinBindings::new(vec![vec![1, 0, 3, 2], vec![3, 1, 2]]);
//         let node_join_ab = test_plan.join(vec![node_fetch_a, node_fetch_b], join_bindings_ab);

//         let join_bindings_abc = JoinBindings::new(vec![vec![4, 1, 0, 2], vec![4, 2, 1, 0, 3]]);
//         let node_join_abc = test_plan.join(vec![node_join_ab, node_fetch_c], join_bindings_abc);

//         test_plan.write_temporary(node_join_abc, "Test");

//         let mut test_tree = ExecutionTree::new(test_plan);
//         test_tree.satisfy_leapfrog_triejoin();

//         let mut current_id = TableId::default();
//         let mut expected_orders = HashMap::<TableId, ColumnOrder>::new();
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![1, 2, 0, 3]),
//         );
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![1, 0, 2]),
//         );
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![0, 4, 3, 1, 2]),
//         );

//         let mut expected_bindings = HashMap::<usize, JoinBindings>::new();
//         expected_bindings.insert(3, JoinBindings::new(vec![vec![0, 1, 2, 3], vec![0, 1, 2]]));
//         expected_bindings.insert(
//             4,
//             JoinBindings::new(vec![vec![0, 1, 2, 4], vec![0, 1, 2, 3, 4]]),
//         );

//         compare_leapfrog_trees(&test_tree, &expected_orders, &expected_bindings);
//     }

//     #[test]
//     fn test_satisfy_leapfrog_4() {
//         let mut test_plan = ExecutionPlan::default();
//         let mut current_id = TableId::default();

//         let subnode_vec = vec![
//             test_plan.fetch_existing(current_id.increment()), // A B
//             test_plan.fetch_existing(current_id.increment()), // A C
//             test_plan.fetch_existing(current_id.increment()), // D B C
//             test_plan.fetch_existing(current_id.increment()), // D
//         ];

//         // Order: A B C D
//         let join_bindings = JoinBindings::new(vec![vec![0, 1], vec![0, 2], vec![3, 1, 2], vec![3]]);

//         let node_join = test_plan.join(subnode_vec, join_bindings);
//         test_plan.write_temporary(node_join, "Test");

//         let mut test_tree = ExecutionTree::new(test_plan);
//         test_tree.satisfy_leapfrog_triejoin();

//         let mut current_id = TableId::default();
//         let mut expected_orders = HashMap::<TableId, ColumnOrder>::new();
//         expected_orders.insert(current_id.increment(), ColumnOrder::from_vector(vec![0, 1]));
//         expected_orders.insert(current_id.increment(), ColumnOrder::from_vector(vec![0, 1]));
//         expected_orders.insert(
//             current_id.increment(),
//             ColumnOrder::from_vector(vec![1, 2, 0]),
//         );
//         expected_orders.insert(current_id.increment(), ColumnOrder::from_vector(vec![0]));

//         let mut expected_bindings = HashMap::<usize, JoinBindings>::new();
//         expected_bindings.insert(
//             4,
//             JoinBindings::new(vec![vec![0, 1], vec![0, 2], vec![1, 2, 3], vec![3]]),
//         );

//         compare_leapfrog_trees(&test_tree, &expected_orders, &expected_bindings);
//     }
// }
