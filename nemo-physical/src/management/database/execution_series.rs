//! This module defines [ExecutionSeries],
//! which defines a series of database operations
//! that can be executed by the [DatabaseInstance][super::DatabaseInstance].

use std::fmt::Debug;

use crate::{
    management::execution_plan::{ColumnOrder, ExecutionResult},
    tabular::operations::{
        OperationGeneratorEnum,
        projectreorder::{GeneratorProjectReorder, ProjectReordering},
        single::GeneratorSingle,
    },
};

use super::id::{ExecutionId, PermanentTableId};

pub(crate) type LoadedTableId = usize;
pub(crate) type ComputedTableId = usize;

/// Leaf node of an [ExecutionTree]
#[derive(Debug)]
pub(crate) enum ExecutionTreeLeaf {
    /// Table was already in the data base
    LoadTable(LoadedTableId),
    /// Table was computed during the execution of the execution plan
    FetchComputedTable(ComputedTableId),
}

/// Possible operation in an [ExecutionTree]
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
pub(crate) enum ExecutionTreeNode {
    Operation(ExecutionTreeOperation),
    ProjectReorder {
        generator: GeneratorProjectReorder,
        subnode: ExecutionTreeLeaf,
    },
    Single {
        generator: GeneratorSingle,
        subnode: ExecutionTreeOperation,
    },
}

impl ExecutionTreeNode {
    /// Returns [ExecutionTreeOperation] if this is not a project node.
    ///
    /// Returns `None` otherwise.
    pub(crate) fn operation(self) -> Option<ExecutionTreeOperation> {
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
    pub root: ExecutionTreeNode,
    /// How the result of this operation will be stored
    pub result: ExecutionResult,
    /// [ExecutionId] to associate this tree to its original node in the plan
    pub id: ExecutionId,
    /// Name of the of tree, e.g. for debugging purposes
    pub operation_name: String,

    /// References to other [ExecutionTree]s that have the same content
    /// as this but whose columns are reordered according to the given [ProjectReordering]
    pub dependents: Vec<(ComputedTableId, ProjectReordering)>,
    /// Whether the resulting table is used anywhere
    pub used: usize,

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
                let subtrees = subnodes.iter().map(Self::ascii_tree_recursive).collect();

                ascii_tree::Tree::Node(format!("{generator:?}"), subtrees)
            }
        }
    }

    /// Return an ascii tree representation of the [ExecutionTree].
    pub(crate) fn ascii_tree(&self) -> ascii_tree::Tree {
        let tree = match &self.root {
            ExecutionTreeNode::Operation(operation_tree) => {
                Self::ascii_tree_recursive(operation_tree)
            }
            ExecutionTreeNode::ProjectReorder { generator, subnode } => {
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
            ExecutionTreeNode::Single { generator, subnode } => {
                let subnode_tree = Self::ascii_tree_recursive(subnode);

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

/// Step-by-steps instructions realizing a series of complex operations
#[derive(Debug)]
pub(crate) struct ExecutionSeries {
    /// Tables that need to be present in memory before executing this series
    pub loaded_tries: Vec<(PermanentTableId, ColumnOrder)>,
    /// List of execution trees that makes up this series
    pub trees: Vec<ExecutionTree>,
}
