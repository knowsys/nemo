//! Structure that represents a series of database operations
use std::ops::Range;

use super::{model::Identifier, table_manager::ColumnOrder};

/// Represents a database operation that should be performed
#[derive(Debug, Clone)]
pub enum ExecutionOperation {
    /// Represents a table that may exist in the table manager
    Fetch(Identifier, Range<usize>, ColumnOrder),
    /// Use temporary table of last round (TODO for later)
    Temp(),
    /// Join operation
    Join(Vec<ExecutionNode>, Vec<Vec<usize>>),
    /// Union operation
    Union(Vec<ExecutionNode>),
    /// Table difference operation
    Minus(Vec<ExecutionNode>),
    /// Table project operation, takes the temporary table as input
    Project(Vec<usize>),
}

/// Represents a node in the operation tree of a [`ExecutionPlan`]
#[derive(Debug, Clone)]
pub struct ExecutionNode {
    /// Operation corresponding to the trie iterator type that should be used
    pub operation: ExecutionOperation,
}

/// Represents the plan for calculating a table
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Root of the operation tree
    pub roots: Vec<ExecutionNode>,
    /// Reference to all the leave nodes
    pub leaves: Vec<ExecutionNode>,
}

#[cfg(test)]
mod test {}
