//! Structure that represents a series of database operations
use std::ops::Range;

use crate::physical::tabular::operations::triescan_select::ValueAssignment;

use super::{
    model::Identifier,
    table_manager::{ColumnOrder, TableId},
};

/// Represents a database operation that should be performed
#[derive(Debug, Clone)]
pub enum ExecutionOperation {
    /// Represents a table that may exist in the table manager
    Fetch(Identifier, Range<usize>, ColumnOrder),
    /// Use temporary table with the (temporary) id
    Temp(TableId),
    /// Join operation
    Join(Vec<ExecutionNode>, Vec<Vec<usize>>),
    /// Union operation
    Union(Vec<ExecutionNode>),
    /// Table difference operation
    Minus(Box<ExecutionNode>, Box<ExecutionNode>),
    /// Table project operation, takes the temporary table as input
    Project(TableId, ColumnOrder),
    /// Only leave entries in that have a certain value
    SelectValue(Box<ExecutionNode>, Vec<ValueAssignment>),
    /// Only leave entries in that contain equal values in certain columns
    SelectEqual(Box<ExecutionNode>, Vec<Vec<usize>>),
}

/// Represents a node in the operation tree of a [`ExecutionPlan`]
#[derive(Debug, Clone)]
pub struct ExecutionNode {
    /// Operation corresponding to the trie iterator type that should be used
    pub operation: ExecutionOperation,
}

impl ExecutionNode {
    /// Create new [`ExecutionNode`]
    pub fn new(operation: ExecutionOperation) -> Self {
        Self { operation }
    }
}

/// Declares whether the resulting table form executing a plan should be kept temporarily or permamently
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    /// Temporary table with the id
    Temp(TableId),
    /// Permanent table with the following identifier, range, column order and priority
    Save(Identifier, Range<usize>, ColumnOrder, u64),
}

/// Represents the plan for calculating a table
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Root of the operation tree
    pub root: ExecutionNode,
    /// Reference to all the leave nodes
    pub leaves: Vec<ExecutionNode>,
    /// How to save the resulting table
    pub result: ExecutionResult,
}

/// A series of execution plans
/// Usually contains the information necessary for evaluating one rule
#[derive(Debug)]
pub struct ExecutionSeries {
    /// The individual plans in the series
    pub plans: Vec<ExecutionPlan>,

    /// Tables for which to update big table step
    pub big_table: Vec<Identifier>,
}

#[cfg(test)]
mod test {}
