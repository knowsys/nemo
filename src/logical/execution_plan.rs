use crate::physical::tables::TrieSchema;

use super::table_manager::{TableId, TableStatus};

/// Represents a database operation that should be performed
#[derive(Debug)]
pub enum ExecutionOperation {
    /// Represents a table that is present within the tablemanager and can be obtained
    Fetch(TableId),
    /// Join operation
    Join(Vec<ExecutionPlan>, TrieSchema),
    /// Union operation
    Union(Vec<ExecutionPlan>),
    /// Table difference operation
    Minus(Vec<ExecutionPlan>),
    /// Table project operation
    Project(TableId, Vec<usize>),
}

/// Represents the plan for calculating another table
#[derive(Debug)]
pub struct ExecutionPlan {
    pub operation: ExecutionOperation,
    pub target_status: TableStatus,
    pub target_priority: u64,
}

#[cfg(test)]
mod test {}
