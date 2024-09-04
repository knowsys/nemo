use crate::model::VariableAssignment;

use super::{Identifier, Term};

/// Aggregate operation on logical values
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogicalAggregateOperation {
    /// Count of distinct values
    CountValues,
    /// Minimum numerical value
    MinNumber,
    /// Maximum numerical value
    MaxNumber,
    /// Sum of numerical values
    SumOfNumbers,
}

impl From<&Identifier> for Option<LogicalAggregateOperation> {
    fn from(value: &Identifier) -> Self {
        match value.name().as_str() {
            "count" => Some(LogicalAggregateOperation::CountValues),
            "min" => Some(LogicalAggregateOperation::MinNumber),
            "max" => Some(LogicalAggregateOperation::MaxNumber),
            "sum" => Some(LogicalAggregateOperation::SumOfNumbers),
            _ => None,
        }
    }
}

/// Aggregate occurring in a predicate in the head
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Aggregate {
    pub(crate) logical_aggregate_operation: LogicalAggregateOperation,
    pub(crate) terms: Vec<Term>,
}

impl Aggregate {
    /// Replaces [super::Variable]s with [Term]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        for term in &mut self.terms {
            term.apply_assignment(assignment);
        }
    }
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{:?}({:?})",
            self.logical_aggregate_operation, self.terms
        )
    }
}
