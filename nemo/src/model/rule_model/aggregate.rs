use crate::model::{types::error::TypeError, PrimitiveType, VariableAssignment};

use super::{Identifier, PrimitiveTerm, Term};

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

impl LogicalAggregateOperation {
    /// Validates whether the logical aggregate operation is allowed on a particular type of aggregated input column
    pub fn check_input_type(
        &self,
        input_variable_name: &str,
        input_type: PrimitiveType,
    ) -> Result<(), TypeError> {
        if self.requires_numerical_input()
            && !(matches!(input_type, PrimitiveType::Float64 | PrimitiveType::Integer))
        {
            return Err(TypeError::NonNumericalAggregateInputType(
                *self,
                input_variable_name.to_owned(),
                input_type,
            ));
        }

        Ok(())
    }

    fn requires_numerical_input(&self) -> bool {
        match self {
            LogicalAggregateOperation::CountValues => false,
            LogicalAggregateOperation::MinNumber => true,
            LogicalAggregateOperation::MaxNumber => true,
            LogicalAggregateOperation::SumOfNumbers => true,
        }
    }
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
    pub(crate) terms: Vec<PrimitiveTerm>,
}

impl Aggregate {
    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        for term in &mut self.terms {
            if let PrimitiveTerm::Variable(variable) = term {
                if let Some(value) = assignment.get(variable) {
                    if let Term::Primitive(PrimitiveTerm::Variable(replacing_variable)) = value {
                        *variable = replacing_variable.clone();
                    }
                }
            }
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
