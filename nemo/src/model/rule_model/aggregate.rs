use crate::model::{types::error::TypeError, PrimitiveType};

use super::{Identifier, Variable};

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
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Aggregate {
    pub(crate) logical_aggregate_operation: LogicalAggregateOperation,
    pub(crate) variable_identifiers: Vec<Identifier>,
}

impl Aggregate {
    /// Substitutes all occurrences of `var` for `subst`.
    pub fn substitute_variable(&mut self, var: &Variable, subst: &Variable) {
        let Variable::Universal(var) = var else {
            panic!("cannot substitute existential variable")
        };

        let Variable::Universal(subst) = subst else {
            panic!("cannot substitute with existential variable")
        };

        for id in &mut self.variable_identifiers {
            if id == var {
                *id = subst.clone();
            }
        }
    }
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{:?}({:?})",
            self.logical_aggregate_operation, self.variable_identifiers
        )
    }
}
