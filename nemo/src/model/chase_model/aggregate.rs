use nemo_physical::aggregates::operation::AggregateOperation;

use crate::model::{Aggregate, LogicalAggregateOperation, PrimitiveTerm, Variable};

/// Specifies how the values for a placeholder aggregate variable will get computed.
#[derive(Debug, Clone)]
pub struct ChaseAggregate {
    pub(crate) aggregate_operation: AggregateOperation,
    pub(crate) logical_aggregate_operation: LogicalAggregateOperation,

    pub(crate) variables: Vec<Variable>,
    pub(crate) output_variable: Variable,
}

impl ChaseAggregate {
    /// Convert an [`Aggregate`] to a [`ChaseAggregate`], given a placeholder name for the output variable
    pub fn from_aggregate(aggregate: Aggregate, output_variable: Variable) -> ChaseAggregate {
        let logical_aggregate_operation = aggregate.logical_aggregate_operation;

        let physical_operation = match logical_aggregate_operation {
            LogicalAggregateOperation::CountValues => AggregateOperation::Count,
            LogicalAggregateOperation::MaxNumber => AggregateOperation::Max,
            LogicalAggregateOperation::MinNumber => AggregateOperation::Min,
            LogicalAggregateOperation::SumOfNumbers => AggregateOperation::Sum,
        };

        let variables = aggregate
            .terms
            .into_iter()
            .map(|t| {
                if let PrimitiveTerm::Variable(variable) = t {
                    variable
                } else {
                    unreachable!("Non-variable terms are not allowed in aggregates.");
                }
            })
            .collect();

        Self {
            aggregate_operation: physical_operation,
            logical_aggregate_operation,
            variables,
            output_variable,
        }
    }
}
