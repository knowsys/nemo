use nemo_physical::aggregates::operation::AggregateOperation;

use crate::model::{Aggregate, LogicalAggregateOperation, PrimitiveTerm, Variable};

/// Specifies how the values for a placeholder aggregate variable will get computed.
///
/// Terminology:
/// * `input_variables` are the distinct variables and the aggregated input variable, not including the group-by variables
/// * `output_variable` is the single aggregated output variable
///
/// See [`nemo_physical::tabular::operations::TrieScanAggregate`]
#[derive(Debug, Clone)]
pub struct ChaseAggregate {
    pub(crate) aggregate_operation: AggregateOperation,
    pub(crate) logical_aggregate_operation: LogicalAggregateOperation,

    pub(crate) input_variables: Vec<Variable>,
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
            input_variables: variables,
            output_variable,
        }
    }

    /// Return the aggregated input variable, which is the first of the input variables
    pub fn aggregated_input_variable(&self) -> &Variable {
        self.input_variables
            .first()
            .expect("aggregates require exactly at least one input variable")
    }
}
