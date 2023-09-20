use nemo_physical::aggregates::operation::AggregateOperation;

use crate::model::{Aggregate, Identifier, LogicalAggregateOperation};

/// Specifies how the values for a placeholder aggregate variable will get computed.
#[derive(Debug, Clone)]
pub struct ChaseAggregate {
    pub(crate) aggregate_operation: AggregateOperation,
    pub(crate) logical_aggregate_operation: LogicalAggregateOperation,

    pub(crate) variable_identifiers: Vec<Identifier>,
    /// Name of placeholder variable for the aggregate, which will get used in the corresponding head atom.
    pub(crate) output_variable: Identifier,
}

impl ChaseAggregate {
    #[allow(missing_docs)]
    pub fn from_aggregate(aggregate: Aggregate, output_variable: Identifier) -> ChaseAggregate {
        let logical_aggregate_operation = aggregate.logical_aggregate_operation;

        let physical_operation = match logical_aggregate_operation {
            LogicalAggregateOperation::CountValues => AggregateOperation::Count,
            LogicalAggregateOperation::MaxNumber => AggregateOperation::Max,
            LogicalAggregateOperation::MinNumber => AggregateOperation::Min,
            LogicalAggregateOperation::SumOfNumbers => AggregateOperation::Sum,
        };

        Self {
            aggregate_operation: physical_operation,
            logical_aggregate_operation,
            variable_identifiers: aggregate.variable_identifiers,
            output_variable,
        }
    }
}
