//! Exposes supported aggregate operations and allows created the associated processors

use crate::datatypes::DataTypeName;

use super::processors::{
    aggregate::Aggregate, count_aggregate::CountAggregateProcessor,
    max_aggregate::MaxAggregateProcessor, min_aggregate::MinAggregateProcessor,
    processor::AggregateProcessor, sum_aggregate::SumAggregateProcessor,
};

#[derive(Clone, Copy, Debug, PartialEq)]
/// Aggregate operations supported by the physical layer
pub enum AggregateOperation {
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Sum of all values
    Sum,
    /// Count of values
    Count,
}

impl AggregateOperation {
    /// Creates a new aggregate processor for the given aggregate operation.
    /// TODO: This is currently implemented using dynamic dispatch, but this may change in the future.
    pub fn create_processor<A: Aggregate>(&self) -> Box<dyn AggregateProcessor<A>> {
        match self {
            AggregateOperation::Count => Box::new(CountAggregateProcessor::new()),
            AggregateOperation::Max => Box::new(MaxAggregateProcessor::new()),
            AggregateOperation::Min => Box::new(MinAggregateProcessor::new()),
            AggregateOperation::Sum => Box::new(SumAggregateProcessor::new()),
        }
    }

    /// Returns whether the aggregate operation always produces an aggregate output column of the same type.
    /// If [`Some`] is returned, this is the static output type of the aggregate operation.
    /// If [`None`] is returns, the aggregate operation will always have the same output and input type.
    pub fn static_output_type(&self) -> Option<DataTypeName> {
        match self {
            AggregateOperation::Count => Some(DataTypeName::I64),
            _ => None,
        }
    }
}
