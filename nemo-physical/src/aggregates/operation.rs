//! Exposes supported aggregate operations and allows created the associated processors

use crate::datatypes::StorageTypeName;

use super::processors::{
    count_aggregate::CountAggregateProcessor,
    max_aggregate::MaxAggregateProcessor,
    min_aggregate::MinAggregateProcessor,
    processor::{AggregateProcessor, AggregateProcessorT},
    sum_aggregate::SumAggregateProcessor,
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
    pub(crate) fn create_processor(&self) -> AggregateProcessorT {
        let aggregate_processor: AggregateProcessorT = match self {
            AggregateOperation::Count => CountAggregateProcessor::new().into(),
            AggregateOperation::Max => MaxAggregateProcessor::new().into(),
            AggregateOperation::Min => MinAggregateProcessor::new().into(),
            AggregateOperation::Sum => SumAggregateProcessor::new().into(),
        };

        aggregate_processor
    }

    /// Returns whether the aggregate processor is invariant to being called with the same aggregated value multiple times in a row.
    /// This function has to return the same value independent of the aggregated value type.
    ///
    /// If `true` is returned this allows for additional optimizations when creating the execution plan. In particular, peripheral variables (not group-by, aggregate or distinct variables) can be converted to distinct variables in an idempotent aggregate processor without changing the semantics of the aggregate.
    pub fn idempotent(&self) -> bool {
        self.create_processor().idempotent()
    }

    /// Returns whether the aggregate operation always produces an aggregate output column of the same type.
    /// If [`Some`] is returned, this is the static output type of the aggregate operation.
    /// If [`None`] is returned, the aggregate operation will always have the same output and input type.
    pub(crate) fn static_output_type(&self) -> Option<StorageTypeName> {
        match self {
            AggregateOperation::Count => Some(StorageTypeName::Int64),
            _ => None,
        }
    }
}
