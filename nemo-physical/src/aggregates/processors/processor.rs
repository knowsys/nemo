//! Traits for implementing new aggregate operations

use crate::datatypes::StorageValueT;

use super::{
    aggregate::Aggregate,
    count_aggregate::{CountAggregateGroupProcessor, CountAggregateProcessor},
    max_aggregate::{MaxAggregateGroupProcessor, MaxAggregateProcessor},
    min_aggregate::{MinAggregateGroupProcessor, MinAggregateProcessor},
    sum_aggregate::{SumAggregateGroupProcessor, SumAggregateProcessor},
};

use enum_dispatch::enum_dispatch;

/// Allows for aggregation of a column, by providing [`AggregateGroupProcessor`] for every group in the input trie scan.
#[enum_dispatch]
pub(crate) trait AggregateProcessor<A: Aggregate> {
    /// Returns whether the aggregate processor is invariant to being called with the same aggregated value multiple times in a row.
    /// This function has to return the same value independent of the aggregated value type.
    ///
    /// If `true` is returned this allows for additional optimizations when creating the execution plan. In particular, peripheral variables (not group-by, aggregate or distinct variables) can be converted to distinct variables in an idempotent aggregate processor without changing the semantics of the aggregate.
    ///
    /// See [`super::super::operation::AggregateOperation::idempotent`]
    fn idempotent(&self) -> bool;

    /// Creates a [`AggregateGroupProcessor`] for aggregating values with the same values in group-by columns.
    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>>;
}

#[enum_dispatch(AggregateProcessor<A>)]
#[derive(Debug)]
pub(crate) enum AggregateProcessorT<A: Aggregate> {
    Count(CountAggregateProcessor<A>),
    Max(MaxAggregateProcessor<A>),
    Min(MinAggregateProcessor<A>),
    Sum(SumAggregateProcessor<A>),
}

/// Allows aggregation of multiple rows (all with the same group-by values) to produce a single aggregate value.
pub(crate) trait AggregateGroupProcessor<A>
where
    A: Aggregate,
{
    /// Processes a row of the aggregated input column and updates the internal state.
    fn write_aggregate_input_value(&mut self, value: A);

    /// Returns the resulting aggregated value of all the processed input values.
    fn finish(&self) -> Option<StorageValueT>;
}

#[enum_dispatch(AggregateGroupProcessor<A>)]
#[derive(Debug)]
pub(crate) enum AggregateGroupProcessorT<A: Aggregate> {
    Count(CountAggregateGroupProcessor<A>),
    Max(MaxAggregateGroupProcessor<A>),
    Min(MinAggregateGroupProcessor<A>),
    Sum(SumAggregateGroupProcessor<A>),
}
