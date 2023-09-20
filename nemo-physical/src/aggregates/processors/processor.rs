//! Traits for implementing new aggregate operations

use crate::datatypes::StorageValueT;

use super::aggregate::Aggregate;

/// Allows for aggregation of a column, by providing [`AggregateGroupProcessor`] for every group in the input trie scan.
pub trait AggregateProcessor<A: Aggregate> {
    /// Returns whether the aggregate processor is invariant to being called with the same aggregated value multiple times in a row.
    /// This function has to return the same value independent of the aggregated value type.
    ///
    /// If `true` is returned this allows for additional optimizations when creating the execution plan (e.g. not needing to reorder if the distinct variables are in the wrong variable order).
    fn idempotent(&self) -> bool;

    /// Creates a [`AggregateGroupProcessor`] for aggregating values with the same values in group-by columns.
    fn group(&self) -> Box<dyn AggregateGroupProcessor<A>>;
}

/// Allows aggregation of multiple rows (all with the same group-by values) to produce a single aggregate value.
pub trait AggregateGroupProcessor<A>
where
    A: Aggregate,
{
    /// Processes a row of the aggregated input column and updates the internal state.
    fn write_aggregate_input_value(&mut self, value: A);

    /// Returns the resulting aggregated value of all the processed input values.
    fn finish(&self) -> Option<StorageValueT>;
}
