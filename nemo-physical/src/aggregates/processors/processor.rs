//! Traits for implementing new aggregate operations

use std::cmp::Ordering;

use crate::datatypes::{Double, StorageValueT};

use super::{
    count_aggregate::CountAggregateProcessor, max_aggregate::MaxAggregateProcessor,
    min_aggregate::MinAggregateProcessor, sum_aggregate::SumAggregateProcessor,
};

use enum_dispatch::enum_dispatch;

/// Allows for aggregation of a column, by providing [AggregateGroupProcessor] for every group in the input trie scan.
#[enum_dispatch]
pub(crate) trait AggregateProcessor {
    /// Returns whether the aggregate processor is invariant to being called with the same aggregated value multiple times in a row.
    /// This function has to return the same value independent of the aggregated value type.
    ///
    /// If `true` is returned this allows for additional optimizations when creating the execution plan. In particular, peripheral variables (not group-by, aggregate or distinct variables) can be converted to distinct variables in an idempotent aggregate processor without changing the semantics of the aggregate.
    ///
    /// See [super::super::operation::AggregateOperation::idempotent]
    fn idempotent(&self) -> bool;

    /// Creates a [AggregateGroupProcessor] for aggregating values with the same values in group-by columns.
    fn group(&self) -> Box<dyn AggregateGroupProcessor>;
}

#[enum_dispatch(AggregateProcessor)]
#[derive(Debug)]
pub(crate) enum AggregateProcessorT {
    Count(CountAggregateProcessor),
    Max(MaxAggregateProcessor),
    Min(MinAggregateProcessor),
    Sum(SumAggregateProcessor),
}

/// Allows aggregation of multiple rows (all with the same group-by values) to produce a single aggregate value.
pub(crate) trait AggregateGroupProcessor {
    /// Processes a row of the aggregated input column and updates the internal state.
    ///
    /// [StorageValueT] needs to implement the following:
    ///
    /// [num::CheckedAdd] is required for sum aggregates.
    /// [Clone] and [`Into<StorageValueT>`] is required to return a storage value in `finish` function in min/max/sum aggregates.
    /// [Debug] for debugging
    /// [Default] is required to initialize the aggregator in sum aggregates.
    /// [PartialOrd] is required for min/max aggregates.
    /// `'static` is required to store the value e.g. in min/max aggregates.
    fn write_aggregate_input_value(&mut self, value: StorageValueT);

    /// Returns the resulting aggregated value of all the processed input values.
    fn finish(&self) -> Option<StorageValueT>;
}

fn as_double(value: StorageValueT) -> Option<Double> {
    match value {
        StorageValueT::Int64(value) => Some(Double::from_number(value as f64)),
        StorageValueT::Float(value) => Some(Double::from_number(Into::<f32>::into(value) as f64)),
        StorageValueT::Double(value) => Some(value),
        _ => None,
    }
}

pub(super) fn select_extremum(
    current: StorageValueT,
    value: StorageValueT,
    ordering: Ordering,
) -> StorageValueT {
    if current.get_type() != value.get_type()
        && let (Some(current), Some(value)) = (as_double(current), as_double(value))
    {
        return StorageValueT::Double(if value.cmp(&current) == ordering {
            value
        } else {
            current
        });
    }

    if value.cmp(&current) == ordering {
        value
    } else {
        current
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        aggregates::processors::{
            max_aggregate::MaxAggregateGroupProcessor, min_aggregate::MinAggregateGroupProcessor,
        },
        datatypes::Float,
    };

    fn aggregate(
        mut processor: impl AggregateGroupProcessor,
        values: [StorageValueT; 2],
    ) -> Option<StorageValueT> {
        for value in values {
            processor.write_aggregate_input_value(value);
        }

        processor.finish()
    }

    fn assert_extrema(
        first: StorageValueT,
        second: StorageValueT,
        expected_min: StorageValueT,
        expected_max: StorageValueT,
    ) {
        for values in [[first, second], [second, first]] {
            assert_eq!(
                aggregate(MinAggregateGroupProcessor::new(), values),
                Some(expected_min)
            );
            assert_eq!(
                aggregate(MaxAggregateGroupProcessor::new(), values),
                Some(expected_max)
            );
        }
    }

    #[test]
    fn mixed_numeric_extrema_use_numeric_order() {
        let int_1 = StorageValueT::Int64(1);
        let int_10 = StorageValueT::Int64(10);
        let float_1 = StorageValueT::Float(Float::from_number(1.0));
        let float_10 = StorageValueT::Float(Float::from_number(10.0));
        let double_1 = StorageValueT::Double(Double::from_number(1.0));
        let double_10 = StorageValueT::Double(Double::from_number(10.0));

        assert_extrema(int_10, float_1, double_1, double_10);
        assert_extrema(int_1, float_10, double_1, double_10);
        assert_extrema(int_10, double_1, double_1, double_10);
        assert_extrema(int_1, double_10, double_1, double_10);
        assert_extrema(float_10, double_1, double_1, double_10);
        assert_extrema(float_1, double_10, double_1, double_10);
    }

    #[test]
    fn same_type_extrema_preserve_type() {
        assert_extrema(
            StorageValueT::Int64(10),
            StorageValueT::Int64(1),
            StorageValueT::Int64(1),
            StorageValueT::Int64(10),
        );
        assert_extrema(
            StorageValueT::Float(Float::from_number(10.0)),
            StorageValueT::Float(Float::from_number(1.0)),
            StorageValueT::Float(Float::from_number(1.0)),
            StorageValueT::Float(Float::from_number(10.0)),
        );
        assert_extrema(
            StorageValueT::Double(Double::from_number(10.0)),
            StorageValueT::Double(Double::from_number(1.0)),
            StorageValueT::Double(Double::from_number(1.0)),
            StorageValueT::Double(Double::from_number(10.0)),
        );
    }
}
