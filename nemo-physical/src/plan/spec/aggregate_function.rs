//! Aggregate functions: what an aggregate computes, and the columns it reads.

use crate::{execution::AggregateKind, plan::Marker};

/// What an aggregate computes, with the columns it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    /// The number of distinct tuples of the aggregate's
    /// [per](super::AggregateColumnSpec::per) markers.
    /// It reads no value of its own.
    Count,
    /// The sum of a column's values, which are numbers or durations of one kind,
    /// exact over integers and decimals.
    Sum(Marker),
    /// The smallest value of a column.
    /// Values compare by value if they are comparable, and by their order as terms otherwise.
    Min(Marker),
    /// The largest value of a column.
    /// Values compare by value if they are comparable, and by their order as terms otherwise.
    Max(Marker),
    /// The average of a column's values, which are numbers or durations of one kind:
    /// their sum divided by their count, exactly over integers and decimals.
    Average(Marker),
    /// A column's values as a tuple, in ascending term order.
    Tuple(Marker),
    /// The pairs of two columns as a map.
    /// A key with two different values fails the group.
    Map {
        /// The column of the keys.
        keys: Marker,
        /// The column of the values.
        values: Marker,
    },
}

impl AggregateFunction {
    /// Returns the kind of the function.
    pub(crate) fn kind(&self) -> AggregateKind {
        match self {
            AggregateFunction::Count => AggregateKind::Count,
            AggregateFunction::Sum(_) => AggregateKind::Sum,
            AggregateFunction::Min(_) => AggregateKind::Min,
            AggregateFunction::Max(_) => AggregateKind::Max,
            AggregateFunction::Average(_) => AggregateKind::Average,
            AggregateFunction::Tuple(_) => AggregateKind::Tuple,
            AggregateFunction::Map { .. } => AggregateKind::Map,
        }
    }

    /// Returns the columns the function reads: its column and `None`,
    /// the keys and the values for [AggregateFunction::Map],
    /// and `None` twice for [AggregateFunction::Count].
    pub(crate) fn columns(&self) -> (Option<Marker>, Option<Marker>) {
        match *self {
            AggregateFunction::Count => (None, None),
            AggregateFunction::Sum(value)
            | AggregateFunction::Min(value)
            | AggregateFunction::Max(value)
            | AggregateFunction::Average(value)
            | AggregateFunction::Tuple(value) => (Some(value), None),
            AggregateFunction::Map { keys, values } => (Some(keys), Some(values)),
        }
    }
}
