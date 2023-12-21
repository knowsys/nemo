//! This module defines the trait [IntervalLookup] and [IntervalLookupBuilder],
//! and provides several implementations of them.

use std::{fmt::Debug, ops::Range};

use crate::management::ByteSized;

pub(crate) mod lookup_bitvector;
pub(crate) mod lookup_column_dual;
pub(crate) mod lookup_column_single;

/// Trait for looking up interval bounds in [IntervalColumn][super::super::intervalcolumn::IntervalColumn]
pub(crate) trait IntervalLookup: Debug + Clone + ByteSized {
    /// [IntervalLookupBuilder] type for building objects that implement this trait
    type Builder: IntervalLookupBuilder<Lookup = Self>;

    /// Given an index of a value from the previous layer,
    /// return a range of indices representing the slice in the data column
    /// that is associated with the successors of that value.
    ///
    /// Returns `None` if the value has no successor in this interval column.
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>>;
}

/// Trait for constructing [IntervalLookup] objects
pub(crate) trait IntervalLookupBuilder: Debug + Default {
    /// [IntervalLookup] that will be built
    type Lookup: IntervalLookup<Builder = Self>;

    /// Add a new interval.
    ///
    /// The length of the interval will be determined by the difference between
    /// the size of the associated data column from the last call and this call.
    ///
    /// A difference of 0 means that no new interval will be created.
    fn add(&mut self, data_count: usize);

    /// Return the constructed [IntervalLookup].
    fn finalize(self) -> Self::Lookup;
}
