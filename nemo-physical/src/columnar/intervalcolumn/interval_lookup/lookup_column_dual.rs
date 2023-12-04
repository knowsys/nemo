//! This module implements [IntervalLookupColumnDual]
//! and the associated builder [IntervalLookupColumnDualBuilder].

use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::{adaptive::ColumnBuilderAdaptive, ColumnBuilder},
    },
    management::ByteSized,
};

use super::{IntervalLookup, IntervalLookupBuilder};

/// Implementation of [IntervalLookup],
/// which internally uses a [ColumnEnum] to store the starting indices of the intervals of the data column
/// and another [ColumnEnum] to associate them with entries from the previous layer
#[derive(Debug, Clone)]
pub struct IntervalLookupColumnDual {
    /// [Column][crate::columnar::column::Column] that stores
    /// the start indices of intervals in the associated data column,
    /// which contain the successor nodes of the values from the previous layer
    ///
    /// This column contains exactly one entry
    /// for every interval in the associated data column.
    ///
    /// The length of an interval can be obtained
    /// subtracting the starting point of the current interval
    /// with the start point of the next interval.
    ///
    /// The last entry in this column is always the length of the
    /// data column associated with this lookup object.
    ///
    /// Note that this is a fully sorted column.
    interval_starts: ColumnEnum<usize>,

    /// [Column][crate::columnar::column::Column]
    /// for associating each node of the previous layer
    /// with an interval in `interval_starts`
    ///
    /// An entry in this column might be `Self::Empty`
    /// to indicate that the corresponding node from the previous layer
    /// has no successor.
    predecessors: ColumnEnum<usize>,
}

impl IntervalLookupColumnDual {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;
}

impl IntervalLookup for IntervalLookupColumnDual {
    type Builder = IntervalLookupColumnDualBuilder;

    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        let interval_index = self.predecessors.get(index);

        if interval_index == Self::EMPTY {
            return None;
        }

        let interval_start = self.interval_starts.get(interval_index);
        let interval_end = self.interval_starts.get(interval_index + 1);

        Some(interval_start..interval_end)
    }
}

impl ByteSized for IntervalLookupColumnDual {
    fn size_bytes(&self) -> ByteSize {
        self.interval_starts.size_bytes() + self.predecessors.size_bytes()
    }
}

#[derive(Debug, Default)]
pub struct IntervalLookupColumnDualBuilder {
    /// [ColumnBuilderAdaptive] for building `interval_starts`
    builder_intervals: ColumnBuilderAdaptive<usize>,
    /// [ColumnBuilderAdaptive] for building `predecessors`
    builder_predecessors: ColumnBuilderAdaptive<usize>,

    /// Parameter passed to `self.add` in the last call
    last_data_count: usize,
}

impl IntervalLookupBuilder for IntervalLookupColumnDualBuilder {
    type Lookup = IntervalLookupColumnDual;

    fn add(&mut self, data_count: usize) {
        if data_count != self.last_data_count {
            self.builder_predecessors
                .add(self.builder_intervals.count());
            self.builder_intervals.add(self.last_data_count);
        } else {
            self.builder_predecessors.add(Self::Lookup::EMPTY);
        }
    }

    fn finalize(mut self) -> Self::Lookup {
        self.builder_intervals.add(self.last_data_count);

        Self::Lookup {
            interval_starts: self.builder_intervals.finalize(),
            predecessors: self.builder_predecessors.finalize(),
        }
    }
}
