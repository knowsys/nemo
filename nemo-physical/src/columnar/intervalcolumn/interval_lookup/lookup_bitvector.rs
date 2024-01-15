//! This module implements [IntervalLookupBitVector]
//! and the associated builder [IntervalLookupBitVectorBuilder].

use std::{mem::size_of, ops::Range};

use bitvec::vec::BitVec;
use bytesize::ByteSize;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::{adaptive::ColumnBuilderAdaptive, ColumnBuilder},
    },
    management::bytesized::ByteSized,
};

use super::{IntervalLookup, IntervalLookupBuilder};

/// Implementation of [IntervalLookup],
/// which internally uses a [ColumnEnum]
/// to store the indices of the successor intervals
/// and a [BitVec] to store which nodes
/// of the previous layers have successors in the current layer
#[derive(Debug, Clone)]
pub(crate) struct IntervalLookupBitVector {
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
    /// data column associated with this lookup object
    /// (except when this structure is empty).
    ///
    /// Note that this is a fully sorted column.
    interval_starts: ColumnEnum<usize>,

    /// [BitVec] which stores for every node of the previous layer
    /// whether it has a successor in this layer.
    ///
    /// To determine the associated interval for a node with index `i`,
    /// one can count the number of `true` entries
    /// that occur in this vector before position `i`.
    predecessors: BitVec,
}

impl IntervalLookupBitVector {}

impl IntervalLookup for IntervalLookupBitVector {
    type Builder = IntervalLookupBitVectorBuilder;

    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        if !self.predecessors[index] {
            return None;
        }

        let interval_index = self.predecessors[0..index].count_ones();

        let interval_start = self.interval_starts.get(interval_index);
        let interval_end = self.interval_starts.get(interval_index + 1);

        Some(interval_start..interval_end)
    }
}

impl ByteSized for IntervalLookupBitVector {
    fn size_bytes(&self) -> ByteSize {
        // Length + Capacity + (Number of bits) / 8
        let size_predecessor =
            ByteSize::b(2 * size_of::<usize>() as u64 + (self.predecessors.capacity() as u64) / 8);
        let size_interval_starts = self.interval_starts.size_bytes();

        size_interval_starts + size_predecessor
    }
}

#[derive(Debug, Default)]
pub(crate) struct IntervalLookupBitVectorBuilder {
    /// [ColumnBuilderAdaptive] for building `interval_starts`
    builder_intervals: ColumnBuilderAdaptive<usize>,
    /// [BitVec] for storing which predecessor value has a successor in the constructed column
    predecessors: BitVec,

    /// The end point of the last added interval
    last_end: usize,
}

impl IntervalLookupBuilder for IntervalLookupBitVectorBuilder {
    type Lookup = IntervalLookupBitVector;

    fn add(&mut self, interval: Range<usize>) {
        if interval.is_empty() {
            self.predecessors.push(false);
        } else {
            self.predecessors.push(true);
            self.builder_intervals.add(interval.start);
            self.last_end = interval.end;
        }
    }

    fn finalize(mut self) -> Self::Lookup {
        if self.builder_intervals.count() > 0 {
            self.builder_intervals.add(self.last_end)
        }

        Self::Lookup {
            interval_starts: self.builder_intervals.finalize(),
            predecessors: self.predecessors,
        }
    }
}

#[cfg(test)]
mod test {
    use bitvec::{bitvec, prelude::Lsb0};

    use crate::columnar::{
        column::Column,
        intervalcolumn::interval_lookup::{
            lookup_bitvector::IntervalLookupBitVectorBuilder, IntervalLookup, IntervalLookupBuilder,
        },
    };

    #[test]
    fn interval_lookup_column_bitvector() {
        let mut builder = IntervalLookupBitVectorBuilder::default();
        builder.add(0..0);
        builder.add(0..0);
        builder.add(0..5);
        builder.add(5..7);
        builder.add(7..7);
        builder.add(7..7);
        builder.add(7..10);
        builder.add(10..10);

        let lookup_column = builder.finalize();
        let interval_starts = lookup_column.interval_starts.iter().collect::<Vec<usize>>();
        let predecessors = lookup_column.predecessors.clone();

        assert_eq!(interval_starts, vec![0, 5, 7, 10]);
        assert_eq!(predecessors, bitvec![0, 0, 1, 1, 0, 0, 1, 0]);

        assert_eq!(lookup_column.interval_bounds(0), None);
        assert_eq!(lookup_column.interval_bounds(1), None);
        assert_eq!(lookup_column.interval_bounds(2), Some(0..5));
        assert_eq!(lookup_column.interval_bounds(3), Some(5..7));
        assert_eq!(lookup_column.interval_bounds(4), None);
        assert_eq!(lookup_column.interval_bounds(5), None);
        assert_eq!(lookup_column.interval_bounds(6), Some(7..10));
        assert_eq!(lookup_column.interval_bounds(7), None);
    }
}
