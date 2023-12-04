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
    management::ByteSized,
};

use super::{IntervalLookup, IntervalLookupBuilder};

/// Implementation of [IntervalLookup],
/// which internally uses a [ColumnEnum]
/// to store the indices of the successor intervals
/// and a [BitVec] to store which nodes
/// of the previous layers have successors in the current layer
#[derive(Debug, Clone)]
pub struct IntervalLookupBitVector {
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

        let interval_index = self.predecessors[0..(index - 1)].count_ones();

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
pub struct IntervalLookupBitVectorBuilder {
    /// [ColumnBuilderAdaptive] for building `interval_starts`
    builder_intervals: ColumnBuilderAdaptive<usize>,
    /// [BitVec] for storing which predecessor value has a successor in the constructed column
    predecessors: BitVec,

    /// Parameter passed to `self.add` in the last call
    last_data_count: usize,
}

impl IntervalLookupBuilder for IntervalLookupBitVectorBuilder {
    type Lookup = IntervalLookupBitVector;

    fn add(&mut self, data_count: usize) {
        if data_count != self.last_data_count {
            self.predecessors.push(true);
            self.builder_intervals.add(self.last_data_count);
        } else {
            self.predecessors.push(false);
        }
    }

    fn finalize(mut self) -> Self::Lookup {
        self.builder_intervals.add(self.last_data_count);

        Self::Lookup {
            interval_starts: self.builder_intervals.finalize(),
            predecessors: self.predecessors,
        }
    }
}
