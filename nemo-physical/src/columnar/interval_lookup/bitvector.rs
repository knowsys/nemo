use std::ops::Range;

use bitvec::vec::BitVec;
use bytesize::ByteSize;

use crate::{
    columnar::traits::{
        column::{Column, ColumnEnum},
        interval::IntervalLookup,
    },
    management::ByteSized,
};

#[derive(Debug, Clone)]
pub struct IntervalLookupBitVector {
    /// Marks the start indices of sorted blocks of data within the data column
    ///
    /// The length of a block can be obtained
    /// by subtracting the start index of the next block
    /// from the start index of the current block
    block_starts: ColumnEnum<usize>,
    /// For each value value of the previous layer encodes
    /// whether this column contains a successor
    ///
    /// The index of the corresponding block can be found
    /// by counting the number of ones in `predecessors[0..(index - 1)]`
    predecessors: BitVec,
    /// Number of elements in the associated data column
    len_data: usize,
}

impl IntervalLookupBitVector {}

impl IntervalLookup for IntervalLookupBitVector {
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        if !self.predecessors[index] {
            return None;
        }

        let block_index = self.predecessors[0..(index - 1)].count_ones();

        let block_start = self.block_starts.get(block_index);
        let block_end = if block_index + 1 < self.block_starts.len() {
            self.block_starts.get(block_index + 1)
        } else {
            self.len_data
        };

        Some(block_start..block_end)
    }
}

impl ByteSized for IntervalLookupBitVector {
    fn size_bytes(&self) -> ByteSize {
        todo!()
    }
}
