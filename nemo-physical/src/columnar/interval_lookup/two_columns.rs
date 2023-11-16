use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::traits::{
        column::{Column, ColumnEnum},
        interval::IntervalLookup,
    },
    management::ByteSized,
};

#[derive(Debug, Clone)]
pub struct IntervalLookupTwoColumns {
    /// Marks the start indices of sorted blocks of data within the data column
    ///
    /// The length of a block can be obtained
    /// by subtracting the start index of the next block from the start index of the current block
    block_starts: ColumnEnum<usize>,
    /// Associates the global index form the previous column with a block index from this column
    predecessors: ColumnEnum<usize>,
    /// Number of elements in the associated data column
    len_data: usize,
}

impl IntervalLookupTwoColumns {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;
}

impl IntervalLookup for IntervalLookupTwoColumns {
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        let block_index = self.predecessors.get(index);

        if block_index == Self::EMPTY {
            return None;
        }

        let block_start = self.block_starts.get(block_index);
        let block_end = if block_index + 1 < self.block_starts.len() {
            self.block_starts.get(block_index + 1)
        } else {
            self.len_data
        };

        Some(block_start..block_end)
    }
}

impl ByteSized for IntervalLookupTwoColumns {
    fn size_bytes(&self) -> ByteSize {
        self.block_starts.size_bytes() + self.predecessors.size_bytes()
    }
}
