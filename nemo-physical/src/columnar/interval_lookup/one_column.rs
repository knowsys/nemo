use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::traits::{
        column::{Column, ColumnEnum},
        columnscan::ColumnScan,
        interval::IntervalLookup,
    },
    management::ByteSized,
};

#[derive(Debug, Clone)]
pub struct IntervalLookupOneColumn {
    /// Marks the start indices of sorted blocks of data within the data column
    /// May contain `Self::EMPTY` value for nodes without successors
    ///
    /// The length of a block can be obtained
    /// by iterating to the next value in this column that is not `Self::EMPTY`
    block_starts: ColumnEnum<usize>,
    /// Number of elements in the associated data column
    len_data: usize,
}

impl IntervalLookupOneColumn {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;

    /// Search the `block_starts` for the next entry that is not `Self::EMPTY`.
    ///
    /// Returns `None` if no such entry exists.
    fn find_next_block_start(&self, start: usize) -> Option<usize> {
        let mut block_scan = self.block_starts.iter();
        block_scan.narrow(start..self.block_starts.len());

        while let Some(value) = block_scan.next() {
            if value == !Self::EMPTY {
                return Some(value);
            }
        }

        None
    }
}

impl IntervalLookup for IntervalLookupOneColumn {
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        let block_start = self.block_starts.get(index);

        if block_start == Self::EMPTY {
            return None;
        }

        let block_end = self
            .find_next_block_start(block_start)
            .map_or(self.len_data, |end| end);

        Some(block_start..block_end)
    }
}

impl ByteSized for IntervalLookupOneColumn {
    fn size_bytes(&self) -> ByteSize {
        self.block_starts.size_bytes()
    }
}
