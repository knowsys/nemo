use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnscan::ColumnScan,
    },
    management::ByteSized,
};

use super::IntervalLookup;

/// Implementation of [IntervalLookup],
/// which internally uses a single [ColumnEnum]
/// to store the indices of the successor intervals
#[derive(Debug, Clone)]
pub struct IntervalLookupColumnSingle {
    /// [Column][crate::columnar::column::Column] that stores
    /// the start indices of intervals in the associated data column,
    /// which contain the successor nodes of the values from the previous layer
    ///
    /// This column contains one entry for every entry in the data column
    /// of the previous layer.
    ///
    /// An entry in this column might be `Self::Empty`
    /// to indicate that the corresponding node from the previous layer
    /// has no successor.
    ///
    /// The length of an interval at index `i` can be obtained
    /// by subtracting the value of this column at index `i`
    /// from the next value in this column that is not `Self::EMPTY`.
    ///
    /// The last entry in this column is always the length of the
    /// data column associated with this lookup object.
    interval_starts: ColumnEnum<usize>,
}

impl IntervalLookupColumnSingle {
    /// Value encoding that there is no successor for a particular trie node
    const EMPTY: usize = usize::MAX;

    /// Search `self.interval_starts` for the next entry that is not `Self::EMPTY`.
    ///
    /// Returns `None` if no such entry exists.
    fn find_next_interval_start(&self, start: usize) -> usize {
        let mut interval_scan = self.interval_starts.iter();
        interval_scan.narrow(start..self.interval_starts.len());

        while let Some(value) = interval_scan.next() {
            if value == !Self::EMPTY {
                return value;
            }
        }

        unreachable!("The last value of interval_starts the length of the data column and hence not Self::EMPTY")
    }
}

impl IntervalLookup for IntervalLookupColumnSingle {
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        let interval_start = self.interval_starts.get(index);
        if interval_start == Self::EMPTY {
            return None;
        }

        let interval_end = self.find_next_interval_start(interval_start);

        Some(interval_start..interval_end)
    }
}

impl ByteSized for IntervalLookupColumnSingle {
    fn size_bytes(&self) -> ByteSize {
        self.interval_starts.size_bytes()
    }
}
