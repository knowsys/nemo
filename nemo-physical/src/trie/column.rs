//! The column, which stores the values of one level of a trie and their block boundaries.
//!
//! The values are read through a borrowed [view](ColumnView),
//! and the boundaries are kept in [intervals](Intervals).

mod intervals;
mod view;

pub(crate) use self::{intervals::Intervals, view::ColumnView};

use crate::storage::{Tag, Tag32};

/// A trie column, which stores the values of one level in a single vector, and the block
/// boundaries.
///
/// Within each block, the values are sorted by tag and then by payload.
/// A column has one of three layouts:
/// - a 32-bit column has values of one tag with a 32-bit type, whose payloads all fit in
///   32 bits;
/// - a 64-bit column has values of one tag, with 64-bit payloads;
/// - a mixed column has values of more than one tag, or no values at all.
#[derive(Debug)]
pub(super) enum Column {
    /// A column of one tag with a 32-bit type, whose payloads fit in 32 bits.
    U32 {
        /// The order-preserving 32-bit payloads.
        values: Vec<u32>,
        /// The tag shared by every value.
        tag: Tag32,
        /// The block boundaries.
        intervals: Intervals,
    },
    /// A column of one tag, with 64-bit payloads.
    U64 {
        /// The order-preserving 64-bit payloads.
        values: Vec<u64>,
        /// The tag shared by every value.
        tag: Tag,
        /// The block boundaries.
        intervals: Intervals,
    },
    /// A column with values of more than one tag, or with no values.
    Mixed {
        /// The order-preserving 64-bit payloads.
        values: Vec<u64>,
        /// The tags, parallel to `values`.
        tags: Vec<Tag>,
        /// The block boundaries.
        intervals: Intervals,
    },
}

impl Column {
    /// Creates a column without values, and without allocating for them.
    pub(super) fn empty() -> Column {
        Column::Mixed {
            values: Vec::new(),
            tags: Vec::new(),
            intervals: Intervals::empty(),
        }
    }

    /// Returns a borrowed view of the column's values.
    pub(super) fn view(&self) -> ColumnView<'_> {
        match self {
            Column::U32 { values, tag, .. } => ColumnView::U32 { values, tag: *tag },
            Column::U64 { values, tag, .. } => ColumnView::U64 { values, tag: *tag },
            Column::Mixed { values, tags, .. } => ColumnView::Mixed { values, tags },
        }
    }

    /// Returns the block boundaries.
    pub(super) fn intervals(&self) -> &Intervals {
        match self {
            Column::U32 { intervals, .. }
            | Column::U64 { intervals, .. }
            | Column::Mixed { intervals, .. } => intervals,
        }
    }

    /// Returns the number of values.
    pub(super) fn len(&self) -> usize {
        match self {
            Column::U32 { values, .. } => values.len(),
            Column::U64 { values, .. } | Column::Mixed { values, .. } => values.len(),
        }
    }

    /// Returns the allocated bytes of the column.
    pub(super) fn memory_bytes(&self) -> usize {
        let data = match self {
            Column::U32 { values, .. } => values.capacity() * size_of::<u32>(),
            Column::U64 { values, .. } => values.capacity() * size_of::<u64>(),
            Column::Mixed { values, tags, .. } => {
                values.capacity() * size_of::<u64>() + tags.capacity() * size_of::<Tag>()
            }
        };
        let intervals = self.intervals().memory_bytes();

        data + intervals
    }
}
