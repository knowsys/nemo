//! The column: the values of one level of a trie and their block boundaries,
//! with the borrowed [view](ColumnView) of the values
//! and the [intervals](Intervals) that hold the boundaries.

mod intervals;
mod view;

pub(crate) use self::{intervals::Intervals, view::ColumnView};

use crate::storage::{Tag, Tag32};

/// A trie column: the values of one level in a single vector,
/// sorted by tag and then payload within each block, and the block boundaries.
///
/// A column has one of three layouts.
/// A 32-bit column holds values of one tag whose payloads fit 32 bits.
/// A 64-bit column holds values of one tag whose payloads fit 64 bits.
/// Mixed columns hold values of more than one tag, or no values at all.
#[derive(Debug)]
pub(super) enum Column {
    /// A uniformly tagged column whose payloads fit 32 bits.
    U32 {
        /// The order-preserving 32-bit payloads.
        values: Vec<u32>,
        /// The tag shared by every value.
        tag: Tag32,
        /// The block boundaries.
        intervals: Intervals,
    },
    /// A uniformly tagged column with 64-bit payloads.
    U64 {
        /// The order-preserving 64-bit payloads.
        values: Vec<u64>,
        /// The tag shared by every value.
        tag: Tag,
        /// The block boundaries.
        intervals: Intervals,
    },
    /// A column holding values of more than one tag;
    /// an empty column is this layout's degenerate case.
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
