//! The column builder, which receives a column's values in storage order, block by block,
//! into storage of a layout decided in advance.

use crate::{
    storage::StorageValue,
    trie::{
        Intervals,
        column::Column,
        construct::builder::{ColumnKind, KindScan, RowChange},
    },
};

/// The exact size and layout of one column, from a pass over its rows.
///
/// It combines a [kind scan](KindScan) over every value
/// with the counts of values and blocks over the distinct rows.
#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnScan {
    /// The kind scan over the values.
    scan: KindScan,
    /// The number of values so far, counted over the distinct rows.
    values: usize,
    /// The number of blocks so far.
    blocks: usize,
}

impl ColumnScan {
    /// Adds one value to the kind scan, whether its row is distinct or not.
    #[inline]
    pub(crate) fn add(&mut self, value: StorageValue) {
        self.scan.add(value);
    }

    /// Counts the value and the block that a distinct row adds to this column,
    /// as `change` says.
    ///
    /// `column` is the index of this column.
    #[inline]
    pub(crate) fn count(&mut self, change: RowChange, column: usize) {
        self.values += usize::from(change.takes_value(column));
        self.blocks += usize::from(change.opens_block(column));
    }

    /// Returns a builder for the column that the scan sized.
    pub(crate) fn builder(&self) -> ColumnBuilder {
        ColumnBuilder::new(self.scan.kind(), self.values, self.blocks)
    }
}

/// A builder of one column, which receives the values in storage order, block by block,
/// into a column of a layout decided in advance.
#[derive(Debug)]
pub(crate) struct ColumnBuilder {
    /// The column so far, whose intervals lack the closing sentinel
    /// until [finish](Self::finish).
    column: Column,
}

impl ColumnBuilder {
    /// Creates a builder for a column of the given kind
    /// with room for `values` values in `blocks` blocks.
    ///
    /// Exact counts build the column without a reallocation.
    /// Counts that are only bounds leave unused capacity, which [finish](Self::finish) releases.
    /// `values` also bounds the interval offsets, which decides their width.
    pub(crate) fn new(kind: ColumnKind, values: usize, blocks: usize) -> Self {
        let intervals = Intervals::with_capacity(values, blocks);
        let column = match kind {
            ColumnKind::U32(tag) => Column::U32 {
                values: Vec::with_capacity(values),
                tag,
                intervals,
            },
            ColumnKind::U64(tag) => Column::U64 {
                values: Vec::with_capacity(values),
                tag,
                intervals,
            },
            ColumnKind::Mixed => Column::Mixed {
                values: Vec::with_capacity(values),
                tags: Vec::with_capacity(values),
                intervals,
            },
        };

        Self { column }
    }

    /// Builds a column of one block with the given values, in the kind that they need.
    #[cfg(test)]
    pub(super) fn single_block(values: &[StorageValue]) -> Column {
        let mut builder = Self::new(ColumnKind::of(values.iter().copied()), values.len(), 1);
        builder.open_block();
        for &value in values {
            builder.push(value);
        }
        builder.finish()
    }

    /// Returns the intervals so far, for changing them.
    fn intervals_mut(&mut self) -> &mut Intervals {
        match &mut self.column {
            Column::U32 { intervals, .. }
            | Column::U64 { intervals, .. }
            | Column::Mixed { intervals, .. } => intervals,
        }
    }

    /// Opens a new block, so that the values that follow belong to the next node above.
    pub(super) fn open_block(&mut self) {
        let offset = self.column.len();
        self.intervals_mut().push(offset);
    }

    /// Appends a value to the current block.
    ///
    /// The value must fit the layout that the builder was created with.
    ///
    /// # Panics
    ///
    /// Panics if the payload does not fit a 32-bit layout.
    /// Debug builds also panic for a tag other than the layout's tag.
    pub(super) fn push(&mut self, value: StorageValue) {
        match &mut self.column {
            Column::U32 { values, tag, .. } => {
                debug_assert_eq!(
                    value.tag,
                    tag.tag(),
                    "the layout was decided over every value"
                );
                values.push(
                    u32::try_from(value.payload).expect("the layout was decided over every value"),
                );
            }
            Column::U64 { values, tag, .. } => {
                debug_assert_eq!(value.tag, *tag, "the layout was decided over every value");
                values.push(value.payload);
            }
            Column::Mixed { values, tags, .. } => {
                values.push(value.payload);
                tags.push(value.tag);
            }
        }
    }

    /// Finishes the column, which closes the last block and releases the unused capacity.
    pub(super) fn finish(mut self) -> Column {
        let offset = self.column.len();
        let intervals = self.intervals_mut();
        intervals.push(offset);
        intervals.shrink_to_fit();

        match &mut self.column {
            Column::U32 { values, .. } => values.shrink_to_fit(),
            Column::U64 { values, .. } => values.shrink_to_fit(),
            Column::Mixed { values, tags, .. } => {
                values.shrink_to_fit();
                tags.shrink_to_fit();
            }
        }

        self.column
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::{Tag, Tag32};

    /// A column of one tag with a 32-bit type and payloads within 32 bits has the 32-bit
    /// layout.
    #[test]
    fn uniform_small_columns_take_32_bits() {
        let column = ColumnBuilder::single_block(&[
            StorageValue::term(1),
            StorageValue::term(u64::from(u32::MAX)),
        ]);
        assert!(matches!(
            column,
            Column::U32 {
                tag: Tag32::Term,
                ..
            }
        ));

        let floats =
            ColumnBuilder::single_block(&[StorageValue::float(1.5), StorageValue::float(-2.0)]);
        assert!(matches!(
            floats,
            Column::U32 {
                tag: Tag32::Float,
                ..
            }
        ));
    }

    /// A payload beyond 32 bits, or a tag with a 64-bit type, gives the 64-bit layout.
    #[test]
    fn payloads_and_tags_beyond_32_bits_take_64_bits() {
        let large = ColumnBuilder::single_block(&[StorageValue::term(u64::from(u32::MAX) + 1)]);
        assert!(matches!(large, Column::U64 { tag: Tag::Term, .. }));

        let integers = ColumnBuilder::single_block(&[StorageValue::integer(0)]);
        assert!(matches!(integers, Column::U64 { tag: Tag::Int, .. }));
    }

    /// Several tags, or no values, give the mixed layout.
    #[test]
    fn several_tags_mix() {
        let column =
            ColumnBuilder::single_block(&[StorageValue::term(1), StorageValue::integer(1)]);
        assert!(matches!(column, Column::Mixed { .. }));

        let empty = ColumnBuilder::new(ColumnKind::Mixed, 0, 0).finish();
        assert!(matches!(empty, Column::Mixed { .. }));
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.intervals().blocks(), 0);
    }

    /// A builder created with larger counts releases the unused capacity when it finishes.
    #[test]
    fn bounded_builds_finish_with_exact_memory() {
        let mut builder = ColumnBuilder::new(ColumnKind::U64(Tag::Int), 1000, 100);
        builder.open_block();
        for value in 0..3 {
            builder.push(StorageValue::integer(value));
        }
        let column = builder.finish();
        assert_eq!(
            column.memory_bytes(),
            3 * size_of::<u64>() + 2 * size_of::<u32>()
        );
    }
}
