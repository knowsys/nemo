//! The column builder: values arrive in storage order, block by block,
//! into storage of a layout decided up front.

use crate::{
    storage::StorageValue,
    trie::{
        Intervals,
        column::Column,
        construct::builder::{ColumnKind, KindScan, RowChange},
    },
};

/// The exact sizing of one column from a pass over the rows it will hold:
/// the [kind scan](KindScan) over every value,
/// and the value and block counts over the distinct rows.
#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnScan {
    /// The kind scan over the values.
    scan: KindScan,
    /// The distinct values so far.
    values: usize,
    /// The blocks so far.
    blocks: usize,
}

impl ColumnScan {
    /// Accounts one value, distinct or not.
    #[inline]
    pub(crate) fn add(&mut self, value: StorageValue) {
        self.scan.add(value);
    }

    /// Accounts one distinct row by the change it makes at this column.
    #[inline]
    pub(crate) fn count(&mut self, change: RowChange, column: usize) {
        self.values += usize::from(change.takes_value(column));
        self.blocks += usize::from(change.opens_block(column));
    }

    /// Returns the builder of the column the scan sized.
    pub(crate) fn builder(&self) -> ColumnBuilder {
        ColumnBuilder::new(self.scan.kind(), self.values, self.blocks)
    }
}

/// A builder of one column: its values arrive in storage order and block by block
/// into a column of a layout decided up front.
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
    /// Exact counts build without a reallocation;
    /// a bound leaves unused capacity that [finish](Self::finish) gives back.
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

    /// Builds the single-block column of the given values, in the kind they take.
    #[cfg(test)]
    pub(super) fn single_block(values: &[StorageValue]) -> Column {
        let mut builder = Self::new(ColumnKind::of(values.iter().copied()), values.len(), 1);
        builder.open_block();
        for &value in values {
            builder.push(value);
        }
        builder.finish()
    }

    /// Returns the block start offsets so far.
    fn intervals_mut(&mut self) -> &mut Intervals {
        match &mut self.column {
            Column::U32 { intervals, .. }
            | Column::U64 { intervals, .. }
            | Column::Mixed { intervals, .. } => intervals,
        }
    }

    /// Opens a new block; the values that follow belong to the next node above.
    pub(super) fn open_block(&mut self) {
        let offset = self.column.len();
        self.intervals_mut().push(offset);
    }

    /// Appends a value to the current block.
    ///
    /// The value must fit the layout the builder was created with.
    ///
    /// # Panics
    ///
    /// Panics if the value does not fit the layout the builder was created with.
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

    /// Finishes the column, closing the last block
    /// and giving back the capacity a bound left unused.
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

    #[test]
    fn payloads_and_tags_beyond_32_bits_take_64_bits() {
        let large = ColumnBuilder::single_block(&[StorageValue::term(u64::from(u32::MAX) + 1)]);
        assert!(matches!(large, Column::U64 { tag: Tag::Term, .. }));

        let integers = ColumnBuilder::single_block(&[StorageValue::integer(0)]);
        assert!(matches!(integers, Column::U64 { tag: Tag::Int, .. }));
    }

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
