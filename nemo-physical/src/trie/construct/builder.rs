//! The builders, which build a trie's columns from its distinct rows in storage order,
//! into storage that already has each column's final layout.
//!
//! Each column's [kind](ColumnKind) is decided before its values arrive.
//! A [column builder](ColumnBuilder) of that kind receives the values block by block,
//! and the [trie builder](TrieBuilder) passes the rows to the column builders one at a time.
//! The columns built for consecutive shards of one trie are
//! [concatenated](Column::concat_shards).

mod column;
mod concat;
mod kind;

pub(super) use self::{
    column::{ColumnBuilder, ColumnScan},
    kind::{ColumnKind, KindScan},
};

use crate::{storage::StorageValue, trie::column::Column};

/// How a row differs from the row built before it, given as the first column whose value
/// changed.
///
/// From the changed column on, each column receives the row's value.
/// Each column below the changed one first opens a block, since the node above it is new.
/// The first row has no predecessor and changes in every column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowChange {
    /// The first row.
    First,
    /// A row that differs from its predecessor from this column on.
    At(usize),
}

impl RowChange {
    /// Returns whether the column opens a block on this row.
    #[inline]
    pub(super) fn opens_block(self, column: usize) -> bool {
        match self {
            RowChange::First => true,
            RowChange::At(changed) => changed < column,
        }
    }

    /// Returns whether the column receives the row's value.
    #[inline]
    pub(super) fn takes_value(self, column: usize) -> bool {
        match self {
            RowChange::First => true,
            RowChange::At(changed) => changed <= column,
        }
    }
}

/// The builders of a trie's columns, which receive one distinct row at a time.
#[derive(Debug)]
pub(super) struct TrieBuilder {
    /// The builder of each column.
    columns: Vec<ColumnBuilder>,
}

impl TrieBuilder {
    /// Creates a trie builder from the builders of its columns, in column order.
    pub(super) fn new(columns: impl IntoIterator<Item = ColumnBuilder>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
        }
    }

    /// Passes a row to the column builders.
    ///
    /// The row must differ from the row before it as `change` says.
    #[inline]
    pub(super) fn push_row(&mut self, row: &[StorageValue], change: RowChange) {
        for (column, builder) in self.columns.iter_mut().enumerate() {
            if change.opens_block(column) {
                builder.open_block();
            }

            if change.takes_value(column) {
                builder.push(row[column]);
            }
        }
    }

    /// Finishes the columns.
    pub(super) fn finish(self) -> Vec<Column> {
        self.columns
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::Tag;

    /// A column opens a block for each node of the column above.
    #[test]
    fn rows_open_blocks_below_their_change() {
        // The rows are (1,1), (1,2) and (2,2), so column 1 opens a block for each node of
        // column 0.
        let rows = [[1, 1], [1, 2], [2, 2]].map(|row| row.map(StorageValue::integer));
        let mut builder = TrieBuilder::new(
            (0..2).map(|column| ColumnBuilder::new(ColumnKind::U64(Tag::Int), 3, column + 1)),
        );
        builder.push_row(&rows[0], RowChange::First);
        builder.push_row(&rows[1], RowChange::At(1));
        builder.push_row(&rows[2], RowChange::At(0));
        let columns = builder.finish();

        assert_eq!(columns[0].len(), 2);
        assert_eq!(columns[0].intervals().blocks(), 1);
        assert_eq!(columns[1].len(), 3);
        assert_eq!(columns[1].intervals().children(0), 0..2);
        assert_eq!(columns[1].intervals().children(1), 2..3);
    }
}
