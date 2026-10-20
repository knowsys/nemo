//! The builders: a trie's columns, built from its distinct rows in storage order
//! into storage that already has each column's final layout.
//!
//! Each column's [kind](ColumnKind) is decided before its values arrive,
//! a [column builder](ColumnBuilder) of that kind takes the values block by block,
//! and the [trie builder](TrieBuilder) feeds the column builders one row at a time.
//! The columns built for consecutive shards of one trie [concatenate](Column::concat_shards).

mod column;
mod concat;
mod kind;

pub(super) use self::{
    column::{ColumnBuilder, ColumnScan},
    kind::{ColumnKind, KindScan},
};

use crate::{storage::StorageValue, trie::column::Column};

/// How a row differs from the one built before it:
/// the first column whose value changed.
///
/// From the changed column on, each column takes the row's value,
/// and each column below it opens a block first, as the node above it is new.
/// The first row has no predecessor and changes everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowChange {
    /// The first row.
    First,
    /// A row differing from its predecessor from this column on.
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

    /// Returns whether the column takes the row's value.
    #[inline]
    pub(super) fn takes_value(self, column: usize) -> bool {
        match self {
            RowChange::First => true,
            RowChange::At(changed) => changed <= column,
        }
    }
}

/// The builders of a trie's columns, fed one distinct row at a time.
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

    /// Feeds a row, which must differ from the row before it as `change` says.
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

    #[test]
    fn rows_open_blocks_below_their_change() {
        // Rows (1,1), (1,2), (2,2): column 1 opens a block per column-0 node.
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
