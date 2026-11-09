//! The row range: a trie's rows as a source for a merge.

use std::ops::Range;

use crate::{
    storage::StorageValue,
    trie::{Trie, construct::merge::RowSource, cursor::RowCursor},
};

/// The rows of one trie with indices in a range, in storage order.
#[derive(Debug)]
pub(super) struct RowRange<'a> {
    /// The reader, positioned at the next row.
    cursor: RowCursor<'a>,
    /// The exclusive end index.
    end: usize,
    /// The index of the trie among the tries merged.
    source_index: usize,
}

impl<'a> RowRange<'a> {
    /// Creates the range of all rows of a trie.
    ///
    /// `source_index` is the index of the trie among the tries merged.
    pub(super) fn all(trie: &'a Trie, source_index: usize) -> Self {
        Self::between(trie, 0..trie.row_count(), source_index)
    }

    /// Creates the range of the rows of a trie with indices in the given range.
    ///
    /// `source_index` is the index of the trie among the tries merged.
    pub(super) fn between(trie: &'a Trie, range: Range<usize>, source_index: usize) -> Self {
        Self {
            cursor: RowCursor::new(trie, range.start),
            end: range.end,
            source_index,
        }
    }

    /// Returns the index of the trie among the tries merged.
    pub(super) fn source_index(&self) -> usize {
        self.source_index
    }

    /// Returns the index of the current row in the trie.
    ///
    /// It is valid while the range [has a row](RowSource::has_row).
    pub(super) fn index(&self) -> usize {
        self.cursor.index()
    }
}

impl RowSource for RowRange<'_> {
    fn row(&self) -> &[StorageValue] {
        self.cursor.row()
    }

    fn has_row(&self) -> bool {
        !self.cursor.is_done() && self.cursor.index() < self.end
    }

    fn advance(&mut self) {
        self.cursor.advance();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int_trie;

    /// A range visits only the rows with indices in it.
    #[test]
    fn ranges_stop_at_their_end() {
        let trie = int_trie(3, &[[1, 1, 2], [1, 2, 2], [2, 1, 1], [2, 1, 4]]);
        let mut range = RowRange::between(&trie, 1..3, 0);
        let mut rows = Vec::new();

        while range.has_row() {
            rows.push(range.row().to_vec());
            range.advance();
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], StorageValue::integer(2));
        assert_eq!(rows[1][2], StorageValue::integer(1));
    }
}
