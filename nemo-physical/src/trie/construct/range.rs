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
    /// The position of the trie in the list of tries merged.
    source: usize,
}

impl<'a> RowRange<'a> {
    /// Creates the range of every row of the trie at the given position of a merge.
    pub(super) fn all(trie: &'a Trie, source: usize) -> Self {
        Self::between(trie, 0..trie.row_count(), source)
    }

    /// Creates the range of the rows with indices in the given range
    /// of the trie at the given position of a merge.
    pub(super) fn between(trie: &'a Trie, range: Range<usize>, source: usize) -> Self {
        Self {
            cursor: RowCursor::new(trie, range.start),
            end: range.end,
            source,
        }
    }

    /// Returns the position of the trie in the list of tries merged.
    pub(super) fn source(&self) -> usize {
        self.source
    }

    /// Returns the index of the current row in the trie;
    /// valid while the range [has a row](RowSource::has_row).
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
