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
}

impl<'a> RowRange<'a> {
    /// Creates the range of every row of the trie.
    pub(super) fn all(trie: &'a Trie) -> Self {
        Self::between(trie, 0..trie.row_count())
    }

    /// Creates the range of the rows with indices in the given range.
    pub(super) fn between(trie: &'a Trie, range: Range<usize>) -> Self {
        Self {
            cursor: RowCursor::new(trie, range.start),
            end: range.end,
        }
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
        let mut range = RowRange::between(&trie, 1..3);
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
