//! The row cursor: a streaming reader of a trie's rows.

use crate::{storage::StorageValue, trie::Trie};

/// A reader of a trie's rows in storage order, one row at a time:
/// the streaming counterpart of [Trie::for_each_row].
///
/// A nullary trie reads as empty.
#[derive(Debug)]
pub(crate) struct RowCursor<'a> {
    /// The trie read.
    trie: &'a Trie,
    /// Per column, the position of the current row's node.
    positions: Vec<usize>,
    /// Per column, the exclusive end of the block the current node lies in.
    ends: Vec<usize>,
    /// The current row.
    row: Vec<StorageValue>,
    /// Whether every row has been read.
    done: bool,
}

impl<'a> RowCursor<'a> {
    /// Creates a cursor on the trie's row of the given index in storage order,
    /// exhausted when the index is out of range.
    pub(crate) fn new(trie: &'a Trie, index: usize) -> Self {
        let arity = trie.arity();
        let mut cursor = Self {
            trie,
            positions: vec![0; arity],
            ends: vec![0; arity],
            row: vec![StorageValue::term(0); arity],
            done: index >= trie.row_count() || arity == 0,
        };
        if cursor.done {
            return cursor;
        }

        // Rows are the last column's positions; each column above holds
        // the position's parent, whose block bounds the walk below it.
        cursor.positions[arity - 1] = index;
        for column in (1..arity).rev() {
            let parent = trie.intervals(column).parent(cursor.positions[column]);

            cursor.ends[column] = trie.intervals(column).children(parent).end;
            cursor.positions[column - 1] = parent;
        }

        cursor.ends[0] = trie.column(0).len();

        for column in 0..arity {
            cursor.row[column] = trie.column(column).get(cursor.positions[column]);
        }

        cursor
    }

    /// Returns the index of the current row, its position in the last column;
    /// valid while the cursor is not [done](Self::is_done).
    pub(crate) fn index(&self) -> usize {
        self.positions[self.positions.len() - 1]
    }

    /// Returns the current row; valid while the cursor is not [done](Self::is_done).
    pub(crate) fn row(&self) -> &[StorageValue] {
        &self.row
    }

    /// Returns whether every row has been read.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    /// Advances to the next row in storage order.
    pub(crate) fn advance(&mut self) {
        debug_assert!(!self.done, "an exhausted cursor stays exhausted");

        // The deepest column with a further node in its block steps;
        // every column beneath it enters its first child block.
        for column in (0..self.positions.len()).rev() {
            if self.positions[column] + 1 < self.ends[column] {
                self.positions[column] += 1;
                self.descend(column);

                return;
            }
        }

        self.done = true;
    }

    /// Fills the row from `column` downward,
    /// each deeper column entering the first block beneath the node above.
    fn descend(&mut self, column: usize) {
        self.row[column] = self.trie.column(column).get(self.positions[column]);
        for deeper in column + 1..self.positions.len() {
            let children = self
                .trie
                .intervals(deeper)
                .children(self.positions[deeper - 1]);

            self.positions[deeper] = children.start;
            self.ends[deeper] = children.end;
            self.row[deeper] = self.trie.column(deeper).get(children.start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int_trie;

    /// The rows a cursor visits until it is done.
    fn visited(mut cursor: RowCursor<'_>) -> Vec<Vec<StorageValue>> {
        let mut rows = Vec::new();

        while !cursor.is_done() {
            rows.push(cursor.row().to_vec());
            cursor.advance();
        }

        rows
    }

    #[test]
    fn empty_tries_read_as_exhausted() {
        assert!(RowCursor::new(&Trie::empty(2), 0).is_done());
        assert!(RowCursor::new(&Trie::nullary(true), 0).is_done());
    }

    #[test]
    fn seeking_visits_exactly_the_suffix() {
        let trie = int_trie(
            3,
            &[
                [1, 1, 2],
                [1, 2, 2],
                [1, 2, 3],
                [2, 1, 1],
                [2, 1, 4],
                [3, 7, 7],
            ],
        );
        let mut all = Vec::new();
        trie.for_each_row(|row| all.push(row.to_vec()));
        assert_eq!(all.len(), 6);

        for start in 0..=all.len() + 1 {
            let expected: Vec<_> = all.iter().skip(start).cloned().collect();
            assert_eq!(
                visited(RowCursor::new(&trie, start)),
                expected,
                "start {start}"
            );
        }
    }
}
