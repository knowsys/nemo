//! The row cursor: a streaming reader of a trie's rows.

use crate::{storage::StorageValue, trie::Trie};

/// A reader of a trie's rows in storage order, one row at a time.
///
/// It is the streaming counterpart of [Trie::for_each_row].
/// A cursor on a nullary trie is done at once, as if the trie were empty.
#[derive(Debug)]
pub(crate) struct RowCursor<'a> {
    /// The trie that is read.
    trie: &'a Trie,
    /// Per column, the position of the current row's node.
    positions: Vec<usize>,
    /// Per column, the exclusive end of the block that the current node lies in.
    ends: Vec<usize>,
    /// The current row.
    row: Vec<StorageValue>,
    /// Whether every row has been read.
    done: bool,
}

impl<'a> RowCursor<'a> {
    /// Creates a cursor at the trie's row with the given index in storage order.
    ///
    /// The cursor is done at once if the index is out of range.
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

        // The row's index is its position in the last column.
        // Each column above has the parent of the position below it,
        // and the block of each position ends the walk in its column.
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

    /// Returns the index of the current row, which is its position in the last column.
    ///
    /// It is valid while the cursor is not [done](Self::is_done).
    pub(crate) fn index(&self) -> usize {
        self.positions[self.positions.len() - 1]
    }

    /// Returns the current row.
    ///
    /// It is valid while the cursor is not [done](Self::is_done).
    pub(crate) fn row(&self) -> &[StorageValue] {
        &self.row
    }

    /// Returns whether every row has been read.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    /// Advances to the next row in storage order.
    pub(crate) fn advance(&mut self) {
        self.advance_past(self.positions.len());
    }

    /// Advances to the next row in storage order that differs from the current row
    /// in one of its first `prefix` columns.
    ///
    /// The rows that agree with the current row on these columns are passed over
    /// without being read.
    /// The cursor is done if no other row is left, as with a prefix of no columns.
    pub(crate) fn advance_past(&mut self, prefix: usize) {
        debug_assert!(!self.done, "an exhausted cursor stays exhausted");

        // The deepest column of the prefix with a further node in its block moves to that node,
        // and every column below it moves to the first node of its new block.
        for column in (0..prefix.min(self.positions.len())).rev() {
            if self.positions[column] + 1 < self.ends[column] {
                self.positions[column] += 1;
                self.descend(column);

                return;
            }
        }

        self.done = true;
    }

    /// Fills the row from `column` downward.
    ///
    /// Each deeper column moves to the first child of the node in the column above.
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

    /// Returns the rows that a cursor visits until it is done.
    fn visited(mut cursor: RowCursor<'_>) -> Vec<Vec<StorageValue>> {
        let mut rows = Vec::new();

        while !cursor.is_done() {
            rows.push(cursor.row().to_vec());
            cursor.advance();
        }

        rows
    }

    /// A cursor on an empty or a nullary trie is done at once.
    #[test]
    fn empty_tries_read_as_exhausted() {
        assert!(RowCursor::new(&Trie::empty(2), 0).is_done());
        assert!(RowCursor::new(&Trie::nullary(true), 0).is_done());
    }

    /// Returns a trie of six rows over three columns, with its rows in storage order.
    fn sample() -> (Trie, Vec<Vec<StorageValue>>) {
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

        (trie, all)
    }

    /// A cursor created at any index visits exactly the rows from that index on.
    #[test]
    fn seeking_visits_exactly_the_suffix() {
        let (trie, all) = sample();

        for start in 0..=all.len() + 1 {
            let expected: Vec<_> = all.iter().skip(start).cloned().collect();
            assert_eq!(
                visited(RowCursor::new(&trie, start)),
                expected,
                "start {start}"
            );
        }
    }

    /// From any row, advancing past a prefix reaches the next row that differs within the
    /// prefix, and a prefix of every column reaches the next row.
    #[test]
    fn advancing_past_a_prefix_passes_over_the_rows_that_share_it() {
        let (trie, all) = sample();

        for start in 0..all.len() {
            for prefix in 0..=4 {
                let shared = prefix.min(3);
                let expected = all[start + 1..]
                    .iter()
                    .find(|row| row[..shared] != all[start][..shared]);

                let mut cursor = RowCursor::new(&trie, start);
                cursor.advance_past(prefix);
                let reached = (!cursor.is_done()).then(|| cursor.row().to_vec());
                assert_eq!(reached.as_ref(), expected, "row {start}, prefix {prefix}");
            }
        }
    }
}
