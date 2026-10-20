//! The run: a borrowed sequence of rows in storage order.

use std::ops::Range;

use crate::storage::StorageValue;

/// A run: rows of one arity in storage order,
/// borrowed from a [SortedRowBuffer](super::SortedRowBuffer)
/// or a segment of a [SegmentedRowBuffer](super::SegmentedRowBuffer),
/// or cut from another run.
///
/// Rows may repeat within a run; a build keeps each once.
/// A run of nullary rows holds no values and counts its rows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Run<'a> {
    /// The rows' values, row-major.
    values: &'a [StorageValue],
    /// The number of values per row.
    arity: usize,
    /// The number of rows.
    rows: usize,
}

impl<'a> Run<'a> {
    /// Creates a run over values holding the given number of whole rows
    /// of the arity, in storage order.
    pub(super) fn new(values: &'a [StorageValue], arity: usize, rows: usize) -> Self {
        debug_assert_eq!(values.len(), arity * rows, "a run holds whole rows");
        Self {
            values,
            arity,
            rows,
        }
    }

    /// Returns the number of values per row.
    pub(crate) fn arity(&self) -> usize {
        self.arity
    }

    /// Returns the number of rows.
    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    /// Returns whether the run holds no rows.
    pub(crate) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Returns the row at the given index.
    pub(crate) fn row(&self, index: usize) -> &'a [StorageValue] {
        &self.values[index * self.arity..(index + 1) * self.arity]
    }

    /// Returns the run of the rows with indices in the given range.
    pub(crate) fn rows(&self, range: Range<usize>) -> Run<'a> {
        Self::new(
            &self.values[range.start * self.arity..range.end * self.arity],
            self.arity,
            range.len(),
        )
    }

    /// Returns the rows in order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &'a [StorageValue]> + 'a {
        let run = *self;
        (0..run.rows).map(move |index| run.row(index))
    }

    /// Returns the first row whose first-column value is at or above the given one,
    /// or the number of rows when there is none; the run must have positive arity.
    pub(in crate::trie) fn lower_bound(&self, value: StorageValue) -> usize {
        debug_assert!(self.arity > 0, "a first-column bound needs a first column");

        let (mut low, mut high) = (0, self.rows);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.row(middle)[0] < value {
                low = middle + 1;
            } else {
                high = middle;
            }
        }

        low
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::trie::RowBuffer;

    #[test]
    fn lower_bounds_find_the_first_row_at_or_above_a_value() {
        let mut buffer = RowBuffer::new(2);
        for row in [[1, 2], [3, 4], [5, 6]] {
            buffer.push_row(&row.map(StorageValue::integer));
        }
        let sorted = buffer.into_sorted();
        let run = sorted.run();

        assert_eq!(run.lower_bound(StorageValue::integer(0)), 0);
        assert_eq!(run.lower_bound(StorageValue::integer(3)), 1);
        assert_eq!(run.lower_bound(StorageValue::integer(4)), 2);
        assert_eq!(run.lower_bound(StorageValue::integer(9)), 3);
    }

    #[test]
    fn nullary_runs_count_rows_without_values() {
        let mut buffer = RowBuffer::new(0);
        buffer.push_row(&[]);
        buffer.push_row(&[]);

        let sorted = buffer.into_sorted();

        let run = sorted.run();
        assert_eq!(run.len(), 2);
        assert!(run.row(0).is_empty());
        assert_eq!(run.iter().count(), 2);
        assert!(run.row(1).is_empty());
    }
}
