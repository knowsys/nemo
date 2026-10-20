//! The sorted row buffer: rows in storage order, which a trie is built from.

use crate::trie::{RowBuffer, Run};

/// A [RowBuffer] whose rows are in storage order.
///
/// Rows may repeat; a trie built from the rows, as one [Run], keeps each once.
#[derive(Debug)]
pub(crate) struct SortedRowBuffer {
    /// The rows, in storage order.
    buffer: RowBuffer,
}

impl SortedRowBuffer {
    /// Wraps a buffer whose rows stand in storage order.
    pub(super) fn new(buffer: RowBuffer) -> Self {
        Self { buffer }
    }

    /// Returns the number of values per row.
    pub(crate) fn arity(&self) -> usize {
        self.buffer.arity()
    }

    /// Returns the number of rows.
    pub(crate) fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns whether the buffer holds no rows.
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the rows as a plain buffer.
    pub(crate) fn rows(&self) -> &RowBuffer {
        &self.buffer
    }

    /// Returns the rows as a plain buffer, forgetting their order.
    pub(crate) fn into_rows(self) -> RowBuffer {
        self.buffer
    }

    /// Returns the rows as one run.
    pub(crate) fn run(&self) -> Run<'_> {
        Run::new(self.buffer.values(), self.buffer.arity(), self.buffer.len())
    }

    /// Removes the repeated rows, which stand adjacent.
    pub(super) fn dedup(&mut self) {
        let arity = self.buffer.arity();
        let rows = self.buffer.len();
        if arity == 0 {
            self.buffer.truncate(1);
            return;
        }

        let values = self.buffer.values_mut();
        let mut kept = 0;
        for row in 0..rows {
            if row > 0
                && values[row * arity..(row + 1) * arity]
                    == values[(kept - 1) * arity..kept * arity]
            {
                continue;
            }

            values.copy_within(row * arity..(row + 1) * arity, kept * arity);

            kept += 1;
        }

        self.buffer.truncate(kept);
    }

    /// Returns the allocated bytes of the buffer.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.buffer.memory_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int_buffer;

    #[test]
    fn deduplication_keeps_one_row_per_run() {
        let mut buffer =
            int_buffer(2, &[&[1, 2], &[1, 2], &[1, 3], &[2, 2], &[2, 2]]).into_sorted();
        buffer.dedup();
        assert_eq!(
            buffer.rows().values(),
            int_buffer(2, &[&[1, 2], &[1, 3], &[2, 2]]).values()
        );

        let mut empty = RowBuffer::new(2).into_sorted();
        empty.dedup();
        assert!(empty.is_empty());

        // The empty rows are all equal; existence collapses to one.
        let mut nullary = int_buffer(0, &[&[], &[], &[]]).into_sorted();
        nullary.dedup();
        assert_eq!(nullary.len(), 1);
    }
}
