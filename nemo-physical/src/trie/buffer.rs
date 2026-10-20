//! The row buffer: temporary storage for rows before they become a trie,
//! with its [sorted](SortedRowBuffer) and [segmented](SegmentedRowBuffer) forms
//! and the [runs](Run) a trie is built from.

mod run;
mod segmented;
mod sorted;

pub(crate) use self::{run::Run, segmented::SegmentedRowBuffer, sorted::SortedRowBuffer};

use crate::storage::StorageValue;

/// A flat buffer of rows of one arity, stored row-major as storage values.
///
/// Rows are appended in any order and may repeat.
#[derive(Debug, Default)]
pub struct RowBuffer {
    /// The number of values per row.
    arity: usize,
    /// The number of rows.
    rows: usize,
    /// The rows' values, row-major.
    values: Vec<StorageValue>,
}

impl RowBuffer {
    /// Creates an empty buffer for rows of the given arity.
    pub fn new(arity: usize) -> Self {
        Self {
            arity,
            rows: 0,
            values: Vec::new(),
        }
    }

    /// Returns the number of values per row.
    pub fn arity(&self) -> usize {
        self.arity
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Returns whether the buffer holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Appends a row.
    ///
    /// # Panics
    ///
    /// Panics if the row does not match the buffer's arity.
    pub fn push_row(&mut self, row: &[StorageValue]) {
        assert_eq!(row.len(), self.arity, "a row must match the buffer arity");

        self.values.extend_from_slice(row);
        self.rows += 1;
    }

    /// Appends every row of another buffer.
    ///
    /// # Panics
    ///
    /// Panics if the other buffer has another arity.
    pub fn append(&mut self, other: &RowBuffer) {
        assert_eq!(
            other.arity, self.arity,
            "appended rows must match the buffer arity"
        );

        self.values.extend_from_slice(&other.values);
        self.rows += other.rows;
    }

    /// Returns every row's values, row-major.
    pub fn values(&self) -> &[StorageValue] {
        &self.values
    }

    /// Returns the row at the given index.
    pub fn row(&self, index: usize) -> &[StorageValue] {
        &self.values[index * self.arity..(index + 1) * self.arity]
    }

    /// Returns whether the rows are in storage order, by a scan.
    pub(crate) fn is_sorted(&self) -> bool {
        self.arity == 0 || self.values.chunks_exact(self.arity).is_sorted()
    }

    /// Sorts the rows into storage order.
    pub(crate) fn into_sorted(mut self) -> SortedRowBuffer {
        Self::sort_rows(&mut self.values, self.arity);

        SortedRowBuffer::new(self)
    }

    /// Returns the rows as sorted without sorting them:
    /// the caller produced them in storage order, which debug builds check.
    pub(crate) fn assume_sorted(self) -> SortedRowBuffer {
        debug_assert!(self.is_sorted(), "the rows were produced in storage order");

        SortedRowBuffer::new(self)
    }

    /// Returns a buffer holding this buffer's columns in a new order,
    /// sorted in that order: output column `target` is input column `order[target]`.
    ///
    /// `order` must be a permutation of the column indexes.
    pub(crate) fn reordered_sorted(&self, order: &[usize]) -> SortedRowBuffer {
        assert_eq!(order.len(), self.arity, "the order must cover every column");

        let mut values = Vec::with_capacity(self.values.len());
        for index in 0..self.rows {
            let row = self.row(index);
            values.extend(order.iter().map(|&source| row[source]));
        }
        Self::sort_rows(&mut values, self.arity);

        SortedRowBuffer::new(Self {
            arity: self.arity,
            rows: self.rows,
            values,
        })
    }

    /// Returns the allocated bytes of the buffer.
    pub fn memory_bytes(&self) -> usize {
        self.values.capacity() * size_of::<StorageValue>()
    }

    /// Returns every row's values, row-major, for changing in place.
    fn values_mut(&mut self) -> &mut [StorageValue] {
        &mut self.values
    }

    /// Keeps the first `rows` rows.
    fn truncate(&mut self, rows: usize) {
        self.rows = self.rows.min(rows);
        self.values.truncate(self.rows * self.arity);
    }

    /// Sorts row-major values of the given arity into storage order, in place.
    ///
    /// Rows of up to eight values sort directly, as arrays.
    /// Longer rows sort through a permutation of their indexes,
    /// which moves each row once instead of at every swap.
    fn sort_rows(values: &mut [StorageValue], arity: usize) {
        match arity {
            0 => {}
            1 => sort_arrays::<1>(values),
            2 => sort_arrays::<2>(values),
            3 => sort_arrays::<3>(values),
            4 => sort_arrays::<4>(values),
            5 => sort_arrays::<5>(values),
            6 => sort_arrays::<6>(values),
            7 => sort_arrays::<7>(values),
            8 => sort_arrays::<8>(values),
            _ => sort_by_permutation(values, arity),
        }
    }
}

/// Sorts row-major values whose rows hold `N` values, as one array per row.
fn sort_arrays<const N: usize>(values: &mut [StorageValue]) {
    let (rows, rest) = values.as_chunks_mut::<N>();
    debug_assert!(rest.is_empty(), "the values are whole rows");

    rows.sort_unstable();
}

/// Sorts row-major values of the given arity through a permutation of their indexes,
/// applied in one pass over scratch.
fn sort_by_permutation(values: &mut [StorageValue], arity: usize) {
    let rows = values.len() / arity;
    if rows <= 1 {
        return;
    }

    let row = |index: usize| &values[index * arity..(index + 1) * arity];
    let mut permutation: Vec<usize> = (0..rows).collect();
    permutation.sort_unstable_by(|&left, &right| row(left).cmp(row(right)));

    let mut scratch = Vec::with_capacity(values.len());
    for index in permutation {
        scratch.extend_from_slice(row(index));
    }

    values.copy_from_slice(&scratch);
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::testing::{assert_panics, int, int_buffer};

    /// A buffer holding the columns in a new order, rows as they are:
    /// output column `target` is input column `order[target]`,
    /// what the sorted reordering is checked against.
    fn reorder(buffer: &RowBuffer, order: &[usize]) -> RowBuffer {
        let mut reordered = RowBuffer::new(buffer.arity);
        for index in 0..buffer.rows {
            let row = buffer.row(index);
            let row: Vec<StorageValue> = order.iter().map(|&source| row[source]).collect();
            reordered.push_row(&row);
        }
        reordered
    }

    #[test]
    fn rows_must_have_the_buffer_arity() {
        let mut buffer = int_buffer(2, &[&[1, 2]]);
        assert_panics("buffer arity", || {
            buffer.push_row(&[StorageValue::integer(3)])
        });
        let other = int_buffer(3, &[&[1, 2, 3]]);
        assert_panics("buffer arity", || buffer.append(&other));
    }

    #[test]
    fn sorting_orders_rows_by_tag_then_payload() {
        let mut buffer = RowBuffer::new(1);
        buffer.push_row(&[StorageValue::integer(-3)]);
        buffer.push_row(&[StorageValue::term(7)]);
        buffer.push_row(&[StorageValue::integer(5)]);
        buffer.push_row(&[StorageValue::term(2)]);

        assert!(!buffer.is_sorted());
        let sorted = buffer.into_sorted();
        assert_eq!(
            sorted.rows().values(),
            &[
                StorageValue::term(2),
                StorageValue::term(7),
                StorageValue::integer(-3),
                StorageValue::integer(5),
            ]
        );
    }

    #[test]
    fn sorting_breaks_ties_by_later_columns() {
        let sorted = int_buffer(2, &[&[1, 9], &[1, 2], &[0, 7]]).into_sorted();
        assert_eq!(
            sorted.rows().values(),
            int_buffer(2, &[&[0, 7], &[1, 2], &[1, 9]]).values()
        );
    }

    #[test]
    fn rows_of_every_arity_sort_like_vectors() {
        // Arities on both sides of the largest one sorted as arrays.
        let mut rng = StdRng::seed_from_u64(5);
        for arity in 1..=10 {
            let rows: Vec<Vec<StorageValue>> = (0..rng.gen_range(0..60))
                .map(|_| {
                    (0..arity)
                        .map(|_| match rng.gen_range(0..2) {
                            0 => StorageValue::term(rng.gen_range(0..3)),
                            _ => int(rng.gen_range(0..3)),
                        })
                        .collect()
                })
                .collect();
            let mut values = rows.concat();
            RowBuffer::sort_rows(&mut values, arity);

            let mut expected = rows;
            expected.sort();
            assert_eq!(values, expected.concat(), "arity {arity}");
        }
    }

    #[test]
    fn sortedness_is_found_by_a_scan() {
        let mut buffer = int_buffer(2, &[&[1, 2], &[2, 1]]);
        assert!(buffer.is_sorted());

        buffer.push_row(&[StorageValue::integer(0), StorageValue::integer(0)]);
        assert!(!buffer.is_sorted());

        let sorted = buffer.into_sorted();
        assert!(sorted.rows().is_sorted());
    }

    #[test]
    fn reordered_sorted_matches_reorder_then_sort() {
        let buffer = int_buffer(
            3,
            &[&[3, 1, 2], &[1, 2, 3], &[1, 2, 1], &[2, 0, 0], &[1, 2, 3]],
        );

        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
            let expected = reorder(&buffer, &order).into_sorted();
            let actual = buffer.reordered_sorted(&order);
            assert_eq!(
                actual.rows().values(),
                expected.rows().values(),
                "order {order:?}"
            );
            assert!(actual.rows().is_sorted());
        }
    }

    #[test]
    fn nullary_buffers_count_rows() {
        let mut buffer = RowBuffer::new(0);
        assert!(buffer.is_empty());

        buffer.push_row(&[]);
        buffer.push_row(&[]);

        assert_eq!(buffer.len(), 2);
        assert!(buffer.is_sorted());
        assert_eq!(buffer.into_sorted().len(), 2);
    }
}
