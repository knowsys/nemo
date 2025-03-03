//! This module defines [SortedTupleBuffer].

use std::cmp::Ordering;

use crate::storagevalues::storagevalue::StorageValueT;

use super::tuple_buffer::TupleBuffer;

/// Read-only wrapper for [TupleBuffer] which allows the retrieval of its tuples in a sorted manner
#[derive(Debug)]
pub(crate) struct SortedTupleBuffer {
    /// Underlying [TupleBuffer] containing the actual values
    tuple_buffer: TupleBuffer,
    /// We imagine the tuple of the `tuple_buffer` to be arranged one after another in the order of its subtables.
    /// This vector contains the first tuple index for each subtable.
    /// This allows us to associate a tuple index with a subtable id by finding the first value in this vector
    /// which is is greater than the tuple index.
    subtable_ids: Vec<usize>,

    /// Sorting of the tuples in `tuple_buffer`
    tuple_order: Vec<usize>,
}

impl SortedTupleBuffer {
    /// Create a new [SortedTupleBuffer] by sorting the tuples in [TupleBuffer].
    pub(crate) fn new(tuple_buffer: TupleBuffer) -> Self {
        let mut subtable_ids = vec![0usize];

        for &subtable_length in tuple_buffer.subtable_lengths() {
            let last = subtable_ids
                .last()
                .expect("There is at least one entry in the vector.");
            subtable_ids.push(last + subtable_length);
        }

        let tuple_order = Self::sort_buffer(&subtable_ids, &tuple_buffer);

        Self {
            tuple_buffer,
            subtable_ids,
            tuple_order,
        }
    }

    /// Sort the tuples in the tuple buffer by creating a [`Vec<usize>`],
    /// which specifies the new order.
    fn sort_buffer(subtable_ids: &[usize], tuple_buffer: &TupleBuffer) -> Vec<usize> {
        let mut order: Vec<usize> = (0..tuple_buffer.size()).collect();
        order.sort_by(|&first_tuple_index, &second_tuple_index| {
            Self::compare_tuples(
                subtable_ids,
                tuple_buffer,
                first_tuple_index,
                second_tuple_index,
            )
        });

        order
    }

    /// For a given global tuple index, return the subtable id where the tuple of that index is contained.
    /// Also computes the the local index of that tuple within the subtable.
    ///
    /// Returns a pair where the first entry is the subtable id and the second entry is the local index.
    fn get_subtable_id(subtable_ids: &[usize], tuple_index: usize) -> (usize, usize) {
        for (subtable_id, start_index) in subtable_ids.iter().skip(1).enumerate() {
            if *start_index > tuple_index {
                return (subtable_id, (tuple_index - subtable_ids[subtable_id]));
            }
        }

        unreachable!(
            "The last value of self.subtable_ids should be higher than any possible tuple index"
        )
    }

    /// Compare two tuples by types and values corresponding to their tuple indexes, according to
    /// the internal order of tuples, i.e., tuple indexes in the first inner table are maintained,
    /// and row indexes in the second table start from `table_lengths[0]`, and so on.
    fn compare_tuples(
        subtable_ids: &[usize],
        tuple_buffer: &TupleBuffer,
        first_tuple_index: usize,
        second_tuple_index: usize,
    ) -> Ordering {
        let (first_subtable_id, first_local_index) =
            Self::get_subtable_id(subtable_ids, first_tuple_index);
        let (second_subtable_id, second_local_index) =
            Self::get_subtable_id(subtable_ids, second_tuple_index);
        let first_record = tuple_buffer.subtable_record(first_subtable_id);
        let second_record = tuple_buffer.subtable_record(second_subtable_id);

        for column_index in 0..tuple_buffer.column_number() {
            let first_type = first_record.storage_types[column_index];
            let second_type = second_record.storage_types[column_index];

            let type_comparison = first_type.cmp(&second_type);
            if type_comparison != Ordering::Equal {
                return type_comparison;
            }

            let first_column_index = first_record.storage_indices[column_index];
            let second_column_index = second_record.storage_indices[column_index];

            let value_comparison = tuple_buffer.compare_stored_values(
                first_type,
                first_column_index,
                second_column_index,
                first_local_index,
                second_local_index,
            );

            if value_comparison != Ordering::Equal {
                return value_comparison;
            }
        }

        Ordering::Equal
    }

    /// Returns the number of columns in the [SortedTupleBuffer]
    pub(crate) fn column_number(&self) -> usize {
        self.tuple_buffer.column_number()
    }

    /// For a specified column, return an iterator of over its values as [StorageValueT]s.
    /// The values returned by the iterator respect to global sorting of the tuples.
    pub(crate) fn get_column(
        &self,
        column_index: usize,
    ) -> impl Iterator<Item = StorageValueT> + '_ {
        self.tuple_order.iter().map(move |&tuple_index| {
            let (subtable_id, local_tuple_index) =
                Self::get_subtable_id(&self.subtable_ids, tuple_index);
            self.tuple_buffer
                .stored_value(subtable_id, local_tuple_index, column_index)
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
        tabular::buffer::tuple_buffer::TupleBuffer,
    };

    #[test]
    fn sorted_tuple_buffer_single_column() {
        let mut tuple_buffer = TupleBuffer::new(1);

        let value_first = StorageValueT::Id32(0);
        let value_second = StorageValueT::Id32(1);
        let value_third = StorageValueT::Id32(2);
        let value_fourth = StorageValueT::Id32(3);

        tuple_buffer.add_tuple_value(value_fourth);
        tuple_buffer.add_tuple_value(value_second);
        tuple_buffer.add_tuple_value(value_first);
        tuple_buffer.add_tuple_value(value_third);

        let sorted_buffer = tuple_buffer.finalize();

        let mut column_iterator = sorted_buffer.get_column(0);

        assert_eq!(column_iterator.next(), Some(value_first));
        assert_eq!(column_iterator.next(), Some(value_second));
        assert_eq!(column_iterator.next(), Some(value_third));
        assert_eq!(column_iterator.next(), Some(value_fourth));
        assert_eq!(column_iterator.next(), None);
    }

    #[test]
    fn sorted_tuple_buffer_dual_column() {
        let mut tuple_buffer = TupleBuffer::new(2);

        let value_first_left = StorageValueT::Id32(0);
        let value_first_right = StorageValueT::Float(Float::new(-2.0).unwrap());

        let value_second_left = StorageValueT::Id32(0);
        let value_second_right = StorageValueT::Float(Float::new(3.0).unwrap());

        let value_third_left = StorageValueT::Id32(0);
        let value_third_right = StorageValueT::Double(Double::new(-10.0).unwrap());

        let value_fourth_left = StorageValueT::Id32(2);
        let value_fourth_right = StorageValueT::Float(Float::new(-20.0).unwrap());

        let value_fifth_left = StorageValueT::Float(Float::new(-5.0).unwrap());
        let value_fifth_right = StorageValueT::Id32(1);

        tuple_buffer.add_tuple_value(value_third_left);
        tuple_buffer.add_tuple_value(value_third_right);

        tuple_buffer.add_tuple_value(value_second_left);
        tuple_buffer.add_tuple_value(value_second_right);

        tuple_buffer.add_tuple_value(value_fifth_left);
        tuple_buffer.add_tuple_value(value_fifth_right);

        tuple_buffer.add_tuple_value(value_first_left);
        tuple_buffer.add_tuple_value(value_first_right);

        tuple_buffer.add_tuple_value(value_fourth_left);
        tuple_buffer.add_tuple_value(value_fourth_right);

        let sorter_buffer = tuple_buffer.finalize();

        let mut first_column = sorter_buffer.get_column(0);
        assert_eq!(first_column.next(), Some(value_first_left));
        assert_eq!(first_column.next(), Some(value_second_left));
        assert_eq!(first_column.next(), Some(value_third_left));
        assert_eq!(first_column.next(), Some(value_fourth_left));
        assert_eq!(first_column.next(), Some(value_fifth_left));
        assert_eq!(first_column.next(), None);

        let mut second_column = sorter_buffer.get_column(1);
        assert_eq!(second_column.next(), Some(value_first_right));
        assert_eq!(second_column.next(), Some(value_second_right));
        assert_eq!(second_column.next(), Some(value_third_right));
        assert_eq!(second_column.next(), Some(value_fourth_right));
        assert_eq!(second_column.next(), Some(value_fifth_right));
        assert_eq!(second_column.next(), None);
    }
}
