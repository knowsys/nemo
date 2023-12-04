//! Module that allows callers to write data into columns of a database table.

use crate::{datasources::TupleBuffer, datatypes::StorageValueT};
use std::cmp::Ordering;

/// The [`SortedTableBuffer`] is a wrapper for [`TableBuffer`] that stores a tuple order.
#[derive(Debug)]
pub struct SortedTupleBuffer<'a> {
    tuple_buffer: TupleBuffer<'a>,
    tuple_order: Vec<usize>,
}

impl<'a> SortedTupleBuffer<'_> {
    /// Constructor for [`SortedTupleBuffer`]. It takes a [`TupleBuffer`] and a tuple_order as
    /// parameters. The tuple order indicates the indexes of the tuples ordered by increasing
    /// [`StorageValueT`]s.
    pub fn new<'b>(tuple_buffer: TupleBuffer<'b>) -> SortedTupleBuffer {
        let tuple_order = SortedTupleBuffer::get_order(&tuple_buffer);
        SortedTupleBuffer {
            tuple_buffer,
            tuple_order,
        }
    }

    /// Returns the number of columns in the [`SortedTableBuffer`]
    pub fn column_number(&self) -> usize {
        self.tuple_buffer.column_number()
    }

    /// Returns the total number of tuples in the [`SortedTableBuffer`]
    pub fn size(&self) -> usize {
        self.tuple_buffer.size()
    }

    /// Returns an iterator of the sorted [`StorageValueT`] values of the n-th column in the [`SortedTableBuffer`]
    pub fn get_column<'b>(&'b self, column_idx: usize) -> impl Iterator<Item = StorageValueT> + 'b {
        self.tuple_order
            .iter()
            .map(move |&tuple_idx| self.tuple_buffer.get_value(tuple_idx, column_idx))
    }

    /// Returns the order of tuples in the TupleBuffer
    fn get_order(tuple_buffer: &TupleBuffer) -> Vec<usize> {
        let mut order: Vec<usize> = (0..tuple_buffer.size()).collect();
        order.sort_by(|x, y| SortedTupleBuffer::compare_tuples(tuple_buffer, *x, *y));
        order
    }

    /// Compare two tuples by types and values corresponding to their tuple indexes, according to
    /// the internal order of tuples, i.e., tuple indexes in the first inner table are maintained,
    /// and row indexes in the second table start from table_lengths[0], and so on.
    fn compare_tuples(
        tuple_buffer: &TupleBuffer,
        first_tuple_idx: usize,
        second_tuple_idx: usize,
    ) -> Ordering {
        let (first_table_idx, first_tuple_idx) =
            tuple_buffer.get_table_and_tuple_indexes(first_tuple_idx);
        let (second_table_idx, second_tuple_idx) =
            tuple_buffer.get_table_and_tuple_indexes(second_tuple_idx);
        for i in 0..tuple_buffer.column_number() {
            let first = tuple_buffer.get_storage_value_t(first_table_idx, i, first_tuple_idx);
            let second = tuple_buffer.get_storage_value_t(second_table_idx, i, second_tuple_idx);
            if first.cmp(&second) != Ordering::Equal {
                return first.cmp(&second);
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datasources::TupleBuffer, datatypes::StorageValueT, datavalues::AnyDataValue,
        management::database::Dict,
    };

    #[test]
    fn test_sorted_column_iterator_one_col() {
        let dict = Dict::new();
        let dict_ref = RefCell::new(dict);
        let mut tb = TupleBuffer::new(&dict_ref, 1);

        let v1 = AnyDataValue::new_string("c".to_string()); // 0
        let v2 = AnyDataValue::new_string("a".to_string()); // 1
        let v3 = AnyDataValue::new_string("b".to_string()); // 2
        let v4 = AnyDataValue::new_string("d".to_string()); // 3

        tb.next_value(v1.clone());
        tb.next_value(v2.clone());
        tb.next_value(v3.clone());
        tb.next_value(v4.clone());

        let stb = tb.finalize();

        let mut iter = stb.get_column(0);
        assert_eq!(iter.next(), Some(StorageValueT::Id32(0)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(1)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(2)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(3)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_sorted_column_iterator_two_cols() {
        let dict = Dict::new();
        let dict_ref = RefCell::new(dict);
        let mut tb = TupleBuffer::new(&dict_ref, 2);

        let v1 = AnyDataValue::new_integer_from_i64(1);
        let v2 = AnyDataValue::new_integer_from_i64(2);
        let v3 = AnyDataValue::new_integer_from_i64(3);
        let v4 = AnyDataValue::new_integer_from_i64(4);
        let v10 = AnyDataValue::new_integer_from_i64(10);
        let v20 = AnyDataValue::new_integer_from_i64(20);

        let va = AnyDataValue::new_string("a".to_string()); // 0
        let vb = AnyDataValue::new_string("b".to_string()); // 1

        // 1 "a"
        tb.next_value(v1.clone());
        tb.next_value(va.clone());
        // 2 10
        tb.next_value(v2.clone());
        tb.next_value(v10.clone());
        // 3 "b"
        tb.next_value(v3.clone());
        tb.next_value(vb.clone());
        // 4 20
        tb.next_value(v4.clone());
        tb.next_value(v20.clone());

        let stb = tb.finalize();

        let mut first_iter = stb.get_column(0);
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(1)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(2)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(3)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(4)));
        assert_eq!(first_iter.next(), None);

        let mut second_iter = stb.get_column(1);
        assert_eq!(second_iter.next(), Some(StorageValueT::Id32(0)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Int64(10)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Id32(1)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Int64(20)));
        assert_eq!(second_iter.next(), None);
    }
}
