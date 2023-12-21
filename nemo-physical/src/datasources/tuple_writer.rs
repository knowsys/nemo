//! Module that allows callers to write tuples of data values into a buffer, which
//! can later be turned into a database table.

use std::cell::RefCell;

use crate::{
    datavalues::AnyDataValue,
    management::database::Dict,
    tabular::buffer::{sorted_tuple_buffer::SortedTupleBuffer, tuple_buffer::TupleBuffer},
};

/// The [`TupleWriter`] is used to send the tuples of [`AnyDataValue`]s to the database, so that they
/// can be turned into a table. The interface allows values to be added one by one, and also provides
/// some roll-back functionality for dropping a previously started tuple in case of errors.
#[derive(Debug)]
pub struct TupleWriter<'a> {
    /// Dictionary that will be used in encoding some kinds of values for which we have no native representation
    dictionary: &'a RefCell<Dict>,
    /// [TupleBuffer] for storing the provided tuples
    tuple_buffer: TupleBuffer,
}

impl<'a> TupleWriter<'a> {
    /// Construct a new [`TupleBuffer`]. This is public to allow
    /// downstream implementations of [TableProvider][super::table_providers::TableProvider] to
    /// test their code. In normal operation, it will be provided by the database.
    pub fn new(dictionary: &'a RefCell<Dict>, column_count: usize) -> Self {
        Self {
            dictionary,
            tuple_buffer: TupleBuffer::new(column_count),
        }
    }

    /// Returns the number of columns on the table, i.e., the
    /// number of values that need to be written to make one tuple.
    pub fn column_number(&self) -> usize {
        self.tuple_buffer.column_number()
    }

    /// Returns the number of rows in the [`TableWriter`]
    pub fn size(&self) -> usize {
        self.tuple_buffer.size()
    }

    /// Provide the next value for the current tuple. Values are added in in order.
    /// When the value for the last column was provided, the tuple is committed to the buffer.
    /// Alternatively, a partially built tuple can be abandonded by calling `drop_current_tuple`.
    pub fn add_tuple_value(&mut self, value: AnyDataValue) {
        self.tuple_buffer
            .add_tuple_value(value.to_storage_value_t_mut(&mut self.dictionary.borrow_mut()));
    }

    /// Forget about any previously added values that have not formed a complete tuple yet,
    /// and start a new tuple from the beginning.
    pub fn drop_current_tuple(&mut self) {
        self.tuple_buffer.drop_current_tuple()
    }

    /// Function to indicate the data loading process has ended, and to sort the data.
    pub(crate) fn finalize(self) -> SortedTupleBuffer {
        self.tuple_buffer.finalize()
    }
}

#[cfg(test)]
mod test {
    #[cfg(test)]
    pub mod test {
        use std::cell::RefCell;

        use crate::{
            datasources::tuple_writer::TupleWriter,
            datatypes::{Float, StorageValueT},
            datavalues::AnyDataValue,
            management::database::Dict,
        };

        #[test]
        fn tuple_writer() {
            let dictionary = RefCell::new(Dict::new());
            let mut writer = TupleWriter::new(&dictionary, 2);

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(1));
            writer.add_tuple_value(AnyDataValue::new_string(String::from("a")));

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
            writer.add_tuple_value(AnyDataValue::new_string(String::from("b")));

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(2));
            writer.add_tuple_value(AnyDataValue::new_string(String::from("b")));

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
            writer.add_tuple_value(AnyDataValue::new_string(String::from("a")));

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(3));
            writer.add_tuple_value(AnyDataValue::new_float_from_f32(1.2).unwrap());

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
            writer.add_tuple_value(AnyDataValue::new_float_from_f32(0.6).unwrap());

            writer.add_tuple_value(AnyDataValue::new_integer_from_i64(0));
            writer.add_tuple_value(AnyDataValue::new_float_from_f32(1.8).unwrap());

            writer.add_tuple_value(AnyDataValue::new_string(String::from("a")));
            writer.add_tuple_value(AnyDataValue::new_string(String::from("b")));

            let sorted_buffer = writer.finalize();

            let mut first_column = sorted_buffer.get_column(0);
            assert_eq!(first_column.next(), Some(StorageValueT::Id32(0)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(0)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(1)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(2)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(3)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
            assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
            assert_eq!(first_column.next(), None);

            let mut second_column = sorted_buffer.get_column(1);
            assert_eq!(second_column.next(), Some(StorageValueT::Id32(1)));
            assert_eq!(
                second_column.next(),
                Some(StorageValueT::Float(Float::new(1.8).unwrap()))
            );
            assert_eq!(second_column.next(), Some(StorageValueT::Id32(0)));
            assert_eq!(second_column.next(), Some(StorageValueT::Id32(1)));
            assert_eq!(
                second_column.next(),
                Some(StorageValueT::Float(Float::new(1.2).unwrap()))
            );
            assert_eq!(second_column.next(), Some(StorageValueT::Id32(0)));
            assert_eq!(second_column.next(), Some(StorageValueT::Id32(1)));
            assert_eq!(
                second_column.next(),
                Some(StorageValueT::Float(Float::new(0.6).unwrap()))
            );
            assert_eq!(second_column.next(), None);
        }
    }
}
