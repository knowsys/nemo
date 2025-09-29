//! Module that allows callers to write tuples of data values into a buffer, which
//! can later be turned into a database table.

use std::cell::RefCell;

use crate::{
    datavalues::{AnyDataValue, DataValue, NullDataValue},
    dictionary::DvDict,
    management::database::Dict,
    tabular::{
        buffer::{sorted_tuple_buffer::SortedTupleBuffer, tuple_buffer::TupleBuffer},
        filters::FilterTransformPattern,
    },
};

use delegate::delegate;

/// Allows sending of [AnyDataValue]s tuples to the database.
///
/// The [TupleWriter] is used to send the tuples of [AnyDataValue]s to the database, so that they
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
    /// Construct a new [TupleWriter]. This is public to allow
    /// downstream implementations of [TableProvider][super::table_providers::TableProvider] to
    /// test their code. In normal operation, it will be provided by the database.
    pub fn new(dictionary: &'a RefCell<Dict>, column_count: usize) -> Self {
        Self {
            dictionary,
            tuple_buffer: TupleBuffer::new(column_count),
        }
    }

    /// Create a new [TupleWriter] with the given [FilterTransformPattern]s. This is public to allow
    /// downstream implementations of [TableProvider][super::table_providers::TableProvider] to
    /// test their code. In normal operation, it will be provided by the database.
    pub fn with_patterns(
        dictionary: &'a RefCell<Dict>,
        column_count: usize,
        patterns: Vec<FilterTransformPattern>,
    ) -> Self {
        Self {
            dictionary,
            tuple_buffer: TupleBuffer::with_patterns(column_count, patterns),
        }
    }

    /// Set the given [FilterTransformPattern]s.
    pub fn set_patterns(&mut self, patterns: Vec<FilterTransformPattern>) {
        self.tuple_buffer.set_patterns(patterns);
    }

    /// Provide the next value for the current tuple. Values are added in in order.
    /// When the value for the last column was provided, the tuple is committed to the buffer.
    /// Alternatively, a partially built tuple can be abandonded by calling `drop_current_tuple`.
    pub fn add_tuple_value(&mut self, value: AnyDataValue) {
        self.tuple_buffer
            .add_tuple_data_value(&mut self.dictionary.borrow_mut(), value);
    }

    /// Create a fresh null value. This is the correct (and only) way to create nulls that
    /// should be used in the written tuples. Each null will be unequal to any other null
    /// created before, so nulls that are to be used in several places must be stored for later.
    #[must_use]
    pub fn fresh_null(&mut self) -> NullDataValue {
        self.dictionary
            .borrow_mut()
            .fresh_null()
            .0
            .to_null_unchecked()
    }

    delegate! {
        to self.tuple_buffer {
            /// Returns the number of columns on the input table, i.e., the
            /// number of values that need to be written to make one tuple.
            pub fn input_column_number(&self) -> usize;
            /// Returns the number of columns on the output table, i.e., the
            /// number of values that make one tuple.
            pub fn output_column_number(&self) -> usize;
            /// Returns the number of rows in the [TupleWriter].
            pub fn size(&self) -> usize;
            /// Forget about any previously added values that have not formed a complete tuple yet,
            /// and start a new tuple from the beginning.
            pub fn drop_current_tuple(&mut self);
            /// Function to indicate the data loading process has ended, and to sort the data.
            #[must_use]
            pub(crate) fn finalize(self) -> SortedTupleBuffer;
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datasources::tuple_writer::TupleWriter,
        datatypes::{Float, StorageValueT},
        datavalues::AnyDataValue,
        dictionary::{DvDict, meta_dv_dict::MetaDvDictionary},
        management::database::Dict,
    };

    #[test]
    fn tuple_writer() {
        let dictionary = RefCell::new(Dict::new());
        let mut writer = TupleWriter::new(&dictionary, 2);

        let null = writer.fresh_null();

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(1));
        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("a")));

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("b")));

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(2));
        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("b")));

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("a")));

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(3));
        writer.add_tuple_value(AnyDataValue::new_float_from_f32(1.2).unwrap());

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(5));
        writer.add_tuple_value(AnyDataValue::new_float_from_f32(0.6).unwrap());

        writer.add_tuple_value(AnyDataValue::new_integer_from_i64(0));
        writer.add_tuple_value(AnyDataValue::new_float_from_f32(1.8).unwrap());

        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("a")));
        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("b")));

        writer.add_tuple_value(AnyDataValue::new_plain_string(String::from("a")));
        writer.add_tuple_value(null.into());

        fn retrieve_dict_id(dictionary: &MetaDvDictionary, dv: &AnyDataValue) -> u32 {
            u32::try_from(
                dictionary
                    .datavalue_to_id(dv)
                    .expect("should have been added"),
            )
            .expect("expecting small value here")
        }

        // Note: the test below is rather fragile, since it supposes a relative order of the ids that might not
        // be guaranteed. It is certainly not part of the contract. By fetching the ids, we at least allow for
        // some variability in the implementation without failing the test.
        let id_null = retrieve_dict_id(&dictionary.borrow(), &null.into());
        let id_a = retrieve_dict_id(
            &dictionary.borrow(),
            &AnyDataValue::new_plain_string(String::from("a")),
        );
        let id_b = retrieve_dict_id(
            &dictionary.borrow(),
            &AnyDataValue::new_plain_string(String::from("b")),
        );

        let sorted_buffer = writer.finalize();

        let mut first_column = sorted_buffer.get_column(0);
        assert_eq!(first_column.next(), Some(StorageValueT::Id32(id_a)));
        assert_eq!(first_column.next(), Some(StorageValueT::Id32(id_a)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(0)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(1)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(2)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(3)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
        assert_eq!(first_column.next(), Some(StorageValueT::Int64(5)));
        assert_eq!(first_column.next(), None);

        let mut second_column = sorted_buffer.get_column(1);
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_null)));
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_b)));
        assert_eq!(
            second_column.next(),
            Some(StorageValueT::Float(Float::new(1.8).unwrap()))
        );
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_a)));
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_b)));
        assert_eq!(
            second_column.next(),
            Some(StorageValueT::Float(Float::new(1.2).unwrap()))
        );
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_a)));
        assert_eq!(second_column.next(), Some(StorageValueT::Id32(id_b)));
        assert_eq!(
            second_column.next(),
            Some(StorageValueT::Float(Float::new(0.6).unwrap()))
        );
        assert_eq!(second_column.next(), None);
    }
}
