//! Module for defining a trait that can be implemented by code that can provide tabular data,
//! such as file readers.

use crate::{datavalues::AnyDataValue, error::ReadingError, management::bytesized::ByteSized};

use super::tuple_writer::TupleWriter;

/// This trait is implemented by code that can provide data in the form of a list of tuples,
/// which are unordered and possibly contain duplicates.
pub trait TableProvider: std::fmt::Debug + ByteSized {
    /// Provide table data by adding values to a [TupleWriter].
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError>;

    /// Provide table data by adding values to a [TupleWriter], taking
    /// bindings for some positions into account.
    fn provide_table_data_with_bindings(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
        _bound_positions: &[usize],
        _bindings: &[Vec<AnyDataValue>],
        _num_bindings: usize,
    ) -> Result<(), ReadingError> {
        self.provide_table_data(tuple_writer)
    }

    /// Whether bindings should be computed for the import
    fn should_import_with_bindings(
        &self,
        _bound_positions: &[usize],
        _num_bindings: usize,
    ) -> bool {
        false
    }

    /// Return the number of columns of this table.
    fn arity(&self) -> usize;
}
