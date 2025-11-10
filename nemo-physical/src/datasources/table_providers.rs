//! Module for defining a trait that can be implemented by code that can provide tabular data,
//! such as file readers.

use crate::{error::ReadingError, management::bytesized::ByteSized};

use super::{bindings::ProductBindings, tuple_writer::TupleWriter};

/// This trait is implemented by code that can provide data in the form of a list of tuples,
/// which are unordered and possibly contain duplicates.
#[async_trait::async_trait(?Send)]
pub trait TableProvider: std::fmt::Debug + ByteSized {
    /// Provide table data by adding values to a [TupleWriter].
    async fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError>;

    /// Provide table data by adding values to a [TupleWriter], taking
    /// bindings for some positions into account.
    async fn provide_table_data_with_bindings(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
        _bindings: &ProductBindings,
    ) -> Result<(), ReadingError> {
        self.provide_table_data(tuple_writer).await
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
