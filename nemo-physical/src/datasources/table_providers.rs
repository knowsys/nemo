//! Module for defining a trait that can be implemented by code that can provide tabular data,
//! such as file readers.

use std::error::Error;

use super::TupleWriter;

/// This trait is implemented by code that can provide data in the form of a list of tuples,
/// which are unordered and possibly contain duplicates.
pub trait TableProvider: std::fmt::Debug {
    /// Provide table data by adding values to a [`TupleWriter`].
    fn provide_table_data(
        self: Box<Self>,
        tuple_buffer: &mut TupleWriter,
    ) -> Result<(), Box<dyn Error>>;
}
