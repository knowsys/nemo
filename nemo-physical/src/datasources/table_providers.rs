//! Module for defining a trait that can be implemented by code that can provide tabular data,
//! such as file readers.

use std::error::Error;

use super::TableWriter;

/// TODO: this is the new TableReader, and the interface will be similar but using ColumnWriters
pub trait TableProvider: std::fmt::Debug {
    /// Provide table data by adding values to a [`TableWriter`].
    fn provide_table_data(
        self: Box<Self>,
        table_writer: &mut TableWriter,
    ) -> Result<(), Box<dyn Error>>;
}
