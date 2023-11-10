//! This module collects functionality for adding data to the database.

/// Module that allows callers to write data into columns of a database table.
pub mod table_writer;
pub use table_writer::TableWriter;
/// Module for defining a trait that can be implemented by code that can provide tabular data, such as file readers.
pub mod table_providers;
pub use table_providers::TableProvider;