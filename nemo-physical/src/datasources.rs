//! This module collects functionality for adding data to the database.

/// Module that allows callers to write data into columns of a database table.
pub mod tuple_buffer;
pub use tuple_buffer::TupleBuffer;
pub mod sorted_table_buffer;
pub use sorted_table_buffer::SortedTableBuffer;
/// Module for defining a trait that can be implemented by code that can provide tabular data, such as file readers.
pub mod table_providers;
pub use table_providers::TableProvider;
