//! This module collects functionality for adding data to the database.

/// Module that allows callers to write data into columns of a database table.
pub mod table_writer;
pub use table_writer::TupleBuffer;
/// Module that allows callers to read sorted data column wise.
pub mod sorted_tuple_buffer;
pub use sorted_tuple_buffer::SortedTupleBuffer;
/// Module for defining a trait that can be implemented by code that can provide tabular data, such as file readers.
pub mod table_providers;
pub use table_providers::TableProvider;
