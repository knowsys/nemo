//! This module collects data structures and operations on relational tables.

/// Module for defining [`Table`]
pub mod table;
pub use table::Table;

/// Module for defining [`TableSchema`]
pub mod table_schema;
pub use table_schema::TableSchema;

/// Module for defining [`FTableSchema`]
pub mod ftable_schema;
pub use ftable_schema::FTableSchema;

/// Module for defining [`FTableIterator`]
pub mod ftable_iterator;
pub use ftable_iterator::FTableIterator;

/// Module for defining [`Ftrie`]
pub mod ftrie;
pub use ftrie::Ftrie;

/// Module for defining [`FtrieIterator`]
pub mod ftrie_iterator;
pub use ftrie_iterator::FtrieIterator;