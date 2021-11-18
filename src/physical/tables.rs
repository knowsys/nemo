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

/// Module for defining [`Ftrie`]
pub mod ftrie;
pub use ftrie::Ftrie;
