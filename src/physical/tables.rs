//! This module collects data structures and operations on relational tables.

/// Module for defining [`Table`]
pub mod table;
pub use table::Table;

/// Module for defining [`TableSchema`]
pub mod table_schema;
pub use table_schema::TableSchema;

// /// Module for defining [`FTableSchema`]
//pub mod ftable_schema;
//pub use ftable_schema::FTableSchema;

// /// Module for defining [`Ftrie`]
//pub mod ftrie;
//pub use ftrie::Ftrie;

/// Module for defining [`Trie`]
pub mod trie;
pub use trie::Trie;
pub use trie::TrieSchema;
pub use trie::TrieSchemaEntry;

/// Module for defining [`TrieScan`]
pub mod trie_scan;
pub use trie_scan::IntervalTrieScan;
pub use trie_scan::TrieScan;
pub use trie_scan::TrieScanEnum;
pub use trie_scan::TrieScanJoin;

/// Module for defining [`TrieProject`]
pub mod trie_project;
pub use trie_project::TrieProject;

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieSelect`]
pub mod trie_select;
pub use trie_select::TrieSelectEqual;
pub use trie_select::TrieSelectValue;
pub use trie_select::ValueAssignment;

/// Module for defining [`TrieDifference`]
pub mod trie_difference;
pub use trie_difference::TrieDifference;

/// Module for defining [`TrieUnion`]
pub mod trie_union;
pub use trie_union::TrieUnion;
