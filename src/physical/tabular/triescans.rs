//! This module collects data structures for iterating over
//! a trie structure or over the result of a operation performed on trie structures

/// Module for defining [`TrieScan`]
pub mod triescan;
pub use triescan::TrieScan;
pub use triescan::TrieScanEnum;
pub use triescan::TrieScanGeneric;

/// Module for defining [`TrieScanProject`]
pub mod triescan_project;
pub use triescan_project::TrieScanProject;

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieScanSelectEqual`] and [`TrieScanSelectValue`]
pub mod triescan_select;
pub use triescan_select::TrieScanSelectEqual;
pub use triescan_select::TrieScanSelectValue;
pub use triescan_select::ValueAssignment;

/// Module for defining [`TrieScanMinus`]
pub mod triescan_minus;
pub use triescan_minus::TrieScanMinus;

/// Module for defining [`TrieScanUnion`]
pub mod triescan_union;
pub use triescan_union::TrieScanUnion;

/// Module for defining [`TrieScanJoin`]
pub mod triescan_join;
pub use triescan_join::TrieScanJoin;

/// Module for defining append functionality
pub mod triescan_append;
pub use triescan_append::trie_add_constant;
