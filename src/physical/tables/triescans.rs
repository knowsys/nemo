//! This module collects data structures for iterating over
//! a trie structure or over the result of a operation performed on trie structures

/// Module for defining [`TrieScan`]
pub mod triescan;
pub use triescan::IntervalTrieScan;
pub use triescan::TrieScan;
pub use triescan::TrieScanEnum;

/// Module for defining [`TrieProject`]
pub mod triescan_project;
pub use triescan_project::TrieProject;

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieSelectEqual`] and [`TrieSelectValue`]
pub mod triescan_select;
pub use triescan_select::TrieSelectEqual;
pub use triescan_select::TrieSelectValue;
pub use triescan_select::ValueAssignment;

/// Module for defining [`TrieDifference`]
pub mod triescan_minus;
pub use triescan_minus::TrieDifference;

/// Module for defining [`TrieUnion`]
pub mod triescan_union;
pub use triescan_union::TrieUnion;

/// Module for defining [`TrieJoin`]
pub mod triescan_join;
pub use triescan_join::TrieJoin;

/// Module for defining append functionality
pub mod triescan_append;
pub use triescan_append::trie_add_constant;
