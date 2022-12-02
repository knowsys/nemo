//! This module collects operations over tries

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieScanProject`]
pub mod triescan_project;
pub use triescan_project::TrieScanProject;

/// Module for defining [`TrieScanSelectEqual`] and [`TrieScanSelectValue`]
pub mod triescan_select;
pub use triescan_select::TrieScanSelectEqual;
pub use triescan_select::TrieScanSelectValue;

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
