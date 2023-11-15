//! This module collects operations over tries

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieScanAggregate`]
pub mod triescan_aggregate;
pub use triescan_aggregate::TrieScanAggregate;

/// Module for defining [`TrieScanProject`]
pub mod triescan_project;
pub use triescan_project::TrieScanProject;

/// Module for defining [`TrieScanPrune`]
pub mod triescan_prune;
pub use triescan_prune::TrieScanPrune;

/// Module for defining [`TrieScanSelectEqual`] and [`TrieScanRestrictValues`]
pub mod triescan_select;
pub use triescan_select::TrieScanRestrictValues;
pub use triescan_select::TrieScanSelectEqual;

/// Module for defining [`TrieScanMinus`]
pub mod triescan_minus;
pub use triescan_minus::TrieScanMinus;

/// Module for defining [`TrieScanUnion`]
pub mod triescan_union;
pub use triescan_union::TrieScanUnion;

/// Module for defining [`TrieScanJoin`]
pub mod triescan_join;
pub use triescan_join::JoinBindings;
pub use triescan_join::TrieScanJoin;

/// Module for defining append functionality
pub mod triescan_append;

/// Module for defining [`TrieScanNulls`]
pub mod triescan_nulls;
pub use triescan_nulls::TrieScanNulls;

/// Module implementing functionality for projecting and reordering tries.
pub mod project_reorder;

/// TODO: Description
/// Experimntal module implementing the union operation for rainbow tries
pub mod rainbow_union;

/// TODO: Description
/// Experimntal module implementing the join operation for rainbow tries
pub mod rainbow_join;
