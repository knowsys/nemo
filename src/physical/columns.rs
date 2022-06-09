//! This module collects data structures and operations on individual columns.

/// Module for defining [`Column`] and [`ColumnT`]
pub mod column;
pub use column::Column;
pub use column::ColumnEnum;

/// Module for defining [`ColumnBuilder`]
pub mod column_builder;
pub use column_builder::ColumnBuilder;

/// Module for defining [`VectorColumn`]
pub mod vector_column;
pub use vector_column::VectorColumn;

/// Module for defining [`AdaptiveColumnBuilder`]
pub mod adaptive_column_builder;
pub use adaptive_column_builder::AdaptiveColumnBuilder;

/// Module for defining [`ColumnScan`]
pub mod column_scan;
pub use column_scan::ColumnScan;

/// Module for defining [`RangedColumnScan`]
pub mod ranged_column_scan;
pub use ranged_column_scan::RangedColumnScan;
pub use ranged_column_scan::RangedColumnScanEnum;
pub use ranged_column_scan::RangedColumnScanT;

/// Module for defining [`GenericColumnScan`]
pub mod generic_column_scan;
pub use generic_column_scan::GenericColumnScan;
pub use generic_column_scan::GenericColumnScanEnum;

/// Module for defining [`OrderedMergeJoin`]
pub mod ordered_merge_join;
pub use ordered_merge_join::OrderedMergeJoin;

/// Module for defining [`IntervalColumn`] and [`IntervalColumnT`]
pub mod interval_column;
pub use interval_column::IntervalColumn;
pub use interval_column::IntervalColumnEnum;
pub use interval_column::IntervalColumnT;

/// Module for defining [`GenericIntervalColumn`]
pub mod generic_interval_column;
pub use generic_interval_column::GenericIntervalColumn;
pub use generic_interval_column::GenericIntervalColumnEnum;

/// Module for defining [`RleColumn`]
pub mod rle_column;
pub use rle_column::RleColumn;
pub use rle_column::RleColumnBuilder;
pub use rle_column::RleColumnScan;
