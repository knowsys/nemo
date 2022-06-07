//! This module collects data structures and operations on individual columns.

/// Module for defining [`Column`] and [`ColumnT`]
pub mod column;
pub use column::Column;
pub use column::ColumnEnum;
pub use column::ColumnT;

/// Module for defining [`ColumnBuilder`]
pub mod column_builder;
pub use column_builder::ColumnBuilder;

/// Module for defining [`VectorColumn`]
pub mod vector_column;
pub use vector_column::VectorColumn;

/// Module for defining [`AdaptiveColumnBuilder`]
pub mod adaptive_column_builder;
pub use adaptive_column_builder::AdaptiveColumnBuilder;
pub use adaptive_column_builder::AdaptiveColumnBuilderT;

/// Module for defining [`ColumnScan`]
pub mod column_scan;
pub use column_scan::ColumnScan;

/// Module for defining [`RangedColumnScan`]
pub mod ranged_column_scan;
pub use ranged_column_scan::RangedColumnScan;
pub use ranged_column_scan::RangedColumnScanCell;
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

/// Module for defining [`RleColumn`]
pub mod rle_column;
pub use rle_column::RleColumn;
pub use rle_column::RleColumnBuilder;
pub use rle_column::RleColumnScan;

/// Module for defining [`ReorderScan`]
pub mod reorder_scan;
pub use reorder_scan::ReorderScan;

/// Module for defining [`EqualColumnScan`]
pub mod equal_column_scan;
pub use equal_column_scan::EqualColumnScan;

/// Module for defining [`EqualValueScan`]
pub mod equal_value_scan;
pub use equal_value_scan::EqualValueScan;

/// Module for defining [`PassScan`]
pub mod pass_scan;
pub use pass_scan::PassScan;

/// Module for defining [`DifferenceScan`]
pub mod difference_scan;
pub use difference_scan::DifferenceScan;
pub use difference_scan::MinusScan;

/// Module for defining [`UnionScan`]
pub mod union_scan;
pub use union_scan::UnionScan;
