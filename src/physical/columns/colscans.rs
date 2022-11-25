//! This module collects objects that represent an iterator over a column

/// Module for defining [`ColumnScan`]
pub mod colscan;
pub use colscan::ColScan;
pub use colscan::ColScanCell;
pub use colscan::ColScanEnum;
pub use colscan::ColScanT;

/// Module for defining [`GenericColumnScan`]
pub mod colscan_generic;
pub use colscan_generic::GenericColumnScan;
pub use colscan_generic::GenericColumnScanEnum;

/// Module for defining [`OrderedMergeJoin`]
pub mod colscan_join;
pub use colscan_join::OrderedMergeJoin;

/// Module for defining [`ReorderScan`]
pub mod colscan_reorder;
pub use colscan_reorder::ReorderScan;

/// Module for defining [`EqualColumnScan`]
pub mod colscan_equal_column;
pub use colscan_equal_column::EqualColumnScan;

/// Module for defining [`EqualValueScan`]
pub mod colscan_equal_value;
pub use colscan_equal_value::EqualValueScan;

/// Module for defining [`PassScan`]
pub mod colscan_pass;
pub use colscan_pass::PassScan;

/// Module for defining [`DifferenceScan`]
pub mod colscan_minus;
pub use colscan_minus::DifferenceScan;
pub use colscan_minus::MinusScan;

/// Module for defining [`UnionScan`]
pub mod colscan_union;
pub use colscan_union::UnionScan;
