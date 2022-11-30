//! This module collects data structures which represent a column

/// Module for defining [`Column`] and [`ColumnT`]
pub mod column;
pub use column::Column;
pub use column::ColumnEnum;
pub use column::ColumnT;

/// Module for defining [`ColumnVector`]
pub mod column_vector;
pub use column_vector::ColumnVector;

/// Module for defining [`ColumnRle`]
pub mod column_rle;
pub use column_rle::ColumnBuilderRle;
pub use column_rle::ColumnRle;
pub use column_rle::ColumnRleScan;

/// Module for defining [`IntervalColumn`] and [`IntervalColumnT`]
pub mod interval_column;
pub use interval_column::IntervalColumn;
pub use interval_column::IntervalColumnEnum;
pub use interval_column::IntervalColumnT;

/// Module for defining [`IntervalColumnGeneric`]
pub mod interval_column_generic;
pub use interval_column_generic::IntervalColumnGeneric;
