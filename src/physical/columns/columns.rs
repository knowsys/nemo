//! This module collects data structures which represent a column

/// Module for defining [`Column`] and [`ColumnT`]
pub mod column;
pub use column::Column;
pub use column::ColumnEnum;
pub use column::ColumnT;

/// Module for defining [`VectorColumn`]
pub mod column_vector;
pub use column_vector::VectorColumn;

/// Module for defining [`RleColumn`]
pub mod column_rle;
pub use column_rle::ColBuilderRle;
pub use column_rle::RleColumn;
pub use column_rle::RleColumnScan;

/// Module for defining [`IntervalColumn`] and [`IntervalColumnT`]
pub mod interval_column;
pub use interval_column::IntervalColumn;
pub use interval_column::IntervalColumnEnum;
pub use interval_column::IntervalColumnT;

/// Module for defining [`GenericIntervalColumn`]
pub mod interval_column_generic;
pub use interval_column_generic::GenericIntervalColumn;
