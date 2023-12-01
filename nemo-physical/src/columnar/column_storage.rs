//! This module collects data structures which represent a column

/// Module for defining [`Column`][column::Column] and [`ColumnT`][column::ColumnT]
pub mod column;

/// Module for defining [`ColumnScan`][columnscan::ColumnScan]
pub mod columnscan;

/// Module for defining [`ColumnVector`][vector::ColumnVector]
pub mod column_vector;

/// Module for defining [`ColumnRle`][rle::ColumnRle]
pub mod column_rle;

/// Module for defining [`ColumnWithIntervals`][interval::ColumnWithIntervals]
pub mod interval;
