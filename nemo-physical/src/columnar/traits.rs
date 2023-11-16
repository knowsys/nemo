//! This module collects traits for columnar types

/// Module for defining [`Column`][column::Column] and [`ColumnT`][column::ColumnT]
pub mod column;

/// Module for defining [`ColumnScan`][columnscan::ColumnScan]
pub mod columnscan;

/// Module for defining [`ColumnBuilder`][columnbuilder::ColumnBuilder]
pub mod columnbuilder;

/// Module for defining [`IntervalLookup`][`interval::IntervalLookup`]
pub mod interval;
