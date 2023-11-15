//! This module collects data structures which represent a column

/// Module for defining [`ColumnVector`][vector::ColumnVector]
pub mod vector;

/// Module for defining [`ColumnRle`][rle::ColumnRle]
pub mod rle;

/// Module for defining [`ColumnWithIntervals`][interval::ColumnWithIntervals]
pub mod interval;

/// TODO: Adjust description
/// Experimental module implenenting a column which can hold values of any type
pub mod rainbow;
