use crate::physical::datatypes::{Double, Float};
use std::fmt::Debug;

/// Iterator for a column of ordered values.
/// The iterator's associated type Item is used to define the type of the column.
pub trait ColumnScan: Debug + Iterator {
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item>;

    /// Return the value at the current position, if any.
    fn current(&mut self) -> Option<Self::Item>;

    /// Return to the initial state
    fn reset(&mut self);
}

/// Enum for column scans for all the supported types
#[derive(Debug)]
pub enum ColumnScanT<'a> {
    /// Case Column<u64>
    ColumnScanU64(Box<dyn ColumnScan<Item = u64> + 'a>),
    /// Case Column<Float>
    ColumnScanFloat(Box<dyn ColumnScan<Item = Float> + 'a>),
    /// Case Column<Double>
    ColumnScanDouble(Box<dyn ColumnScan<Item = Double> + 'a>),
}
