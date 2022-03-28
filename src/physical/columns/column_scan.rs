use std::fmt::Debug;
use crate::physical::datatypes::{Float,Double};

/// Iterator for a column of ordered values.
/// The iterator's associated type Item is used to define the type of the column.
pub trait ColumnScan: Debug + Iterator {

    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item>;

    /// Return the value at the current position, if any.
    fn current(&mut self) -> Option<Self::Item>;

}

/// Enum for column scans of all supported basic types.
#[derive(Debug)]
pub enum ColumnScanT {
    /// Case u64
    ColumnScanU64(Box<dyn ColumnScan<Item=u64>>),
    /// Case Float
    ColumnScanFloat(Box<dyn ColumnScan<Item=Float>>),
    /// Case Double
    ColumnScanDouble(Box<dyn ColumnScan<Item=Double>>),
}