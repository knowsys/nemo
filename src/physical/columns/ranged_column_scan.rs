use super::ColumnScan;
use crate::physical::datatypes::{Double, Float};
use std::ops::Range;

/// Iterator for a sorted interval of values that also stores the current position
pub trait RangedColumnScan: ColumnScan {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&self) -> Option<usize>;

    /// Restricts the iterator to the given `interval`.
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for ranged column scans for all the supported types
#[derive(Debug)]
pub enum RangedColumnScanT<'a> {
    /// Case Column<u64>
    RangedColumnScanU64(Box<dyn RangedColumnScan<Item = u64> + 'a>),
    /// Case Column<Float>
    RangedColumnScanFloat(Box<dyn RangedColumnScan<Item = Float> + 'a>),
    /// Case Column<Double>
    RangedColumnScanDouble(Box<dyn RangedColumnScan<Item = Double> + 'a>),
}

impl<'a> RangedColumnScanT<'a> {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    pub fn pos(&self) -> Option<usize> {
        match self {
            RangedColumnScanT::RangedColumnScanU64(column) => column.pos(),
            RangedColumnScanT::RangedColumnScanFloat(column) => column.pos(),
            RangedColumnScanT::RangedColumnScanDouble(column) => column.pos(),
        }
    }

    /// Restricts the iterator to the given `interval`.
    pub fn narrow(&mut self, interval: Range<usize>) {
        match self {
            RangedColumnScanT::RangedColumnScanU64(column) => column.narrow(interval),
            RangedColumnScanT::RangedColumnScanFloat(column) => column.narrow(interval),
            RangedColumnScanT::RangedColumnScanDouble(column) => column.narrow(interval),
        }
    }
}
