use super::ColumnScan;
use crate::physical::datatypes::{Double, Float};
use std::ops::Range;

/// Iterator for a column of ordered values that is available in materialised
/// form.
pub trait MaterialColumnScan: ColumnScan {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&self) -> Option<usize>;

    /// Restricts the iterator to the given `interval`.
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for material column scans for all the supported types
#[derive(Debug)]
pub enum MaterialColumnScanT<'a> {
    /// Case Column<u64>
    MaterialColumnScanU64(Box<dyn MaterialColumnScan<Item = u64> + 'a>),
    /// Case Column<Float>
    MaterialColumnScanFloat(Box<dyn MaterialColumnScan<Item = Float> + 'a>),
    /// Case Column<Double>
    MaterialColumnScanDouble(Box<dyn MaterialColumnScan<Item = Double> + 'a>),
}

impl<'a> MaterialColumnScanT<'a> {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    pub fn pos(&self) -> Option<usize> {
        match self {
            MaterialColumnScanT::MaterialColumnScanU64(column) => column.pos(),
            MaterialColumnScanT::MaterialColumnScanFloat(column) => column.pos(),
            MaterialColumnScanT::MaterialColumnScanDouble(column) => column.pos(),
        }
    }

    /// Restricts the iterator to the given `interval`.
    pub fn narrow(&mut self, interval: Range<usize>) {
        match self {
            MaterialColumnScanT::MaterialColumnScanU64(column) => column.narrow(interval),
            MaterialColumnScanT::MaterialColumnScanFloat(column) => column.narrow(interval),
            MaterialColumnScanT::MaterialColumnScanDouble(column) => column.narrow(interval),
        }
    }
}
