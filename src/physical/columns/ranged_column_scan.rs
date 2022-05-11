#![allow(incomplete_features)]

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

    /// Cast to RangedColumnScan with type u64
    pub fn to_colum_scan_u64(&mut self) -> Option<&mut dyn RangedColumnScan<Item = u64>> {
        match self {
            RangedColumnScanT::RangedColumnScanU64(column) => Some(&mut **column),
            _ => None,
        }
    }

    /// Cast to RangedColumnScan with type Float
    pub fn to_colum_scan_float(&mut self) -> Option<&mut dyn RangedColumnScan<Item = Float>> {
        match self {
            RangedColumnScanT::RangedColumnScanFloat(column) => Some(&mut **column),
            _ => None,
        }
    }

    /// Cast to RangedColumnScan with type Double
    pub fn to_colum_scan_double(&mut self) -> Option<&mut dyn RangedColumnScan<Item = Double>> {
        match self {
            RangedColumnScanT::RangedColumnScanDouble(column) => Some(&mut **column),
            _ => None,
        }
    }
}
