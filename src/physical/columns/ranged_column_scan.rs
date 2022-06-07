#![allow(incomplete_features)]

use super::{ColumnScan, GenericColumnScanEnum, OrderedMergeJoin, RleColumnScan};
use crate::physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize};
use std::{fmt::Debug, ops::Range};

/// Iterator for a sorted interval of values that also stores the current position
pub trait RangedColumnScan: ColumnScan {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&self) -> Option<usize>;

    /// Restricts the iterator to the given `interval`.
    fn narrow(&mut self, interval: Range<usize>);

    fn narrow_unsafe(&self, interval: Range<usize>);
}

/// Enum for ranged column scans for all the supported types
#[derive(Debug)]
pub enum RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case GenericColumnScan
    GenericColumnScan(GenericColumnScanEnum<'a, T>),
    /// Case RleColumnScan
    RleColumnScan(RleColumnScan<'a, T>),
    /// Case OrderedMergeJoin
    OrderedMergeJoin(OrderedMergeJoin<'a, T, RangedColumnScanEnum<'a, T>>),
}

impl<'a, T> Iterator for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::GenericColumnScan(cs) => cs.next(),
            Self::RleColumnScan(cs) => cs.next(),
            Self::OrderedMergeJoin(cs) => cs.next(),
        }
    }
}

impl<'a, T> ColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::GenericColumnScan(cs) => cs.seek(value),
            Self::RleColumnScan(cs) => cs.seek(value),
            Self::OrderedMergeJoin(cs) => cs.seek(value),
        }
    }

    fn current(&mut self) -> Option<Self::Item> {
        match self {
            Self::GenericColumnScan(cs) => cs.current(),
            Self::RleColumnScan(cs) => cs.current(),
            Self::OrderedMergeJoin(cs) => cs.current(),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::GenericColumnScan(cs) => cs.reset(),
            Self::RleColumnScan(cs) => cs.reset(),
            Self::OrderedMergeJoin(cs) => cs.reset(),
        }
    }
}

impl<'a, T> RangedColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        match self {
            Self::GenericColumnScan(cs) => cs.pos(),
            Self::RleColumnScan(cs) => cs.pos(),
            Self::OrderedMergeJoin(cs) => cs.pos(),
        }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        match self {
            Self::GenericColumnScan(cs) => cs.narrow(interval),
            Self::RleColumnScan(cs) => cs.narrow(interval),
            Self::OrderedMergeJoin(cs) => cs.narrow(interval),
        }
    }

    fn narrow_unsafe(&self, interval: Range<usize>) {
        match self {
            Self::GenericColumnScan(cs) => cs.narrow_unsafe(interval),
            Self::RleColumnScan(cs) => cs.narrow_unsafe(interval),
            Self::OrderedMergeJoin(cs) => cs.narrow_unsafe(interval),
        }
    }
}

/// enum for RangedColumnScan for underlying data type
#[derive(Debug)]
pub enum RangedColumnScanT<'a> {
    /// Case u64
    U64(RangedColumnScanEnum<'a, u64>),
    /// Case Float
    Float(RangedColumnScanEnum<'a, Float>),
    /// Case Double
    Double(RangedColumnScanEnum<'a, Double>),
}

impl<'a> Iterator for RangedColumnScanT<'a> {
    type Item = DataValueT;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => cs.next().map(DataValueT::U64),
            Self::Float(cs) => cs.next().map(DataValueT::Float),
            Self::Double(cs) => cs.next().map(DataValueT::Double),
        }
    }
}

impl<'a> ColumnScan for RangedColumnScanT<'a> {
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => match value {
                Self::Item::U64(val) => cs.seek(val).map(DataValueT::U64),
                Self::Item::Float(_val) => None,
                Self::Item::Double(_val) => None,
            },
            Self::Float(cs) => match value {
                Self::Item::U64(_val) => None,
                Self::Item::Float(val) => cs.seek(val).map(DataValueT::Float),
                Self::Item::Double(_val) => None,
            },
            Self::Double(cs) => match value {
                Self::Item::U64(_val) => None,
                Self::Item::Float(_val) => None,
                Self::Item::Double(val) => cs.seek(val).map(DataValueT::Double),
            },
        }
    }

    fn current(&mut self) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => cs.current().map(DataValueT::U64),
            Self::Float(cs) => cs.current().map(DataValueT::Float),
            Self::Double(cs) => cs.current().map(DataValueT::Double),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::U64(cs) => cs.reset(),
            Self::Float(cs) => cs.reset(),
            Self::Double(cs) => cs.reset(),
        }
    }
}

impl<'a> RangedColumnScan for RangedColumnScanT<'a> {
    fn pos(&self) -> Option<usize> {
        match self {
            Self::U64(cs) => cs.pos(),
            Self::Float(cs) => cs.pos(),
            Self::Double(cs) => cs.pos(),
        }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        match self {
            Self::U64(cs) => cs.narrow(interval),
            Self::Float(cs) => cs.narrow(interval),
            Self::Double(cs) => cs.narrow(interval),
        }
    }

    fn narrow_unsafe(&self, interval: Range<usize>) {
        match self {
            Self::U64(cs) => cs.narrow_unsafe(interval),
            Self::Float(cs) => cs.narrow_unsafe(interval),
            Self::Double(cs) => cs.narrow_unsafe(interval),
        }
    }
}
