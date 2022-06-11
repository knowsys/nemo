use super::{
    Column, GenericColumnScanEnum, GenericIntervalColumnEnum, RangedColumnScanCell,
    RangedColumnScanEnum, RangedColumnScanT,
};
use crate::physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize};
use std::{fmt::Debug, ops::Range};

/// Column of values that are grouped into numbered intervals.
pub trait IntervalColumn<'a, T>: Debug + Column<'a, T> {
    /// Returns the number of intervals in the interval column.
    fn int_len(&self) -> usize;

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    fn int_bounds(&self, int_idx: usize) -> Range<usize>;
}

/// Enum for columns of all supported basic types.
#[derive(Debug)]
pub enum IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case GenericIntervalColumn
    GenericIntervalColumn(GenericIntervalColumnEnum<'a, T>),
}

impl<'a, T> Column<'a, T> for IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = RangedColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        match self {
            Self::GenericIntervalColumn(col) => col.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::GenericIntervalColumn(col) => col.is_empty(),
        }
    }

    fn get(&self, index: usize) -> T {
        match self {
            Self::GenericIntervalColumn(col) => col.get(index),
        }
    }

    fn iter(&'a self) -> Self::ColScan {
        match self {
            Self::GenericIntervalColumn(col) => RangedColumnScanEnum::GenericColumnScan(
                GenericColumnScanEnum::GenericIntervalColumn(col.iter()),
            ),
        }
    }
}

impl<'a, T> IntervalColumn<'a, T> for IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn int_len(&self) -> usize {
        match self {
            Self::GenericIntervalColumn(col) => col.int_len(),
        }
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        match self {
            Self::GenericIntervalColumn(col) => col.int_bounds(int_idx),
        }
    }
}

/// Enum for Interval Column with different underlying datatypes
#[derive(Debug)]
pub enum IntervalColumnT<'a> {
    /// Case u64
    U64(IntervalColumnEnum<'a, u64>),
    /// Case Float
    Float(IntervalColumnEnum<'a, Float>),
    /// Case Double
    Double(IntervalColumnEnum<'a, Double>),
}

impl<'a> Column<'a, DataValueT> for IntervalColumnT<'a> {
    type ColScan = RangedColumnScanT<'a>;

    fn len(&self) -> usize {
        match self {
            Self::U64(col) => col.len(),
            Self::Float(col) => col.len(),
            Self::Double(col) => col.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::U64(col) => col.is_empty(),
            Self::Float(col) => col.is_empty(),
            Self::Double(col) => col.is_empty(),
        }
    }

    fn get(&self, index: usize) -> DataValueT {
        match self {
            Self::U64(col) => DataValueT::U64(col.get(index)),
            Self::Float(col) => DataValueT::Float(col.get(index)),
            Self::Double(col) => DataValueT::Double(col.get(index)),
        }
    }

    fn iter(&'a self) -> Self::ColScan {
        match self {
            Self::U64(col) => RangedColumnScanT::U64(RangedColumnScanCell::new(col.iter())),
            Self::Float(col) => RangedColumnScanT::Float(RangedColumnScanCell::new(col.iter())),
            Self::Double(col) => RangedColumnScanT::Double(RangedColumnScanCell::new(col.iter())),
        }
    }
}

impl<'a> IntervalColumn<'a, DataValueT> for IntervalColumnT<'a> {
    fn int_len(&self) -> usize {
        match self {
            Self::U64(col) => col.int_len(),
            Self::Float(col) => col.int_len(),
            Self::Double(col) => col.int_len(),
        }
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        match self {
            Self::U64(col) => col.int_bounds(int_idx),
            Self::Float(col) => col.int_bounds(int_idx),
            Self::Double(col) => col.int_bounds(int_idx),
        }
    }
}
