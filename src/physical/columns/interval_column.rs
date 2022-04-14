use super::Column;
use crate::physical::datatypes::{Double, Float};
use std::fmt::Debug;
use std::ops::Range;

/// Column of values that are grouped into numbered intervals.
pub trait IntervalColumn<T>: Debug + Column<T> {
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
pub enum IntervalColumnT {
    /// Case Column<u64>
    IntervalColumnU64(Box<dyn IntervalColumn<u64>>),
    /// Case Column<Float>
    IntervalColumnFloat(Box<dyn IntervalColumn<Float>>),
    /// Case Column<Double>
    IntervalColumnDouble(Box<dyn IntervalColumn<Double>>),
}

impl IntervalColumnT {
    /// Returns the number of intervals in the interval column.
    pub fn int_len(&self) -> usize {
        match self {
            IntervalColumnT::IntervalColumnU64(col) => col.int_len(),
            IntervalColumnT::IntervalColumnFloat(col) => col.int_len(),
            IntervalColumnT::IntervalColumnDouble(col) => col.int_len(),
        }
    }

    /// Return the number of elements in the column.
    pub fn len(&self) -> usize {
        match self {
            IntervalColumnT::IntervalColumnU64(col) => col.len(),
            IntervalColumnT::IntervalColumnFloat(col) => col.len(),
            IntervalColumnT::IntervalColumnDouble(col) => col.len(),
        }
    }

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    pub fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        match self {
            IntervalColumnT::IntervalColumnU64(col) => col.int_bounds(int_idx),
            IntervalColumnT::IntervalColumnFloat(col) => col.int_bounds(int_idx),
            IntervalColumnT::IntervalColumnDouble(col) => col.int_bounds(int_idx),
        }
    }
}
