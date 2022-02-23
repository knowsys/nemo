use super::Column;
use crate::physical::datatypes::{Double, Float};
use std::fmt::Debug;

/// Column of values that are grouped into numbered intervals.
pub trait IntervalColumn<T>: Debug + Column<T> {
    /// Returns the number of intervals in the interval column.
    fn int_len(&self) -> usize;

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    fn int_bounds(&self, int_idx: usize) -> (usize, usize);
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

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    pub fn int_bounds(&self, int_idx: usize) -> (usize, usize) {
        match self {
            IntervalColumnT::IntervalColumnU64(col) => col.int_bounds(int_idx),
            IntervalColumnT::IntervalColumnFloat(col) => col.int_bounds(int_idx),
            IntervalColumnT::IntervalColumnDouble(col) => col.int_bounds(int_idx),
        }
    }
}
