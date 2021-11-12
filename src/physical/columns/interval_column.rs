use super::Column;
use crate::physical::datatypes::{Float,Double};
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
        fn int_bounds(&self, int_idx: usize) -> (usize,usize);
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