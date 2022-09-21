use super::{Field, FloorToUsize};
use std::fmt::Debug;
use std::panic::RefUnwindSafe;

/// A combination of traits that is required for a data type to be used in a column
pub trait ColumnDataType:
    Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + RefUnwindSafe
{
}

impl<T> ColumnDataType for T where
    T: Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field + RefUnwindSafe
{
}
