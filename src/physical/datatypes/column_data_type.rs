use super::{Field, FloorToUsize};
use num::CheckedMul;
use std::fmt::Debug;

/// A combination of traits that is required for a data type to be used in a column
pub trait ColumnDataType:
    Debug
    + Copy
    + Ord
    + TryFrom<usize>
    + FloorToUsize
    + Field
    + CheckedMul
    + TryFrom<u32>
    + TryInto<u32>
{
}

impl<T> ColumnDataType for T where
    T: Debug
        + Copy
        + Ord
        + TryFrom<usize>
        + FloorToUsize
        + Field
        + CheckedMul
        + TryFrom<u32>
        + TryInto<u32>
{
}
