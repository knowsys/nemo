use super::{
    casting::{ImplicitCastFrom, ImplicitCastInto},
    Field, FloorToUsize, RunLengthEncodable,
};
use num::{Bounded, CheckedMul};
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
    + ImplicitCastFrom<u32>
    + ImplicitCastInto<u32>
    + ImplicitCastFrom<u64>
    + ImplicitCastInto<u64>
    + ImplicitCastFrom<i64>
    + ImplicitCastInto<i64>
    + Bounded
    + RunLengthEncodable
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
        + ImplicitCastFrom<u32>
        + ImplicitCastInto<u32>
        + ImplicitCastFrom<u64>
        + ImplicitCastInto<u64>
        + ImplicitCastFrom<i64>
        + ImplicitCastInto<i64>
        + Bounded
        + RunLengthEncodable
{
}
