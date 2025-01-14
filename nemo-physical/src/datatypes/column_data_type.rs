use core::f32;
use std::{fmt::Debug, u32};

use num::{Bounded, CheckedMul};

use crate::{
    datatypes::into_datavalue::IntoDataValue,
    function::definitions::numeric::traits::ArithmeticOperations,
};

use super::{Double, Field, Float, FloorToUsize, RunLengthEncodable};

/// A combination of traits that is required for a data type to be used in a column
pub(crate) trait ColumnDataType:
    Debug
    + Copy
    + Ord
    + TryFrom<usize>
    + FloorToUsize
    + Field
    + CheckedMul
    + Bounded
    + RunLengthEncodable
    + ArithmeticOperations
    + IntoDataValue
    + DeletedValue
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
        + Bounded
        + RunLengthEncodable
        + ArithmeticOperations
        + IntoDataValue
        + DeletedValue
{
}

pub trait DeletedValue {
    fn deleted_value() -> Self;
}

impl DeletedValue for u32 {
    fn deleted_value() -> Self {
        u32::MAX
    }
}

impl DeletedValue for u16 {
    fn deleted_value() -> Self {
        u16::MAX
    }
}

impl DeletedValue for u8 {
    fn deleted_value() -> Self {
        u8::MAX
    }
}

impl DeletedValue for u64 {
    fn deleted_value() -> Self {
        u64::MAX
    }
}
impl DeletedValue for i64 {
    fn deleted_value() -> Self {
        i64::MAX
    }
}

impl DeletedValue for i32 {
    fn deleted_value() -> Self {
        i32::MAX
    }
}

impl DeletedValue for Float {
    fn deleted_value() -> Self {
        Float::new_unchecked(f32::NAN)
    }
}

impl DeletedValue for Double {
    fn deleted_value() -> Self {
        Double::new_unchecked(f64::NAN)
    }
}

impl DeletedValue for usize {
    fn deleted_value() -> Self {
        usize::MAX
    }
}
