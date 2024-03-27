use std::fmt::Debug;

use num::{Bounded, CheckedMul};

use crate::{
    datatypes::into_datavalue::IntoDataValue,
    function::definitions::numeric::traits::ArithmeticOperations,
};

use super::{Field, FloorToUsize, RunLengthEncodable};

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
{
}
