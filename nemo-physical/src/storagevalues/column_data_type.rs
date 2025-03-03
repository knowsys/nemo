use std::fmt::Debug;

use num::{Bounded, CheckedMul};

use crate::{
    function::definitions::numeric::traits::ArithmeticOperations,
    storagevalues::into_datavalue::IntoDataValue,
};

use super::{Field, FloorToUsize, RunLengthEncodable};

/// A combination of traits that is required for a data type to be used in a column
pub(crate) trait ColumnDataType:
    Debug
    + Copy
    + Ord
    + TryFrom<usize>
    + FloorToUsize
    + CheckedMul
    + Field
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
        + CheckedMul
        + Field
        + Bounded
        + RunLengthEncodable
        + ArithmeticOperations
        + IntoDataValue
{
}
