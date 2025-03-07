//! This module implements the casting functions for [StorageValueT].

use crate::storagevalues::{double::Double, float::Float, storagevalue::StorageValueT};

use super::OperableCasting;

// TODO: Review if functions below can be implemented in a more accurate way?

/// Casting of [Float] into [i64]
pub(super) fn casting_float_into_integer(float: Float) -> Option<i64> {
    if float.floor() != float {
        return None;
    }

    // TODO: What to do if float value is too large?
    Some(f32::from(float) as i64)
}

/// Casting of [Double] into [i64]
pub(super) fn casting_double_into_integer(double: Double) -> Option<i64> {
    if double.floor() != double {
        return None;
    }

    // TODO: What to do if float value is too large?
    Some(f64::from(double) as i64)
}

/// Casting [i64] into [Float]
pub(super) fn casting_integer_into_float(integer: i64) -> Float {
    Float::new_unchecked(integer as f32)
}

/// Casting [u64] into [Float]
pub(super) fn casting_unsigned_integer_into_float(integer: u64) -> Float {
    Float::new_unchecked(integer as f32)
}

/// Casting [Double] into [Float]
pub(super) fn casting_double_into_float(double: Double) -> Float {
    Float::new_unchecked(f64::from(double) as f32)
}

/// Casting [i64] into [Double]
pub(crate) fn casting_integer_into_double(integer: i64) -> Double {
    Double::new_unchecked(integer as f64)
}

/// Casting [u64] into [Double]
pub(super) fn casting_unsigned_integer_into_double(integer: u64) -> Double {
    Double::new_unchecked(integer as f64)
}

/// Casting [Float] into [Double]
pub(crate) fn casting_float_into_double(float: Float) -> Double {
    Double::new_unchecked(f32::from(float) as f64)
}

impl OperableCasting for StorageValueT {
    fn casting_into_integer(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let result = match parameter {
            StorageValueT::Int64(integer) => integer,
            StorageValueT::Float(float) => casting_float_into_integer(float)?,
            StorageValueT::Double(double) => casting_double_into_integer(double)?,
            StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
        };

        Some(StorageValueT::Int64(result))
    }

    fn casting_into_float(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let result = match parameter {
            StorageValueT::Float(_) => return Some(parameter),
            StorageValueT::Int64(integer) => casting_integer_into_float(integer),
            StorageValueT::Double(double) => casting_double_into_float(double),
            StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
        };

        Some(StorageValueT::Float(result))
    }

    fn casting_into_double(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let result = match parameter {
            StorageValueT::Double(_) => return Some(parameter),
            StorageValueT::Int64(integer) => casting_integer_into_double(integer),
            StorageValueT::Float(float) => casting_float_into_double(float),
            StorageValueT::Id32(_) | StorageValueT::Id64(_) => return None,
        };

        Some(StorageValueT::Double(result))
    }
}
