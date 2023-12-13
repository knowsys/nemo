//! This module defines functions on string.

use crate::{
    datatypes::{Double, Float},
    datavalues::{AnyDataValue, DataValue},
};

use super::UnaryFunction;

/// Casting of values into 64-bit integers
///
/// This operation is defined for
///   * unsigned integers as long as they can be represented in 63 bits.
///   * floating point numbers as long as they don't contain a fractional part.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoInteger64;
impl UnaryFunction for CastingIntoInteger64 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter {
            AnyDataValue::String(_) => None,
            AnyDataValue::LanguageTaggedString(_) => None,
            AnyDataValue::Iri(_) => None,
            AnyDataValue::Float(value) => Some(AnyDataValue::new_integer_from_i64(
                value.to_double_unchecked().as_i64()?,
            )),
            AnyDataValue::Double(value) => Some(AnyDataValue::new_integer_from_i64(
                value.to_double_unchecked().as_i64()?,
            )),
            AnyDataValue::UnsignedLong(value) => {
                Some(AnyDataValue::new_integer_from_i64(value.to_i64()?))
            }
            AnyDataValue::Long(_) => Some(parameter),
            AnyDataValue::Other(_) => None,
            AnyDataValue::Boolean(_) => None,
        }
    }
}

/// Casting of a value into a 32bit floating point number
///
/// This operation is defined for:
///   * Integers
///   * 64bit floating point numbers
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoFloat;
impl UnaryFunction for CastingIntoFloat {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter {
            AnyDataValue::String(_) => None,
            AnyDataValue::LanguageTaggedString(_) => None,
            AnyDataValue::Iri(_) => None,
            AnyDataValue::Float(_) => Some(parameter),
            AnyDataValue::Double(value) => Some(AnyDataValue::new_float(
                value.to_double_unchecked().as_float()?,
            )),
            AnyDataValue::UnsignedLong(value) => Some(AnyDataValue::new_float(Float::from_u64(
                value.to_u64_unchecked(),
            )?)),
            AnyDataValue::Long(value) => Some(AnyDataValue::new_float(Float::from_i64(
                value.to_i64_unchecked(),
            )?)),
            AnyDataValue::Other(_) => None,
            AnyDataValue::Boolean(_) => None,
        }
    }
}

/// Casting of a value into a 64-bit floating point number
///
/// This operation is defined for:
///   * Integers
///   * 32-bit floating point numbers
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoDouble;
impl UnaryFunction for CastingIntoDouble {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter {
            AnyDataValue::String(_) => None,
            AnyDataValue::LanguageTaggedString(_) => None,
            AnyDataValue::Iri(_) => None,
            AnyDataValue::Float(value) => Some(AnyDataValue::new_double(
                value.to_float_unchecked().as_double()?,
            )),
            AnyDataValue::Double(_) => Some(parameter),
            AnyDataValue::UnsignedLong(value) => Some(AnyDataValue::new_double(Double::from_u64(
                value.to_u64_unchecked(),
            )?)),
            AnyDataValue::Long(value) => Some(AnyDataValue::new_double(Double::from_i64(
                value.to_i64_unchecked(),
            )?)),
            AnyDataValue::Other(_) => None,
            AnyDataValue::Boolean(_) => None,
        }
    }
}
