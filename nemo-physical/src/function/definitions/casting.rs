//! This module defines functions on string.

use crate::datavalues::{AnyDataValue, DataValue};

use super::UnaryFunction;

/// Casting of values into 64-bit integers
///
/// Returns an integer number representing its input
/// as close as possible.
///
/// This operation is defined for
///   * signed 64-bit integers
///   * unsigned integers that can be represented in 63 bits
///   * floating point numbers that don't contain a fractional part
///   * booleans
///
/// Returns `None` when called on values outside the range described above.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoInteger64;
impl UnaryFunction for CastingIntoInteger64 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Other
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::String
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Iri => None,
            // FIXME: This seems suspicious. Can't f32 also represent integer numbers *without any fraction) that are still too large for i64?
            crate::datavalues::ValueDomain::Float => {
                let value = parameter.to_f32_unchecked();
                if value.round() == value {
                    #[allow(clippy::cast_possible_truncation)]
                    Some(AnyDataValue::new_integer_from_i64(value as i64))
                } else {
                    None
                }
            }
            // FIXME: This seems suspicious. Can't f64 also represent integer numbers *without any fraction) that are still too large for i64?
            crate::datavalues::ValueDomain::Double => {
                let value = parameter.to_f64_unchecked();
                if value.round() == value {
                    #[allow(clippy::cast_possible_truncation)]
                    Some(AnyDataValue::new_integer_from_i64(value as i64))
                } else {
                    None
                }
            }
            crate::datavalues::ValueDomain::Boolean => {
                if parameter.to_boolean_unchecked() {
                    Some(AnyDataValue::new_integer_from_i64(1))
                } else {
                    Some(AnyDataValue::new_integer_from_i64(0))
                }
            }
            crate::datavalues::ValueDomain::UnsignedLong => None,
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => Some(parameter),
        }
    }
}

/// Casting of a value into a 32bit floating point number
///
/// Returns a 32-bit floating point number representing its input
/// as close as possible.
///
/// This operation is defined for:
///   * 32bit floating point numbers
///   * all integers
///   * 64bit floating point numbers
///
/// Returns `None` when called on values outside the range described above.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoFloat;
impl UnaryFunction for CastingIntoFloat {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::Other
            | crate::datavalues::ValueDomain::String
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Boolean
            | crate::datavalues::ValueDomain::Iri => None,
            crate::datavalues::ValueDomain::Float => Some(parameter),
            crate::datavalues::ValueDomain::Double =>
            {
                #[allow(clippy::cast_possible_truncation)]
                Some(
                    AnyDataValue::new_float_from_f32(parameter.to_f64_unchecked() as f32)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::UnsignedLong => Some(
                AnyDataValue::new_float_from_f32(parameter.to_u64_unchecked() as f32)
                    .expect("resulting float must be finite"),
            ),
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => Some(
                AnyDataValue::new_float_from_f32(parameter.to_i64_unchecked() as f32)
                    .expect("resulting float must be finite"),
            ),
        }
    }
}

/// Casting of a value into a 64-bit floating point number
///
/// Returns a 64-bit floating point number representing its input
/// as close as possible.
///
/// This operation is defined for:
///   * all integers
///   * 32-bit floating point numbers
///   * 64-bit floating point numbers
///
/// Returns `None` when called on values outside the range described above.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoDouble;
impl UnaryFunction for CastingIntoDouble {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Other
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::String
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Boolean
            | crate::datavalues::ValueDomain::Iri => None,
            crate::datavalues::ValueDomain::Double => Some(parameter),
            crate::datavalues::ValueDomain::Float =>
            {
                #[allow(clippy::cast_possible_truncation)]
                Some(
                    AnyDataValue::new_double_from_f64(parameter.to_f32_unchecked() as f64)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::UnsignedLong => Some(
                AnyDataValue::new_double_from_f64(parameter.to_u64_unchecked() as f64)
                    .expect("resulting float must be finite"),
            ),
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => Some(
                AnyDataValue::new_double_from_f64(parameter.to_i64_unchecked() as f64)
                    .expect("resulting float must be finite"),
            ),
        }
    }
}
