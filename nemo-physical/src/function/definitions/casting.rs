//! This module defines functions on string.

use crate::{
    datavalues::{syntax::encodings, AnyDataValue, DataValue},
    storagevalues::storagetype::StorageType,
};

use super::{FunctionTypePropagation, UnaryFunction};

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
///   * strings
///   * other
///
/// Returns `None` when called on values outside the range described above
/// or if the value cannot be converted to an integer.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoInteger64;
impl UnaryFunction for CastingIntoInteger64 {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::Iri
            | crate::datavalues::ValueDomain::LanguageTaggedString => None,
            crate::datavalues::ValueDomain::PlainString | crate::datavalues::ValueDomain::Other => {
                let lex_val = parameter.lexical_value();

                // Handle decimal, binary (0b), octal (0o) and hexadecimal (0x) encoded strings
                // Expect 2 chars for encoding prefix and at least one char as suffix
                let result = {
                    if lex_val.chars().count() >= 3 {
                        match &lex_val[0..2] {
                            encodings::BIN => <i64>::from_str_radix(&lex_val[2..], 2).ok()?,
                            encodings::OCT => <i64>::from_str_radix(&lex_val[2..], 8).ok()?,
                            encodings::HEX => <i64>::from_str_radix(&lex_val[2..], 16).ok()?,
                            _ => lex_val.parse::<i64>().ok()?,
                        }
                    } else {
                        lex_val.parse::<i64>().ok()?
                    }
                };

                Some(AnyDataValue::new_integer_from_i64(result))
            }
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageType::Int64.bitset())
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
///   * strings
///   * other
///
/// Returns `None` when called on values outside the range described above
/// or if value cannot be converted into a 32-bit float.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoFloat;
impl UnaryFunction for CastingIntoFloat {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Iri
            | crate::datavalues::ValueDomain::Boolean => None,
            crate::datavalues::ValueDomain::PlainString | crate::datavalues::ValueDomain::Other => {
                // TODO: This is uses rusts string to float implementation and not ours
                let result = parameter.lexical_value().parse::<f32>().ok()?;

                Some(AnyDataValue::new_float_from_f32(result).ok()?)
            }
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageType::Float.bitset())
    }
}

/// Casting of a value into a 64-bit floating point number
///
/// Returns a 64-bit floating point number representing its input
/// as close as possible.
///
/// This operation is defined for:
///   * 64bit floating point numbers
///   * all integers
///   * 32-bit floating point numbers
///   * strings
///   * other
///
/// Returns `None` when called on values outside the range described above
/// or if value cannot be converted into a 64-bit float.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoDouble;
impl UnaryFunction for CastingIntoDouble {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        match parameter.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Iri
            | crate::datavalues::ValueDomain::Boolean => None,
            crate::datavalues::ValueDomain::PlainString | crate::datavalues::ValueDomain::Other => {
                // TODO: This is uses rusts string to float implementation and not ours
                let result = parameter.lexical_value().parse::<f64>().ok()?;

                Some(AnyDataValue::new_double_from_f64(result).ok()?)
            }
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageType::Double.bitset())
    }
}

/// Casting of a string value into an IRI
///
/// Returns an IRI with the same content as the given string.
///
/// Returns `None` when called on values other than plain strings.
#[derive(Debug, Copy, Clone)]
pub struct CastingIntoIri;
impl UnaryFunction for CastingIntoIri {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_plain_string()
            .or_else(|| parameter.to_iri())
            .map(AnyDataValue::new_iri)
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageType::Id32.bitset().union(StorageType::Id64.bitset()),
        )
    }
}
