//! This module implements the casting functions for [AnyDataValue].

use crate::{
    datavalues::{syntax::encodings, AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float},
};

use super::{
    storage_t::{
        casting_double_into_float, casting_double_into_integer, casting_float_into_double,
        casting_float_into_integer, casting_integer_into_double, casting_integer_into_float,
        casting_unsigned_integer_into_double, casting_unsigned_integer_into_float,
    },
    OperableCasting,
};

impl OperableCasting for AnyDataValue {
    fn casting_into_integer(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
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
            crate::datavalues::ValueDomain::Float => {
                let value = parameter.to_f32_unchecked();
                Some(AnyDataValue::new_integer_from_i64(
                    casting_float_into_integer(Float::new_unchecked(value))?,
                ))
            }
            crate::datavalues::ValueDomain::Double => {
                let value = parameter.to_f64_unchecked();
                Some(AnyDataValue::new_integer_from_i64(
                    casting_double_into_integer(Double::new_unchecked(value))?,
                ))
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

    fn casting_into_float(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
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
            crate::datavalues::ValueDomain::Double => {
                let value = parameter.to_f64_unchecked();
                let float = f32::from(casting_double_into_float(Double::new_unchecked(value)));

                Some(
                    AnyDataValue::new_float_from_f32(float)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::UnsignedLong => {
                let value = parameter.to_u64_unchecked();
                let float = f32::from(casting_unsigned_integer_into_float(value));

                Some(
                    AnyDataValue::new_float_from_f32(float)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => {
                let value = parameter.to_i64_unchecked();
                let float = f32::from(casting_integer_into_float(value));

                Some(
                    AnyDataValue::new_float_from_f32(float)
                        .expect("resulting float must be finite"),
                )
            }
        }
    }

    fn casting_into_double(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
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
            crate::datavalues::ValueDomain::Float => {
                let value = parameter.to_f32_unchecked();
                let double = f64::from(casting_float_into_double(Float::new_unchecked(value)));

                Some(
                    AnyDataValue::new_double_from_f64(double)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::UnsignedLong => {
                let value = parameter.to_u64_unchecked();
                let double = f64::from(casting_unsigned_integer_into_double(value));

                Some(
                    AnyDataValue::new_double_from_f64(double)
                        .expect("resulting float must be finite"),
                )
            }
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => {
                let value = parameter.to_i64_unchecked();
                let double = f64::from(casting_integer_into_double(value));

                Some(
                    AnyDataValue::new_double_from_f64(double)
                        .expect("resulting float must be finite"),
                )
            }
        }
    }

    fn casting_into_iri(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_plain_string()
            .or_else(|| parameter.to_iri())
            .map(AnyDataValue::new_iri)
    }
}
