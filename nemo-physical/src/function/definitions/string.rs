//! This module defines functions on string.

use std::cmp::Ordering;

use crate::datavalues::{AnyDataValue, DataValue};

use super::{BinaryFunction, UnaryFunction};

/// Given two [AnyDataValue]s,
/// check if both are strings and return a pair of [String]
/// if this is the case.
/// Returns `None` otherwise.
fn string_pair_from_any(
    parameter_first: AnyDataValue,
    parameter_second: AnyDataValue,
) -> Option<(String, String)> {
    if let Some(first_string) = parameter_first.to_string() {
        if let Some(second_string) = parameter_second.to_string() {
            return Some((first_string, second_string));
        }
    }

    None
}

/// Comparison of strings
///
/// Evaluates to -1 from the integer value space if the first string is alphabetically smaller than the second.
/// Evaluates to 0 from the integer value space if both strings are equal.
/// Evaluates to 1 from the integer value space if the second string is alphabetically larger than the first.
#[derive(Debug, Copy, Clone)]
pub struct StringCompare;
impl BinaryFunction for StringCompare {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| match first_string.cmp(&second_string) {
                Ordering::Less => AnyDataValue::new_integer_from_i64(-1),
                Ordering::Equal => AnyDataValue::new_integer_from_i64(0),
                Ordering::Greater => AnyDataValue::new_integer_from_i64(1),
            },
        )
    }
}

/// Concatenation of strings
///
/// Returns a string, that results from appending the second parameter
/// to the first.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringConcatenation;
impl BinaryFunction for StringConcatenation {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                AnyDataValue::new_string([first_string, second_string].concat())
            },
        )
    }
}

/// Containment of strings
///
/// Returns `true` from the boolean value space if the string provided as the second parameter
/// is contained in the string provided as the first parameter and `false` otherwise.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringContains;
impl BinaryFunction for StringContains {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                if first_string.contains(&second_string) {
                    AnyDataValue::new_boolean(true)
                } else {
                    AnyDataValue::new_boolean(false)
                }
            },
        )
    }
}

/// Substring
///
/// Expects a string value as the first parameter and an integer value as the second.
///
/// Return a string containing the first $n$ characters from the first parameter,
/// where $n$ equals to the value supplied as the second parameter.
///
/// Returns `None` if the type requirements from above are not met.
#[derive(Debug, Copy, Clone)]
pub struct StringSubstring;
impl BinaryFunction for StringSubstring {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        let string = parameter_first.to_string()?;
        let length = parameter_second.to_u64()? as usize;

        Some(AnyDataValue::new_string(string[0..length].to_string()))
    }
}

/// Length of a string
///
/// Returns the length of the given string as a number from the integer value space.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringLength;
impl UnaryFunction for StringLength {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_string()
            .map(|string| AnyDataValue::new_integer_from_u64(string.len() as u64))
    }
}

/// Transformation of a string into upper case
///
/// Returns the upper case version of the provided string.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUppercase;
impl UnaryFunction for StringUppercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_string()
            .map(|string| AnyDataValue::new_string(string.to_ascii_uppercase()))
    }
}

/// Transformation of a string into lower case
///
/// Returns the lower case version of the provided string.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringLowercase;
impl UnaryFunction for StringLowercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_string()
            .map(|string| AnyDataValue::new_string(string.to_ascii_lowercase()))
    }
}
