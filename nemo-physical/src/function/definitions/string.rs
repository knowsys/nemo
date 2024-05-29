//! This module defines functions on string.

use std::cmp::Ordering;

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{
    BinaryFunction, FunctionTypePropagation, NaryFunction, TernaryFunction, UnaryFunction,
};

/// Given two [AnyDataValue]s,
/// check if both are strings and return a pair of [String]
/// if this is the case.
///
/// Returns `None` otherwise.
fn string_pair_from_any(
    parameter_first: AnyDataValue,
    parameter_second: AnyDataValue,
) -> Option<(String, String)> {
    Some((
        parameter_first.to_plain_string()?,
        parameter_second.to_plain_string()?,
    ))
}

/// Given a list of [AnyDataValue]s,
/// check if all of them are strings and return a list of [String]
/// if this is the case.
///
/// Returns `None` otherwise.
fn string_vec_from_any(parameters: &[AnyDataValue]) -> Option<Vec<String>> {
    let mut result = Vec::new();

    for parameter in parameters {
        result.push(parameter.to_plain_string()?);
    }

    Some(result)
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Concatenation of strings
///
/// Returns a string, that results from merging together
/// all input strings.
///
/// Returns an empty string if no parameters are given.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringConcatenation;
impl NaryFunction for StringConcatenation {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        string_vec_from_any(parameters)
            .map(|strings| AnyDataValue::new_plain_string(strings.concat()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Start of a string
///
/// Returns `true` from the boolean value space if the string provided as the first parameter
/// starts with the string provided as the first paramter and `false` otherwise.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringStarts;
impl BinaryFunction for StringStarts {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                if first_string.starts_with(&second_string) {
                    AnyDataValue::new_boolean(true)
                } else {
                    AnyDataValue::new_boolean(false)
                }
            },
        )
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// End of a string
///
/// Returns `true` from the boolean value space if the string provided as the first parameter
/// ends with the string provided as the first paramter and `false` otherwise.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringEnds;
impl BinaryFunction for StringEnds {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                if first_string.ends_with(&second_string) {
                    AnyDataValue::new_boolean(true)
                } else {
                    AnyDataValue::new_boolean(false)
                }
            },
        )
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// First part of a string
///
/// Returns the part of the string given in the first parameter which comes before
/// the string provided as the second parameter.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringBefore;
impl BinaryFunction for StringBefore {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                let result = first_string
                    .find(&second_string)
                    .map_or("", |position| &first_string[0..position])
                    .to_string();

                AnyDataValue::new_plain_string(result)
            },
        )
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Second part of a string
///
/// Returns the part of the string given in the first parameter which comes after
/// the string provided as the second parameter.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringAfter;
impl BinaryFunction for StringAfter {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        string_pair_from_any(parameter_first, parameter_second).map(
            |(first_string, second_string)| {
                let result = first_string
                    .find(&second_string)
                    .map_or("", |position| {
                        &first_string[(position + second_string.len())..]
                    })
                    .to_string();

                AnyDataValue::new_plain_string(result)
            },
        )
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Substring
///
/// Expects a string value as the first parameter and an integer value as the second.
///
/// Return a string containing the characters from the first parameter,
/// starting from the position given by the second paramter.
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
        let string = parameter_first.to_plain_string()?;
        let start = usize::try_from(parameter_second.to_u64()?).ok()?;

        if start >= string.len() {
            return None;
        }

        Some(AnyDataValue::new_plain_string(string[start..].to_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
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
            .to_plain_string()
            .map(|string| AnyDataValue::new_integer_from_u64(string.len() as u64))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
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
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(string.to_ascii_uppercase()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
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
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(string.to_ascii_lowercase()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Substring with Length
///
/// Expects a string value as the first parameter
/// and an integer value as the second and third parameter.
///
/// Return a string containing the characters from the first parameter,
/// starting from the position given by the second paramter
/// with the length given by the third parameter.
///
/// Returns `None` if the type requirements from above are not met.
#[derive(Debug, Copy, Clone)]
pub struct StringSubstringLength;
impl TernaryFunction for StringSubstringLength {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
        parameter_third: AnyDataValue,
    ) -> Option<AnyDataValue> {
        let string = parameter_first.to_plain_string()?;
        let start = usize::try_from(parameter_second.to_u64()?).ok()?;
        let length = usize::try_from(parameter_third.to_u64()?).ok()?;

        if start + length >= string.len() {
            return None;
        }

        Some(AnyDataValue::new_plain_string(
            string[start..(start + length)].to_string(),
        ))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
