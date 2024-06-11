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

        if start > string.len() || start < 1 {
            return None;
        }

        Some(AnyDataValue::new_plain_string(
            string[(start - 1)..].to_string(),
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

/// Transformation of a string into its reverse
///
/// Returns the reversed version of the provided string.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringReverse;
impl UnaryFunction for StringReverse {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(string.chars().rev().collect::<String>()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
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
/// starting from the position given by the second parameter
/// with the maximum length given by the third parameter.
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

        if start > string.len() || start < 1 {
            return None;
        }

        let length = usize::try_from(parameter_third.to_u64()?).ok()?;
        let end = start + length;

        let result = if end > string.len() {
            string[(start - 1)..].to_string()
        } else {
            string[(start - 1)..(end - 1)].to_string()
        };

        Some(AnyDataValue::new_plain_string(result))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{datavalues::AnyDataValue, function::definitions::TernaryFunction};

    use super::StringSubstringLength;

    #[test]
    fn test_string_substring_length() {
        let string = AnyDataValue::new_plain_string("abc".to_string());

        let start1 = AnyDataValue::new_integer_from_u64(1);
        let length1 = AnyDataValue::new_integer_from_u64(1);
        let result1 = AnyDataValue::new_plain_string("a".to_string());
        let actual_result1 = StringSubstringLength.evaluate(string.clone(), start1, length1);
        assert!(actual_result1.is_some());
        assert_eq!(result1, actual_result1.unwrap());

        let start2 = AnyDataValue::new_integer_from_u64(2);
        let length2 = AnyDataValue::new_integer_from_u64(1);
        let result2 = AnyDataValue::new_plain_string("b".to_string());
        let actual_result2 = StringSubstringLength.evaluate(string.clone(), start2, length2);
        assert!(actual_result2.is_some());
        assert_eq!(result2, actual_result2.unwrap());

        let start3 = AnyDataValue::new_integer_from_u64(3);
        let length3 = AnyDataValue::new_integer_from_u64(1);
        let result3 = AnyDataValue::new_plain_string("c".to_string());
        let actual_result3 = StringSubstringLength.evaluate(string.clone(), start3, length3);
        assert!(actual_result3.is_some());
        assert_eq!(result3, actual_result3.unwrap());

        let start4 = AnyDataValue::new_integer_from_u64(4);
        let length4 = AnyDataValue::new_integer_from_u64(1);
        let actual_result4 = StringSubstringLength.evaluate(string.clone(), start4, length4);
        assert!(actual_result4.is_none());

        let start5 = AnyDataValue::new_integer_from_u64(1);
        let length5 = AnyDataValue::new_integer_from_u64(3);
        let result5 = AnyDataValue::new_plain_string("abc".to_string());
        let actual_result5 = StringSubstringLength.evaluate(string.clone(), start5, length5);
        assert!(actual_result5.is_some());
        assert_eq!(result5, actual_result5.unwrap());

        let start6 = AnyDataValue::new_integer_from_u64(1);
        let length6 = AnyDataValue::new_integer_from_u64(4);
        let result6 = AnyDataValue::new_plain_string("abc".to_string());
        let actual_result6 = StringSubstringLength.evaluate(string.clone(), start6, length6);
        assert!(actual_result6.is_some());
        assert_eq!(result6, actual_result6.unwrap());

        let start7 = AnyDataValue::new_integer_from_u64(0);
        let length7 = AnyDataValue::new_integer_from_u64(4);
        let actual_result7 = StringSubstringLength.evaluate(string.clone(), start7, length7);
        assert!(actual_result7.is_none());
    }
}
