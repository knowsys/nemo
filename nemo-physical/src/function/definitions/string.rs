//! This module defines functions on string.

use levenshtein::levenshtein;
use once_cell::sync::OnceCell;
use std::{cmp::Ordering, num::NonZero, sync::Mutex};
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue, ValueDomain},
};

use super::{
    BinaryFunction, FunctionTypePropagation, NaryFunction, TernaryFunction, UnaryFunction,
};

/// Unicode friendly version of [String] find
///
/// Returns the index of the first occurrence of the needle in the haystack
/// or `None` if the needle is not found.
fn unicode_find(haystack: &str, needle: &str) -> Option<usize> {
    let haystack_graphemes = haystack.graphemes(true).collect::<Vec<&str>>();
    let needle_graphemes = needle.graphemes(true).collect::<Vec<&str>>();
    if needle_graphemes.len() > haystack_graphemes.len() {
        return None;
    }
    (0..(haystack_graphemes.len() - needle_graphemes.len() + 1))
        .find(|&i| needle_graphemes == haystack_graphemes[i..i + needle_graphemes.len()])
}

#[derive(Clone)]
pub(crate) struct LangTaggedString {
    string: String,
    tag: Option<String>,
}

impl LangTaggedString {
    fn new(string: String, tag: Option<String>) -> Self {
        Self { string, tag }
    }

    fn into_data_value(self) -> AnyDataValue {
        match self.tag {
            Some(tag) => AnyDataValue::new_language_tagged_string(self.string, tag),
            None => AnyDataValue::new_plain_string(self.string),
        }
    }
    
    pub fn tag_into_data_value(self) -> Option<AnyDataValue> {
        self.tag.map(AnyDataValue::new_plain_string)
    }
}

impl TryFrom<AnyDataValue> for LangTaggedString {
    type Error = ();

    fn try_from(parameter: AnyDataValue) -> Result<Self, ()> {
        match parameter.value_domain() {
            ValueDomain::PlainString => Ok(Self::new(parameter.to_plain_string_unchecked(), None)),
            ValueDomain::LanguageTaggedString => {
                let (string, lang_tag) = parameter.to_language_tagged_string_unchecked();
                Ok(Self::new(string, Some(lang_tag)))
            }
            _ => Err(()),
        }
    }
}

/// Given two [AnyDataValue]s,
/// check if both are plain strings or language tagged strings and return a pair of [LangString]
/// if this is the case and
///
/// Returns `None` otherwise.
fn lang_string_pair_from_any(
    parameter_first: AnyDataValue,
    parameter_second: AnyDataValue,
) -> Option<(LangTaggedString, LangTaggedString)> {
    let lang_string_first = LangTaggedString::try_from(parameter_first).ok()?;
    let lang_string_second = LangTaggedString::try_from(parameter_second).ok()?;

    // Implement the Argument Compatibility Rules for language tags from https://www.w3.org/TR/sparql11-query/#func-arg-compatibility
    match (&lang_string_first.tag, &lang_string_second.tag) {
        (None, None) | (Some(_), None) => Some((lang_string_first, lang_string_second)),
        (Some(tag_first), Some(tag_second)) if tag_first == tag_second => {
            Some((lang_string_first, lang_string_second))
        }
        _ => None,
    }
}

/// Given a list of [AnyDataValue]s,
/// check if all of them are strings and return a list of [String]
/// if this is the case.
///
/// Returns `None` otherwise.
fn string_vec_from_any(parameters: &[AnyDataValue]) -> Option<Vec<LangTaggedString>> {
    let mut result = Vec::new();

    for parameter in parameters {
        result.push(LangTaggedString::try_from(parameter.clone()).ok()?);
    }

    Some(result)
}


/// Comparison of strings
///
/// Evaluates to -1 from the integer value space if the first string is alphabetically smaller than the second.
/// Evaluates to 0 from the integer value space if both strings are equal.
/// Evaluates to 1 from the integer value space if the second string is alphabetically larger than the first.
///
/// Language tags that comply with the Argument Compatibility Rules are ignored.
#[derive(Debug, Copy, Clone)]
pub struct StringCompare;
impl BinaryFunction for StringCompare {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| match first_lang_string
                .string
                .cmp(&second_lang_string.string)
            {
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
/// Adds a language tag if all parameters have the identical language tag
///
/// Returns an empty string if no parameters are given.
///
/// Returns `None` if either parameter is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringConcatenation;
impl NaryFunction for StringConcatenation {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        let lang_strings = string_vec_from_any(parameters)?;
        let result = lang_strings
            .iter()
            .map(|ls| ls.string.as_str())
            .collect::<String>();

        let mut iter = lang_strings.into_iter().map(|ls| ls.tag);
        let result_tag = iter
            .next()
            .flatten()
            .filter(|first| iter.all(|tag| tag.as_ref() == Some(first)));
        Some(LangTaggedString::new(result, result_tag).into_data_value())
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
/// Language tags that comply with the Argument Compatibility Rules are ignored.
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                AnyDataValue::new_boolean(
                    unicode_find(&first_lang_string.string, &second_lang_string.string).is_some(),
                )
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
/// starts with the string provided as the second parameter and `false` otherwise.
///
/// Language tags that comply with the Argument Compatibility Rules are ignored.
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                let first_graphemes = first_lang_string
                    .string
                    .graphemes(true)
                    .collect::<Vec<&str>>();
                let second_graphemes = second_lang_string
                    .string
                    .graphemes(true)
                    .collect::<Vec<&str>>();
                if second_graphemes.len() > first_graphemes.len() {
                    return AnyDataValue::new_boolean(false);
                }
                AnyDataValue::new_boolean(
                    first_graphemes[..second_graphemes.len()] == second_graphemes,
                )
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
/// ends with the string provided as the second parameter and `false` otherwise.
///
/// Language tags that comply with the Argument Compatibility Rules are ignored.
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                let first_graphemes = first_lang_string
                    .string
                    .graphemes(true)
                    .collect::<Vec<&str>>();
                let second_graphemes = second_lang_string
                    .string
                    .graphemes(true)
                    .collect::<Vec<&str>>();
                if second_graphemes.len() > first_graphemes.len() {
                    return AnyDataValue::new_boolean(false);
                }
                AnyDataValue::new_boolean(
                    first_graphemes[first_graphemes.len() - second_graphemes.len()..]
                        == second_graphemes,
                )
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
/// Language tags that comply with the Argument Compatibility Rules are ignored.
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                let result_lang_string =
                    match unicode_find(&first_lang_string.string, &second_lang_string.string) {
                        Some(i) => {
                            let string = first_lang_string
                                .string
                                .graphemes(true)
                                .collect::<Vec<&str>>()[..i]
                                .join("");
                            LangTaggedString::new(string, first_lang_string.tag)
                        }
                        None => {
                            // SPARQL defines to only apply the language tag of the first parameter if the second parameter is empty
                            let tag = if second_lang_string.string.is_empty() {
                                first_lang_string.tag
                            } else {
                                None
                            };
                            LangTaggedString::new(String::new(), tag)
                        }
                    };
                result_lang_string.into_data_value()
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
/// Language tags that comply with the Argument Compatibility Rules are ignored.
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                let result_lang_string =
                    match unicode_find(&first_lang_string.string, &second_lang_string.string) {
                        Some(i) => {
                            let string = first_lang_string
                                .string
                                .graphemes(true)
                                .collect::<Vec<&str>>()[i + second_lang_string
                                .string
                                .graphemes(true)
                                .collect::<Vec<&str>>()
                                .len()..]
                                .join("");
                            LangTaggedString::new(string, first_lang_string.tag)
                        }
                        None => {
                            // SPARQL defines to only apply the language tag of the first parameter if the second parameter is empty
                            let tag = if second_lang_string.string.is_empty() {
                                first_lang_string.tag
                            } else {
                                None
                            };
                            LangTaggedString::new(String::new(), tag)
                        }
                    };
                result_lang_string.into_data_value()
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
/// Preserves the original lanuage tag, if available.
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
        let lang_string = LangTaggedString::try_from(parameter_first).ok()?;
        let start = usize::try_from(parameter_second.to_i64().map(|val| val.max(1))?).ok()?;

        let graphemes = lang_string.string.graphemes(true).collect::<Vec<&str>>();

        let result = graphemes
            .get((start - 1)..)
            .map_or_else(String::new, |slice| slice.join(""));

        Some(LangTaggedString::new(result, lang_string.tag).into_data_value())
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

const REGEX_CACHE_SIZE: NonZero<usize> = NonZero::new(32).unwrap();
static REGEX_CACHE: OnceCell<Mutex<lru::LruCache<String, regex::Regex>>> = OnceCell::new();

/// Regex string matching
///
/// Returns `true` from the boolean value space if the regex provided as the second parameter
/// is matched in the string provided as the first parameter and `false` otherwise.
///
/// Returns `None` if either parameter is not a string or the second parameter is not
/// a regular expression.
#[derive(Debug, Copy, Clone)]
pub struct StringRegex;
impl BinaryFunction for StringRegex {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(lang_string, lang_pattern)| {
                let mut cache = REGEX_CACHE
                    .get_or_init(|| Mutex::new(lru::LruCache::new(REGEX_CACHE_SIZE)))
                    .lock()
                    .unwrap();

                let regex = cache.try_get_or_insert(lang_pattern.string.clone(), || {
                    regex::Regex::new(&lang_pattern.string)
                });

                match regex {
                    Ok(regex) => AnyDataValue::new_boolean(regex.is_match(&lang_string.string)),
                    Err(_) => AnyDataValue::new_boolean(false),
                }
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

/// Levenshtein distance between two strings
///
/// Returns the Levenshtein distance (i.e., the minimal number of
/// insertions, deletions, or character substitutions required to
/// change one argument into the other) between the two given strings
/// as a number from the integer value space.
///
/// Language tags that comply with the Argument Compatibility Rules are ignored.
///
/// Return `None` if the the provided arguments are not both strings.
#[derive(Debug, Copy, Clone)]
pub struct StringLevenshtein;
impl BinaryFunction for StringLevenshtein {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(from, to)| {
                AnyDataValue::new_integer_from_u64(levenshtein(&from.string, &to.string) as u64)
            },
        )
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
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
        LangTaggedString::try_from(parameter).ok().map(|lang_string: LangTaggedString| {
            AnyDataValue::new_integer_from_u64(lang_string.string.graphemes(true).count() as u64)
        })
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Transformation of a string into its reverse
///
/// Returns the reversed version of the provided string.
/// Preserves the original lanuage tag, if available.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringReverse;
impl UnaryFunction for StringReverse {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        LangTaggedString::try_from(parameter).ok().map(|lang_string| {
            let reverse = lang_string.string.graphemes(true).rev().collect::<String>();
            LangTaggedString::new(reverse, lang_string.tag).into_data_value()
        })
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
/// Preserves the original lanuage tag, if available.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUppercase;
impl UnaryFunction for StringUppercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        LangTaggedString::try_from(parameter).ok().map(|lang_string| {
            let ucase = lang_string.string.to_uppercase();
            LangTaggedString::new(ucase, lang_string.tag).into_data_value()
        })
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
/// Preserves the original lanuage tag, if available.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringLowercase;
impl UnaryFunction for StringLowercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        LangTaggedString::try_from(parameter).ok().map(|lang_string| {
            let lcase = lang_string.string.to_lowercase();
            LangTaggedString::new(lcase, lang_string.tag).into_data_value()
        })
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// URI encoding (percent encoding) of a string
///
/// Returns the percent-encoded version of the provided string.
/// Ignores language tags.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUriEncode;
impl UnaryFunction for StringUriEncode {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        LangTaggedString::try_from(parameter).ok().map(|lang_string| {
            let uri_encode = urlencoding::encode(&lang_string.string).to_string();
            LangTaggedString::new(uri_encode, None).into_data_value()
        })
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// URI encoding (percent encoding) of a string
///
/// Returns the percent-encoded version of the provided string.
/// Ignores language tags.
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUriDecode;
impl UnaryFunction for StringUriDecode {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let lang_string = LangTaggedString::try_from(parameter).ok()?;
        let uri_decode = urlencoding::decode(&lang_string.string).ok()?.to_string();
        Some(LangTaggedString::new(uri_decode, None).into_data_value())
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
/// Preserves the original lanuage tag, if available.
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
        let lang_string = LangTaggedString::try_from(parameter_first).ok()?;
        let graphemes = lang_string.string.graphemes(true).collect::<Vec<&str>>();
        let start = parameter_second.to_i64()?;
        let length = parameter_third.to_i64()?;

        let result = match length {
            length if length < 1 => String::new(),
            _ => {
                let end = usize::try_from(start + length).ok()?;
                let start = usize::try_from(start.max(1)).ok()?;
                if start > graphemes.len() {
                    String::new()
                } else {
                    graphemes
                        .get((start - 1)..(end - 1))
                        .or_else(|| graphemes.get((start - 1)..))
                        .expect("Start index is validated")
                        .join("")
                }
            }
        };

        Some(LangTaggedString::new(result, lang_string.tag).into_data_value())
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

    use crate::{
        datavalues::AnyDataValue,
        function::definitions::{BinaryFunction, NaryFunction, TernaryFunction, UnaryFunction},
    };

    use super::{
        StringAfter, StringBefore, StringCompare, StringConcatenation, StringContains, StringEnds,
        StringLength, StringLevenshtein, StringLowercase, StringRegex, StringReverse, StringStarts,
        StringSubstring, StringSubstringLength, StringUppercase, StringUriDecode, StringUriEncode,
    };

    #[test]
    fn test_string_length() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let result_string = AnyDataValue::new_integer_from_u64(3);
        let actual_result_string = StringLength.evaluate(string);
        assert!(actual_result_string.is_some());
        assert_eq!(result_string, actual_result_string.unwrap());

        let null_string = AnyDataValue::new_plain_string("".to_string());
        let result_null_string = AnyDataValue::new_integer_from_u64(0);
        let actual_result_null_string = StringLength.evaluate(null_string);
        assert!(actual_result_null_string.is_some());
        assert_eq!(result_null_string, actual_result_null_string.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let result_unicode = AnyDataValue::new_integer_from_u64(5);
        let actual_result_unicode = StringLength.evaluate(string_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let actual_result_notstring = StringLength.evaluate(string_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("chat".to_string(), "en".to_string());
        let result_lang = AnyDataValue::new_integer_from_u64(4);
        let actual_result_lang = StringLength.evaluate(string_lang);
        assert_eq!(result_lang, actual_result_lang.unwrap());
    }

    #[test]
    fn test_string_contains() {
        let string_first_unicode = AnyDataValue::new_plain_string("oẅks".to_string());
        let string_second_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode =
            StringContains.evaluate(string_first_unicode.clone(), string_second_unicode.clone());
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_first_empty = AnyDataValue::new_plain_string("abc".to_string());
        let string_second_empty = AnyDataValue::new_plain_string("".to_string());
        let result_empty = AnyDataValue::new_boolean(true);
        let actual_result_empty =
            StringContains.evaluate(string_first_empty.clone(), string_second_empty.clone());
        assert!(actual_result_empty.is_some());
        assert_eq!(result_empty, actual_result_empty.unwrap());

        let string_first_impossible = AnyDataValue::new_plain_string("".to_string());
        let string_second_impossible = AnyDataValue::new_plain_string("abc".to_string());
        let actual_result_impossible = StringContains.evaluate(
            string_first_impossible.clone(),
            string_second_impossible.clone(),
        );
        let result_impossible = AnyDataValue::new_boolean(false);
        assert!(actual_result_impossible.is_some());
        assert_eq!(result_impossible, actual_result_impossible.unwrap());

        let string_lang1 =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let substring_lang1 =
            AnyDataValue::new_language_tagged_string("foo".to_string(), "en".to_string());
        let result_lang1 = AnyDataValue::new_boolean(true);
        let actual_result_lang1 = StringContains.evaluate(string_lang1, substring_lang1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(actual_result_lang1.unwrap(), result_lang1);

        let string_lang2 =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let substring_lang2 = AnyDataValue::new_plain_string("foo".to_string());
        let result_lang2 = AnyDataValue::new_boolean(true);
        let actual_result_lang2 = StringContains.evaluate(string_lang2, substring_lang2);
        assert!(actual_result_lang2.is_some());
        assert_eq!(actual_result_lang2.unwrap(), result_lang2);

        let string_error =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let substring_error =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "y".to_string());
        let actual_result_error = StringContains.evaluate(string_error, substring_error);
        assert!(actual_result_error.is_none());
    }

    #[test]
    fn test_uppercase() {
        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let result_unicode = AnyDataValue::new_plain_string("LOẄKS".to_string());
        let actual_result_unicode = StringUppercase.evaluate(string_unicode.clone());
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_lang =
            AnyDataValue::new_language_tagged_string("foo".to_string(), "en".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("FOO".to_string(), "en".to_string());
        let actual_result_lang = StringUppercase.evaluate(string_lang.clone());
        assert!(actual_result_lang.is_some());
        assert_eq!(result_lang, actual_result_lang.unwrap());
    }

    #[test]
    fn test_lowercase() {
        let string_unicode = AnyDataValue::new_plain_string("LOẄKS".to_string());
        let result_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let actual_result_unicode = StringLowercase.evaluate(string_unicode.clone());
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let actual_result_notstring = StringLowercase.evaluate(string_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("FOO".to_string(), "en".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("foo".to_string(), "en".to_string());
        let actual_result_lang = StringLowercase.evaluate(string_lang.clone());
        assert!(actual_result_lang.is_some());
        assert_eq!(result_lang, actual_result_lang.unwrap());
    }

    #[test]
    fn test_string_substring() {
        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_integer_from_u64(3);
        let result_unicode = AnyDataValue::new_plain_string("ẅks".to_string());
        let actual_result_unicode = StringSubstring.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let start_unicode_negative = AnyDataValue::new_integer_from_i64(-1);
        let actual_result_unicode =
            StringSubstring.evaluate(string_unicode.clone(), start_unicode_negative);
        assert!(actual_result_unicode.is_some());
        assert_eq!(string_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_integer_from_u64(3);
        let actual_result_notstring = StringSubstring.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let start_lang = AnyDataValue::new_integer_from_i64(4);
        let result_lang =
            AnyDataValue::new_language_tagged_string("bar".to_string(), "en".to_string());
        let actual_result_lang = StringSubstring.evaluate(string_lang, start_lang);
        assert!(actual_result_lang.is_some());
        assert_eq!(actual_result_lang.unwrap(), result_lang);

        let string_lang =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let start_lang = AnyDataValue::new_integer_from_u64(8);
        let result_lang =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let actual_result_lang = StringSubstring.evaluate(string_lang, start_lang);
        assert!(actual_result_lang.is_some());
        assert_eq!(actual_result_lang.unwrap(), result_lang);
    }

    #[test]
    fn test_string_substring_length() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let empty_string = AnyDataValue::new_plain_string("".to_string());
        let empty_string_en =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());

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
        assert!(actual_result4.is_some());
        assert_eq!(actual_result4.unwrap(), empty_string);

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
        let length7 = AnyDataValue::new_integer_from_u64(3);
        let result7 = AnyDataValue::new_plain_string("ab".to_string());
        let actual_result7 = StringSubstringLength.evaluate(string.clone(), start7, length7);
        assert!(actual_result7.is_some());
        assert_eq!(result7, actual_result7.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start8 = AnyDataValue::new_integer_from_u64(3);
        let length8 = AnyDataValue::new_integer_from_u64(2);
        let result8 = AnyDataValue::new_plain_string("ẅk".to_string());
        let actual_result8 =
            StringSubstringLength.evaluate(string_unicode.clone(), start8, length8);
        assert!(actual_result8.is_some());
        assert_eq!(result8, actual_result8.unwrap());

        let string_clip = AnyDataValue::new_plain_string("12345".to_string());
        let start9 = AnyDataValue::new_integer_from_u64(5);
        let length9 = AnyDataValue::new_integer_from_i64(-3);
        let actual_result9 = StringSubstringLength.evaluate(string_clip.clone(), start9, length9);
        assert!(actual_result9.is_some());
        assert_eq!(actual_result9.unwrap(), empty_string);

        let start10 = AnyDataValue::new_integer_from_i64(-3);
        let length10 = AnyDataValue::new_integer_from_u64(5);
        let result10 = AnyDataValue::new_plain_string("1".to_string());
        let actual_result10 =
            StringSubstringLength.evaluate(string_clip.clone(), start10, length10);
        assert!(actual_result10.is_some());
        assert_eq!(result10, actual_result10.unwrap());

        let start11 = AnyDataValue::new_integer_from_u64(0);
        let length11 = AnyDataValue::new_integer_from_u64(3);
        let result11 = AnyDataValue::new_plain_string("12".to_string());
        let actual_result11 =
            StringSubstringLength.evaluate(string_clip.clone(), start11, length11);
        assert!(actual_result11.is_some());
        assert_eq!(result11, actual_result11.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_integer_from_u64(3);
        let length_notstring = AnyDataValue::new_integer_from_u64(2);
        let actual_result_notstring =
            StringSubstringLength.evaluate(string_notstring, start_notstring, length_notstring);
        assert!(actual_result_notstring.is_none());

        let foobar = AnyDataValue::new_plain_string("foobar".to_string());
        let foobar_en =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());

        let start_lang = AnyDataValue::new_integer_from_i64(4);
        let end_lang = AnyDataValue::new_integer_from_i64(1);
        let result_lang =
            AnyDataValue::new_language_tagged_string("b".to_string(), "en".to_string());
        let actual_result_lang =
            StringSubstringLength.evaluate(foobar_en.clone(), start_lang, end_lang);
        assert!(actual_result_lang.is_some());
        assert_eq!(actual_result_lang.unwrap(), result_lang);

        let start_empty = AnyDataValue::new_integer_from_i64(0);
        let end_empty = AnyDataValue::new_integer_from_i64(-1);
        let actual_result_lang_empty = StringSubstringLength.evaluate(
            foobar_en.clone(),
            start_empty.clone(),
            end_empty.clone(),
        );
        assert!(actual_result_lang_empty.is_some());
        assert_eq!(actual_result_lang_empty.unwrap(), empty_string_en);

        let actual_result_lang_empty =
            StringSubstringLength.evaluate(foobar.clone(), start_empty.clone(), end_empty.clone());
        assert!(actual_result_lang_empty.is_some());
        assert_eq!(actual_result_lang_empty.unwrap(), empty_string);

        let actual_result_empty = StringSubstringLength.evaluate(
            empty_string.clone(),
            start_empty.clone(),
            end_empty.clone(),
        );
        assert!(actual_result_empty.is_some());
        assert_eq!(actual_result_empty.unwrap(), empty_string);

        let actual_result_empty_en = StringSubstringLength.evaluate(
            empty_string_en.clone(),
            start_empty.clone(),
            end_empty.clone(),
        );
        assert!(actual_result_empty_en.is_some());
        assert_eq!(actual_result_empty_en.unwrap(), empty_string_en);
    }

    #[test]
    fn test_string_starts() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let start = AnyDataValue::new_plain_string("a".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = StringStarts.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("loẅ".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode = StringStarts.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("loẅ".to_string());
        let actual_result_notstring = StringStarts.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "es".to_string());
        let start_lang1 =
            AnyDataValue::new_language_tagged_string("foo".to_string(), "es".to_string());
        let result_lang = AnyDataValue::new_boolean(true);
        let actual_result_lang1 = StringStarts.evaluate(string_lang.clone(), start_lang1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let start_lang2 = AnyDataValue::new_plain_string("foo".to_string());
        let actual_result_lang2 = StringStarts.evaluate(string_lang, start_lang2);
        assert!(actual_result_lang2.is_some());
        assert_eq!(result_lang, actual_result_lang2.unwrap());
    }

    #[test]
    fn test_string_ends() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let start = AnyDataValue::new_plain_string("c".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = StringEnds.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅks".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode = StringEnds.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅks".to_string());
        let actual_result_notstring = StringEnds.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "es".to_string());
        let start_lang1 =
            AnyDataValue::new_language_tagged_string("bar".to_string(), "es".to_string());
        let result_lang = AnyDataValue::new_boolean(true);
        let actual_result_lang1 = StringEnds.evaluate(string_lang.clone(), start_lang1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let start_lang2 = AnyDataValue::new_plain_string("bar".to_string());
        let actual_result_lang2 = StringEnds.evaluate(string_lang, start_lang2);
        assert!(actual_result_lang2.is_some());
        assert_eq!(result_lang, actual_result_lang2.unwrap());
    }

    #[test]
    fn test_string_reverse() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let result = AnyDataValue::new_plain_string("olleh".to_string());
        let actual_result = StringReverse.evaluate(string.clone());
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let result_unicode = AnyDataValue::new_plain_string("skẅol".to_string());
        let actual_result_unicode = StringReverse.evaluate(string_unicode.clone());
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let actual_result_notstring = StringReverse.evaluate(string_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang =
            AnyDataValue::new_language_tagged_string("bar".to_string(), "es".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("rab".to_string(), "es".to_string());
        let actual_result_lang = StringReverse.evaluate(string_lang);
        assert!(actual_result_lang.is_some());
        assert_eq!(result_lang, actual_result_lang.unwrap());
    }

    #[test]
    fn test_string_before() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let start = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_plain_string("he".to_string());
        let actual_result = StringBefore.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_plain_string("lo".to_string());
        let actual_result_unicode = StringBefore.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("fçs".to_string());
        let start_unicode = AnyDataValue::new_plain_string("s".to_string());
        let result_unicode = AnyDataValue::new_plain_string("fç".to_string());
        let actual_result_unicode = StringBefore.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅ".to_string());
        let actual_result_notstring = StringBefore.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang1 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_lang1 = AnyDataValue::new_plain_string("bc".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("a".to_string(), "en".to_string());
        let actual_result_lang1 = StringBefore.evaluate(string_lang1, start_lang1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let string_lang2 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_lang2 =
            AnyDataValue::new_language_tagged_string("bc".to_string(), "en".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("a".to_string(), "en".to_string());
        let actual_result_lang1 = StringBefore.evaluate(string_lang2, start_lang2);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let string_empty1 = AnyDataValue::new_plain_string("abc".to_string());
        let start_empty1 = AnyDataValue::new_plain_string("xyz".to_string());
        let result_empty1 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty1 = StringBefore.evaluate(string_empty1, start_empty1);
        assert!(actual_result_empty1.is_some());
        assert_eq!(result_empty1, actual_result_empty1.unwrap());

        let string_empty2 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty2 =
            AnyDataValue::new_language_tagged_string("z".to_string(), "en".to_string());
        let result_empty2 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty2 = StringBefore.evaluate(string_empty2, start_empty2);
        assert!(actual_result_empty2.is_some());
        assert_eq!(result_empty2, actual_result_empty2.unwrap());

        let string_empty3 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty3 = AnyDataValue::new_plain_string("z".to_string());
        let result_empty3 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty3 = StringBefore.evaluate(string_empty3, start_empty3);
        assert!(actual_result_empty3.is_some());
        assert_eq!(result_empty3, actual_result_empty3.unwrap());

        let string_empty4 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty4 =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let result_empty4 =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let actual_result_empty4 = StringBefore.evaluate(string_empty4, start_empty4);
        assert!(actual_result_empty4.is_some());
        assert_eq!(result_empty4, actual_result_empty4.unwrap());

        let string_empty5 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty5 = AnyDataValue::new_plain_string("".to_string());
        let result_empty5 =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let actual_result_empty5 = StringBefore.evaluate(string_empty5, start_empty5);
        assert!(actual_result_empty5.is_some());
        assert_eq!(result_empty5, actual_result_empty5.unwrap());

        let string_error =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_error =
            AnyDataValue::new_language_tagged_string("b".to_string(), "y".to_string());
        let actual_result_error = StringBefore.evaluate(string_error, start_error);
        assert!(actual_result_error.is_none());
    }

    #[test]
    fn test_string_after() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let start = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_plain_string("lo".to_string());
        let actual_result = StringAfter.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_plain_string("ks".to_string());
        let actual_result_unicode = StringAfter.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("fççsa".to_string());
        let start_unicode = AnyDataValue::new_plain_string("s".to_string());
        let result_unicode = AnyDataValue::new_plain_string("a".to_string());
        let actual_result_unicode = StringAfter.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅ".to_string());
        let actual_result_notstring = StringAfter.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());

        let string_lang1 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_lang1 = AnyDataValue::new_plain_string("ab".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("c".to_string(), "en".to_string());
        let actual_result_lang1 = StringAfter.evaluate(string_lang1, start_lang1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let string_lang2 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_lang2 =
            AnyDataValue::new_language_tagged_string("ab".to_string(), "en".to_string());
        let result_lang =
            AnyDataValue::new_language_tagged_string("c".to_string(), "en".to_string());
        let actual_result_lang1 = StringAfter.evaluate(string_lang2, start_lang2);
        assert!(actual_result_lang1.is_some());
        assert_eq!(result_lang, actual_result_lang1.unwrap());

        let string_empty1 = AnyDataValue::new_plain_string("abc".to_string());
        let start_empty1 = AnyDataValue::new_plain_string("xyz".to_string());
        let result_empty1 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty1 = StringAfter.evaluate(string_empty1, start_empty1);
        assert!(actual_result_empty1.is_some());
        assert_eq!(result_empty1, actual_result_empty1.unwrap());

        let string_empty2 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty2 =
            AnyDataValue::new_language_tagged_string("z".to_string(), "en".to_string());
        let result_empty2 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty2 = StringAfter.evaluate(string_empty2, start_empty2);
        assert!(actual_result_empty2.is_some());
        assert_eq!(result_empty2, actual_result_empty2.unwrap());

        let string_empty3 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty3 = AnyDataValue::new_plain_string("z".to_string());
        let result_empty3 = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty3 = StringAfter.evaluate(string_empty3, start_empty3);
        assert!(actual_result_empty3.is_some());
        assert_eq!(result_empty3, actual_result_empty3.unwrap());

        let string_empty4 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty4 =
            AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let result_empty4 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let actual_result_empty4 = StringAfter.evaluate(string_empty4, start_empty4);
        assert!(actual_result_empty4.is_some());
        assert_eq!(result_empty4, actual_result_empty4.unwrap());

        let string_empty5 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_empty5 = AnyDataValue::new_plain_string("".to_string());
        let result_empty5 =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let actual_result_empty5 = StringAfter.evaluate(string_empty5, start_empty5);
        assert!(actual_result_empty5.is_some());
        assert_eq!(result_empty5, actual_result_empty5.unwrap());

        let string_error =
            AnyDataValue::new_language_tagged_string("abc".to_string(), "en".to_string());
        let start_error =
            AnyDataValue::new_language_tagged_string("b".to_string(), "y".to_string());
        let actual_result_error = StringAfter.evaluate(string_error, start_error);
        assert!(actual_result_error.is_none());
    }

    #[test]
    fn test_string_regex() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let pattern = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = StringRegex.evaluate(string.clone(), pattern);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let pattern_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode = StringRegex.evaluate(string_unicode.clone(), pattern_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_regex = AnyDataValue::new_plain_string("looks".to_string());
        let pattern_regex = AnyDataValue::new_plain_string("o+".to_string());
        let result_regex = AnyDataValue::new_boolean(true);
        let actual_result_regex = StringRegex.evaluate(string_regex.clone(), pattern_regex);
        assert!(actual_result_regex.is_some());
        assert_eq!(result_regex, actual_result_regex.unwrap());
    }

    #[test]
    fn test_uri_encode() {
        let string = AnyDataValue::new_plain_string("Los Angeles".to_string());
        let result = AnyDataValue::new_plain_string("Los%20Angeles".to_string());
        let actual_result = StringUriEncode.evaluate(string);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);

        // language tags are ignored
        let tagged_string =
            AnyDataValue::new_language_tagged_string("Los Angeles".to_string(), "en".to_string());
        let actual_result = StringUriEncode.evaluate(tagged_string);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);
    }

    #[test]
    fn test_uri_decode() {
        let string = AnyDataValue::new_plain_string("Los%20Angeles".to_string());
        let result = AnyDataValue::new_plain_string("Los Angeles".to_string());
        let actual_result = StringUriDecode.evaluate(string);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);

        // language tags for encoded strings are ignored
        let tagged_string =
            AnyDataValue::new_language_tagged_string("Los%20Angeles".to_string(), "en".to_string());
        let actual_result = StringUriDecode.evaluate(tagged_string);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);
    }

    #[test]
    fn test_levenshtein() {
        let string1 = AnyDataValue::new_plain_string("foobar".to_string());
        let string2 = AnyDataValue::new_plain_string("bar".to_string());
        let result = AnyDataValue::new_integer_from_i64(3);
        let actual_result = StringLevenshtein.evaluate(string1, string2);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);

        let string1_lang =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "es".to_string());
        let string2_lang =
            AnyDataValue::new_language_tagged_string("bar".to_string(), "es".to_string());
        let result_lang = AnyDataValue::new_integer_from_i64(3);
        let actual_result_lang = StringLevenshtein.evaluate(string1_lang, string2_lang);
        assert!(actual_result_lang.is_some());
        assert_eq!(actual_result_lang.unwrap(), result_lang);
    }

    #[test]
    fn test_string_compare() {
        let lang_base1 =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "en".to_string());
        let lang_cmp1 =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "en".to_string());
        let result_lang1 = AnyDataValue::new_integer_from_i64(0);
        let actual_result_lang1 = StringCompare.evaluate(lang_base1, lang_cmp1);
        assert!(actual_result_lang1.is_some());
        assert_eq!(actual_result_lang1.unwrap(), result_lang1);

        let lang_base2 =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "en".to_string());
        let lang_cmp2 =
            AnyDataValue::new_language_tagged_string("asde".to_string(), "en".to_string());
        let result_lang2 = AnyDataValue::new_integer_from_i64(-1);
        let actual_result_lang2 = StringCompare.evaluate(lang_base2, lang_cmp2);
        assert!(actual_result_lang2.is_some());
        assert_eq!(actual_result_lang2.unwrap(), result_lang2);

        let lang_base2 =
            AnyDataValue::new_language_tagged_string("asde".to_string(), "en".to_string());
        let lang_cmp2 =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "en".to_string());
        let result_lang2 = AnyDataValue::new_integer_from_i64(1);
        let actual_result_lang2 = StringCompare.evaluate(lang_base2, lang_cmp2);
        assert!(actual_result_lang2.is_some());
        assert_eq!(actual_result_lang2.unwrap(), result_lang2);

        let error_base =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "en".to_string());
        let error_cmp =
            AnyDataValue::new_language_tagged_string("asd".to_string(), "gr".to_string());
        let actual_result_error = StringCompare.evaluate(error_base, error_cmp);
        assert!(actual_result_error.is_none());
    }

    #[test]
    fn test_concat() {
        let foo = AnyDataValue::new_plain_string("foo".to_string());
        let b = AnyDataValue::new_plain_string("b".to_string());
        let a = AnyDataValue::new_plain_string("a".to_string());
        let r = AnyDataValue::new_plain_string("r".to_string());
        let result = AnyDataValue::new_plain_string("foobar".to_string());
        let actual_result = StringConcatenation.evaluate(&[foo, b, a, r]);
        assert!(actual_result.is_some());
        assert_eq!(actual_result.unwrap(), result);

        let foo_en = AnyDataValue::new_language_tagged_string("foo".to_string(), "en".to_string());
        let bar_en = AnyDataValue::new_language_tagged_string("bar".to_string(), "en".to_string());
        let result_en_en =
            AnyDataValue::new_language_tagged_string("foobar".to_string(), "en".to_string());
        let actual_result_en_en = StringConcatenation.evaluate(&[foo_en.clone(), bar_en]);
        assert!(actual_result_en_en.is_some());
        assert_eq!(actual_result_en_en.unwrap(), result_en_en);

        let bar_gr = AnyDataValue::new_language_tagged_string("bar".to_string(), "gr".to_string());
        let result_no_lang = AnyDataValue::new_plain_string("foobar".to_string());
        let actual_result_no_lang = StringConcatenation.evaluate(&[foo_en.clone(), bar_gr]);
        assert!(actual_result_no_lang.is_some());
        assert_eq!(actual_result_no_lang.unwrap(), result_no_lang);

        let bar = AnyDataValue::new_plain_string("bar".to_string());
        let actual_result_no_lang = StringConcatenation.evaluate(&[foo_en.clone(), bar]);
        assert!(actual_result_no_lang.is_some());
        assert_eq!(actual_result_no_lang.unwrap(), result_no_lang);

        let actual_result_single_val = StringConcatenation.evaluate(&[foo_en.clone()]);
        assert!(actual_result_single_val.is_some());
        assert_eq!(actual_result_single_val.unwrap(), foo_en.clone());

        let empty_en = AnyDataValue::new_language_tagged_string("".to_string(), "en".to_string());
        let actual_result_empty1 = StringConcatenation.evaluate(&[empty_en.clone()]);
        assert!(actual_result_empty1.is_some());
        assert_eq!(actual_result_empty1.unwrap(), empty_en);

        let empty = AnyDataValue::new_plain_string("".to_string());
        let actual_result_empty2 = StringConcatenation.evaluate(&[empty.clone(), empty_en.clone()]);
        assert!(actual_result_empty2.is_some());
        assert_eq!(actual_result_empty2.unwrap(), empty);
    }
}
