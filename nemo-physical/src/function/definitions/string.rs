//! This module defines functions on string.

use levenshtein::levenshtein;
use once_cell::sync::OnceCell;
use std::{cmp::Ordering, collections::HashSet, num::NonZero, sync::Mutex};
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
struct LangString {
    string: String,
    tag : Option<String>
}

impl LangString {
    fn new(string: String, tag: Option<String>) -> Self{
        Self {string , tag}
    }

    fn as_string_data_value(self) -> AnyDataValue {
        match self.tag {
            Some(tag) => AnyDataValue::new_language_tagged_string(self.string, tag),
            None => AnyDataValue::new_plain_string(self.string) 
        }
    }

}

impl TryFrom<AnyDataValue> for LangString {
    type Error = () ;

    fn try_from(parameter: AnyDataValue) -> Result<Self, ()>{
        match parameter.value_domain() {
            ValueDomain::PlainString => {
                Ok(Self::new( parameter.to_plain_string_unchecked(),None))
            } 
            ValueDomain::LanguageTaggedString => {
                let (string, lang_tag) = parameter.to_language_tagged_string_unchecked();
                Ok(Self::new( string, Some(lang_tag)))
            }
            _ => Err(()),
        }
    }
}




fn lang_string_from_any(
    parameter: AnyDataValue,
) -> Option<LangString> {
    Some(
        parameter.try_into().ok()?
    )
}

/// Given two [AnyDataValue]s,
/// check if both are plain strings or language tagged strings and return a pair of [LangString]
/// if this is the case and 
///
/// Returns `None` otherwise.
fn lang_string_pair_from_any(
    parameter_first: AnyDataValue,
    parameter_second: AnyDataValue,
) -> Option<(LangString, LangString)> {
        let lang_string_first = lang_string_from_any(parameter_first)?;
        let lang_string_second = lang_string_from_any(parameter_second)?;

        // Implement the Argument Compatibility Rules for language tags from https://www.w3.org/TR/sparql11-query/#func-arg-compatibility
        match (&lang_string_first.tag, &lang_string_second.tag) {
            (None, None) |
            (Some(_), None) => Some((lang_string_first, lang_string_second)),
            (Some(tag_first), Some(tag_second)) if tag_first == tag_second => Some((lang_string_first, lang_string_second)),            
            _ => None,
        }
    }

/// Given a list of [AnyDataValue]s,
/// check if all of them are strings and return a list of [String]
/// if this is the case.
///
/// Returns `None` otherwise.
fn string_vec_from_any(parameters: &[AnyDataValue]) -> Option<Vec<LangString>> {
    let mut result = Vec::new();

    for parameter in parameters {
        result.push(lang_string_from_any(parameter.clone())?);
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| match first_lang_string.string.cmp(&second_lang_string.string) {
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
        let lang_strings = string_vec_from_any(parameters)?;
        let lang_tags: HashSet<String> = lang_strings.iter().filter_map(|lang_string| lang_string.tag.as_ref()).cloned().collect();
        let result = lang_strings.iter().map(|ls| ls.string.as_str()).collect::<String>();
        
        let lang_tag = if lang_tags.len() == 1 {
            lang_tags.into_iter().next()
        } else {
            None
        };
        Some(LangString::new(result, lang_tag).as_string_data_value())
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                AnyDataValue::new_boolean(unicode_find(&first_lang_string.string, &second_lang_string.string).is_some())
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
                let first_graphemes = first_lang_string.string.graphemes(true).collect::<Vec<&str>>();
                let second_graphemes = second_lang_string.string.graphemes(true).collect::<Vec<&str>>();
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
                let first_graphemes = first_lang_string.string.graphemes(true).collect::<Vec<&str>>();
                let second_graphemes = second_lang_string.string.graphemes(true).collect::<Vec<&str>>();
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
                let result = if let Some(i) = unicode_find(&first_lang_string.string, &second_lang_string.string) {
                    first_lang_string.string.graphemes(true).collect::<Vec<&str>>()[..i].join("")
                } else {
                    "".to_string()
                };

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
        lang_string_pair_from_any(parameter_first, parameter_second).map(
            |(first_lang_string, second_lang_string)| {
                let result = if let Some(i) = unicode_find(&first_lang_string.string, &second_lang_string.string) {
                    first_lang_string.string.graphemes(true).collect::<Vec<&str>>()
                        [i + second_lang_string.string.graphemes(true).collect::<Vec<&str>>().len()..]
                        .join("")
                } else {
                    "".to_string()
                };

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
        let lang_string = lang_string_from_any(parameter_first)?;
        let start = usize::try_from(parameter_second.to_i64().map(|val| val.max(1))?).ok()?;

        let graphemes = lang_string.string.graphemes(true).collect::<Vec<&str>>();

        if start > graphemes.len() {
            return None;
        }

        Some(LangString::new(graphemes[(start - 1)..].join(""), lang_string.tag).as_string_data_value())
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
        lang_string_pair_from_any(parameter_first, parameter_second).map(|(lang_string, lang_pattern)| {
            let mut cache = REGEX_CACHE
                .get_or_init(|| Mutex::new(lru::LruCache::new(REGEX_CACHE_SIZE)))
                .lock()
                .unwrap();

            let regex = cache.try_get_or_insert(lang_pattern.string.clone(), || regex::Regex::new(&lang_pattern.string));

            match regex {
                Ok(regex) => AnyDataValue::new_boolean(regex.is_match(&lang_string.string)),
                Err(_) => AnyDataValue::new_boolean(false),
            }
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

/// Levenshtein distance between two strings
///
/// Returns the Levenshtein distance (i.e., the minimal number of
/// insertions, deletions, or character substitutions required to
/// change one argument into the other) between the two given strings
/// as a number from the integer value space.
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
        lang_string_pair_from_any(parameter_first, parameter_second)
            .map(|(from, to)| AnyDataValue::new_integer_from_u64(levenshtein(&from.string, &to.string) as u64))
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
        lang_string_from_any(parameter)
            .map(|lang_string : LangString| 
        AnyDataValue::new_integer_from_u64(lang_string.string.graphemes(true).count() as u64))
    
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
        lang_string_from_any(parameter).map(|lang_string| {
            let reverse = lang_string.string.graphemes(true).rev().collect::<String>();
            LangString::new(reverse, lang_string.tag).as_string_data_value()
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
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUppercase;
impl UnaryFunction for StringUppercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        lang_string_from_any(parameter).map(|lang_string| {
            let ucase = lang_string.string.to_uppercase();
            LangString::new(ucase, lang_string.tag).as_string_data_value()
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
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringLowercase;
impl UnaryFunction for StringLowercase {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        lang_string_from_any(parameter)
            .map(|lang_string| {
                let lcase = lang_string.string.to_lowercase();
                LangString::new(lcase, lang_string.tag).as_string_data_value()
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
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUriEncode;
impl UnaryFunction for StringUriEncode {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        lang_string_from_any(parameter)
        .map(|lang_string| {
            let uri_encode = urlencoding::encode(&lang_string.string).to_string();
            LangString::new(uri_encode, None).as_string_data_value()
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
///
/// Returns `None` if the provided argument is not a string.
#[derive(Debug, Copy, Clone)]
pub struct StringUriDecode;
impl UnaryFunction for StringUriDecode {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        let lang_string = lang_string_from_any(parameter)?;
        let uri_decode = urlencoding::decode(&lang_string.string).ok()?.to_string();
        Some(LangString::new(uri_decode, None).as_string_data_value())
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
        let lang_string = lang_string_from_any(parameter_first)?;
        let graphemes = lang_string.string.graphemes(true).collect::<Vec<&str>>();
        let start = parameter_second.to_i64()?;
        let length = parameter_third.to_i64()?;


        if length < 1 {
            return None;
        }

        let end = usize::try_from(start + length).ok()?;
        let start = usize::try_from(start.max(1)).ok()?;

        if start > graphemes.len() {
            return None;
        }

        let result = if end > graphemes.len() {
            graphemes[(start - 1)..].join("")
        } else {
            graphemes[(start - 1)..(end - 1)].join("")
        };

        Some(LangString::new(result, lang_string.tag).as_string_data_value())
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
        function::definitions::{
            string::{StringContains, StringLowercase, StringUppercase},
            BinaryFunction, TernaryFunction, UnaryFunction,
        },
    };

    use super::{StringLength, StringReverse, StringSubstring, StringSubstringLength};

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
    }

    #[test]
    fn test_uppercase() {
        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let result_unicode = AnyDataValue::new_plain_string("LOẄKS".to_string());
        let actual_result_unicode = StringUppercase.evaluate(string_unicode.clone());
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());
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
    }

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
        assert!(actual_result9.is_none());

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
    }

    #[test]
    fn test_string_starts() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let start = AnyDataValue::new_plain_string("a".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = super::StringStarts.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("loẅ".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode =
            super::StringStarts.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("loẅ".to_string());
        let actual_result_notstring =
            super::StringStarts.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());
    }

    #[test]
    fn test_string_ends() {
        let string = AnyDataValue::new_plain_string("abc".to_string());
        let start = AnyDataValue::new_plain_string("c".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = super::StringEnds.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅks".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode =
            super::StringEnds.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅks".to_string());
        let actual_result_notstring = super::StringEnds.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());
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
    }

    #[test]
    fn test_string_before() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let start = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_plain_string("he".to_string());
        let actual_result = super::StringBefore.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_plain_string("lo".to_string());
        let actual_result_unicode =
            super::StringBefore.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("fçs".to_string());
        let start_unicode = AnyDataValue::new_plain_string("s".to_string());
        let result_unicode = AnyDataValue::new_plain_string("fç".to_string());
        let actual_result_unicode =
            super::StringBefore.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅ".to_string());
        let actual_result_notstring =
            super::StringBefore.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());
    }

    #[test]
    fn test_string_after() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let start = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_plain_string("lo".to_string());
        let actual_result = super::StringAfter.evaluate(string.clone(), start);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let start_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_plain_string("ks".to_string());
        let actual_result_unicode =
            super::StringAfter.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("fççsa".to_string());
        let start_unicode = AnyDataValue::new_plain_string("s".to_string());
        let result_unicode = AnyDataValue::new_plain_string("a".to_string());
        let actual_result_unicode =
            super::StringAfter.evaluate(string_unicode.clone(), start_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_notstring = AnyDataValue::new_integer_from_i64(1);
        let start_notstring = AnyDataValue::new_plain_string("ẅ".to_string());
        let actual_result_notstring =
            super::StringAfter.evaluate(string_notstring, start_notstring);
        assert!(actual_result_notstring.is_none());
    }

    #[test]
    fn test_string_regex() {
        let string = AnyDataValue::new_plain_string("hello".to_string());
        let pattern = AnyDataValue::new_plain_string("l".to_string());
        let result = AnyDataValue::new_boolean(true);
        let actual_result = super::StringRegex.evaluate(string.clone(), pattern);
        assert!(actual_result.is_some());
        assert_eq!(result, actual_result.unwrap());

        let string_unicode = AnyDataValue::new_plain_string("loẅks".to_string());
        let pattern_unicode = AnyDataValue::new_plain_string("ẅ".to_string());
        let result_unicode = AnyDataValue::new_boolean(true);
        let actual_result_unicode =
            super::StringRegex.evaluate(string_unicode.clone(), pattern_unicode);
        assert!(actual_result_unicode.is_some());
        assert_eq!(result_unicode, actual_result_unicode.unwrap());

        let string_regex = AnyDataValue::new_plain_string("looks".to_string());
        let pattern_regex = AnyDataValue::new_plain_string("o+".to_string());
        let result_regex = AnyDataValue::new_boolean(true);
        let actual_result_regex = super::StringRegex.evaluate(string_regex.clone(), pattern_regex);
        assert!(actual_result_regex.is_some());
        assert_eq!(result_regex, actual_result_regex.unwrap());
    }
}
