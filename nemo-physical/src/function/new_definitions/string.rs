//! This module defines all supported functions relating to strings.

use levenshtein::levenshtein;
use once_cell::sync::OnceCell;
use unicode_segmentation::UnicodeSegmentation;

use std::{cmp::Ordering, num::NonZero, sync::Mutex};

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

/// Trait for types for which generic operations are defined
pub(crate) trait OperableString {
    /// Comparison of strings
    ///
    /// Evaluates to -1 from the integer value space if the first string is alphabetically smaller than the second.
    /// Evaluates to 0 from the integer value space if both strings are equal.
    /// Evaluates to 1 from the integer value space if the second string is alphabetically larger than the first.
    #[allow(unused)]
    fn string_compare(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Concatenation of strings
    ///
    /// Returns a string, that results from merging together
    /// all input strings.
    ///
    /// Returns an empty string if no parameters are given.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_concatenation(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Containment of strings
    ///
    /// Returns `true` from the boolean value space if the string provided as the second parameter
    /// is contained in the string provided as the first parameter and `false` otherwise.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_contains(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Levenshtein distance between two strings
    ///
    /// Returns the Levenshtein distance (i.e., the minimal number of
    /// insertions, deletions, or character substitutions required to
    /// change one argument into the other) between the two given strings
    /// as a number from the integer value space.
    ///
    /// Return `None` if the the provided arguments are not both strings.
    #[allow(unused)]
    fn string_levenshtein(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Start of a string
    ///
    /// Returns `true` from the boolean value space if the string provided as the first parameter
    /// starts with the string provided as the second parameter and `false` otherwise.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_starts(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// End of a string
    ///
    /// Returns `true` from the boolean value space if the string provided as the first parameter
    /// ends with the string provided as the second parameter and `false` otherwise.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_ends(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// First part of a string
    ///
    /// Returns the part of the string given in the first parameter which comes before
    /// the string provided as the second parameter.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_before(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Second part of a string
    ///
    /// Returns the part of the string given in the first parameter which comes after
    /// the string provided as the second parameter.
    ///
    /// Returns `None` if either parameter is not a string.
    #[allow(unused)]
    fn string_after(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Substring
    ///
    /// Expects a string value as the first parameter and an integer value as the second.
    ///
    /// Return a string containing the characters from the first parameter,
    /// starting from the position given by the second paramter.
    ///
    /// Returns `None` if the type requirements from above are not met.
    #[allow(unused)]
    fn string_substring(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Regex string matching
    ///
    /// Returns `true` from the boolean value space if the regex provided as the second parameter
    /// is matched in the string provided as the first parameter and `false` otherwise.
    ///
    /// Returns `None` if either parameter is not a string or the second parameter is not
    /// a regular expression.
    #[allow(unused)]
    fn string_regex(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Length of a string
    ///
    /// Returns the length of the given string as a number from the integer value space.
    ///
    /// Returns `None` if the provided argument is not a string.    
    #[allow(unused)]
    fn string_length(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Transformation of a string into its reverse
    ///
    /// Returns the reversed version of the provided string.
    ///
    /// Returns `None` if the provided argument is not a string.
    #[allow(unused)]
    fn string_reverse(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Transformation of a string into upper case
    ///
    /// Returns the upper case version of the provided string.
    ///
    /// Returns `None` if the provided argument is not a string.
    #[allow(unused)]
    fn string_uppercase(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Transformation of a string into lower case
    ///
    /// Returns the lower case version of the provided string.
    ///
    /// Returns `None` if the provided argument is not a string.
    #[allow(unused)]
    fn string_lowercase(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// URI encoding (percent encoding) of a string
    ///
    /// Returns the percent-encoded version of the provided string.
    ///
    /// Returns `None` if the provided argument is not a string.
    #[allow(unused)]
    fn string_uri_encode(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// URI encoding (percent encoding) of a string
    ///
    /// Returns the percent-encoded version of the provided string.
    ///
    /// Returns `None` if the provided argument is not a string.
    #[allow(unused)]
    fn string_uri_decode(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
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
    #[allow(unused)]
    fn string_substring_length(first: Self, second: Self, third: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

// Use default implementation for all storage values
impl OperableString for i64 {}
impl OperableString for Float {}
impl OperableString for Double {}
impl OperableString for StorageValueT {}

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

/// Given two [AnyDataValue]s,
/// check if both are strings and return a pair of [String]
/// if this is the case.
///
/// Returns `None` otherwise.
fn string_pair_from_any(first: AnyDataValue, second: AnyDataValue) -> Option<(String, String)> {
    Some((first.to_plain_string()?, second.to_plain_string()?))
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

/// Number of entries in [REGEX_CACHE]
const REGEX_CACHE_SIZE: NonZero<usize> = NonZero::new(32).unwrap();
/// Cache for storing [regex::Regex] objects
static REGEX_CACHE: OnceCell<Mutex<lru::LruCache<String, regex::Regex>>> = OnceCell::new();

impl OperableString for AnyDataValue {
    fn string_compare(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| match first_string
            .cmp(&second_string)
        {
            Ordering::Less => AnyDataValue::new_integer_from_i64(-1),
            Ordering::Equal => AnyDataValue::new_integer_from_i64(0),
            Ordering::Greater => AnyDataValue::new_integer_from_i64(1),
        })
    }

    fn string_concatenation(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        string_vec_from_any(parameters)
            .map(|strings| AnyDataValue::new_plain_string(strings.concat()))
    }

    fn string_contains(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| {
            AnyDataValue::new_boolean(unicode_find(&first_string, &second_string).is_some())
        })
    }

    fn string_levenshtein(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second)
            .map(|(from, to)| AnyDataValue::new_integer_from_u64(levenshtein(&from, &to) as u64))
    }

    fn string_starts(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| {
            let first_graphemes = first_string.graphemes(true).collect::<Vec<&str>>();
            let second_graphemes = second_string.graphemes(true).collect::<Vec<&str>>();
            if second_graphemes.len() > first_graphemes.len() {
                return AnyDataValue::new_boolean(false);
            }
            AnyDataValue::new_boolean(first_graphemes[..second_graphemes.len()] == second_graphemes)
        })
    }

    fn string_ends(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| {
            let first_graphemes = first_string.graphemes(true).collect::<Vec<&str>>();
            let second_graphemes = second_string.graphemes(true).collect::<Vec<&str>>();
            if second_graphemes.len() > first_graphemes.len() {
                return AnyDataValue::new_boolean(false);
            }
            AnyDataValue::new_boolean(
                first_graphemes[first_graphemes.len() - second_graphemes.len()..]
                    == second_graphemes,
            )
        })
    }

    fn string_before(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| {
            let result = if let Some(i) = unicode_find(&first_string, &second_string) {
                first_string[..i].to_string()
            } else {
                "".to_string()
            };

            AnyDataValue::new_plain_string(result)
        })
    }

    fn string_after(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(first_string, second_string)| {
            let result = if let Some(i) = unicode_find(&first_string, &second_string) {
                first_string[i + second_string.len()..].to_string()
            } else {
                "".to_string()
            };

            AnyDataValue::new_plain_string(result)
        })
    }

    fn string_substring(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let string = first.to_plain_string()?;
        let start = usize::try_from(second.to_u64()?).ok()?;

        let graphemes = string.graphemes(true).collect::<Vec<&str>>();

        if start > graphemes.len() || start < 1 {
            return None;
        }

        Some(AnyDataValue::new_plain_string(
            graphemes[(start - 1)..].join(""),
        ))
    }

    fn string_regex(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        string_pair_from_any(first, second).map(|(string, pattern)| {
            let mut cache = REGEX_CACHE
                .get_or_init(|| Mutex::new(lru::LruCache::new(REGEX_CACHE_SIZE)))
                .lock()
                .unwrap();

            let regex = cache.try_get_or_insert(pattern.clone(), || regex::Regex::new(&pattern));

            match regex {
                Ok(regex) => AnyDataValue::new_boolean(regex.is_match(&string)),
                Err(_) => AnyDataValue::new_boolean(false),
            }
        })
    }

    fn string_length(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_plain_string()
            .map(|string| AnyDataValue::new_integer_from_u64(string.graphemes(true).count() as u64))
    }

    fn string_reverse(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter.to_plain_string().map(|string| {
            AnyDataValue::new_plain_string(string.graphemes(true).rev().collect::<String>())
        })
    }

    fn string_uppercase(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(string.to_uppercase()))
    }

    fn string_lowercase(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(string.to_lowercase()))
    }

    fn string_uri_encode(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_plain_string()
            .map(|string| AnyDataValue::new_plain_string(urlencoding::encode(&string).to_string()))
    }

    fn string_uri_decode(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let string = parameter.to_plain_string()?;
        let decoded = urlencoding::decode(&string).ok()?;

        Some(AnyDataValue::new_plain_string(decoded.to_string()))
    }

    fn string_substring_length(first: Self, second: Self, third: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let string = first.to_plain_string()?;
        let start = usize::try_from(second.to_u64()?).ok()?;

        let graphemes = string.graphemes(true).collect::<Vec<&str>>();

        if start > graphemes.len() || start < 1 {
            return None;
        }

        let length = usize::try_from(third.to_u64()?).ok()?;
        let end = start + length;

        let result = if end > graphemes.len() {
            graphemes[(start - 1)..].join("")
        } else {
            graphemes[(start - 1)..(end - 1)].join("")
        };

        Some(AnyDataValue::new_plain_string(result))
    }
}
