//! This module defines the trait [DvConverter]
//! for converting datavalues to string,
//! and also provides serveral implementations.

use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use std::fmt::Debug;

/// Trait to encapsulate (static) functions for converting datavalues to strings
/// and vice versa. The mapping must therefore be invertible, but otherwise it
/// can be arbitrary. Implementations may choose which datavalues to support.
pub(crate) trait DvConverter: Debug {
    /// Converts a datavalue to a string, if supported.
    fn dict_string(dv: &AnyDataValue) -> Option<String>;
    /// Converts a string to a datavalue, if supported.
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue>;
    /// Each converter supports exactly one domain, returned by this function.
    fn supported_value_domain() -> ValueDomain;
}

/// Implementation of [DvConverter] to handle [ValueDomain::PlainString] values.
#[derive(Debug)]
pub(crate) struct StringDvConverter;
impl DvConverter for StringDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        dv.to_plain_string()
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(string.to_string()))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::PlainString
    }
}

/// Implementation of [DvConverter] to handle [ValueDomain::Iri] values.
#[derive(Debug)]
pub(crate) struct IriDvConverter;
impl DvConverter for IriDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        dv.to_iri()
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_iri(string.to_string()))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Iri
    }
}

/// Combine two strings into one in an invertible way.
/// If either of the strings is typically shorter than 127 bytes, it
/// should be given as `string2` to enable more efficient coding.
#[inline(always)]
fn two_strings_to_one(string1: &str, string2: &str) -> String {
    let mut result: String;
    // In the common case that type IRIs are at most 127 bytes, we encode the pair by storing the length of that IRI
    // as a single char at the start of the string for faster reconstruction.
    // Note: We do not use the full first byte to ensure that the result is a valid UTF-8 string. If we would use
    // byte sequences instead of strings internally, we could go higher here.
    assert!(!string2.is_empty());

    if string2.len() <= 127 {
        result = String::with_capacity(string1.len() + string2.len() + 1);
        let tiny_len = u8::try_from(string2.len()).expect("length is less than 128");
        result.push(tiny_len as char);
        result.push_str(string2);
        result.push_str(string1);
    } else {
        // >127 char type IRIs are unusual; maybe never relevant what we do here ...
        result = String::with_capacity(string1.len() + string2.len() + 2);
        result.push(0 as char);
        // Escape \ as \\ and > as \a (so no more > in type IRI after this)
        result.push_str(string2.replace('\\', "\\\\").replace('>', "\\a").as_str());
        result.push('>'); // separator
        result.push_str(string1);
    }
    result
}

/// Extract two strings from one that uses the format of [two_strings_to_one].
#[inline(always)]
fn one_string_to_two(string: &str) -> Option<(String, String)> {
    if string.is_empty() {
        return None;
    }
    let string1: String;
    let string2: String;
    let marker = string.as_bytes()[0] as usize;
    if marker > 0 {
        if let Some(iri_string) = string.get(1..marker + 1) {
            string2 = iri_string.into();
        } else {
            return None;
        }
        string1 = string
            .get(marker + 1..)
            .expect("must be valid if previous call was")
            .into();
    } else if let Some(pos) = string.find('>') {
        string2 = string
            .get(1..pos)
            .expect("must be valid if previous call was")
            .into();
        string1 = string
            .get(pos + 1..)
            .expect("must be valid if previous call was")
            .into();
    } else {
        return None;
    }

    Some((string1, string2))
}

/// Implementation of [DvConverter] to handle [ValueDomain::Other] values.
///
/// FIXME: This currently also handles booleans, but our API is not designed for having several
/// value domains in one dictionary, so the supported_value_domain() is just Other. Should not
/// hurt much once we do not have "isOther" as a check in DV dicts, but is still not a clean solution.
#[derive(Debug)]
pub(crate) struct OtherDvConverter;
impl DvConverter for OtherDvConverter {
    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        if dv.value_domain() == ValueDomain::Other
            || dv.value_domain() == ValueDomain::Boolean
            || dv.value_domain() == ValueDomain::UnsignedLong
        {
            Some(two_strings_to_one(
                dv.lexical_value().as_str(),
                dv.datatype_iri().as_str(),
            ))
        } else {
            None
        }
    }

    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        one_string_to_two(string).map(|(lexical_value, datatype_iri)| {
            if datatype_iri.as_str() == "http://www.w3.org/2001/XMLSchema#boolean" {
                AnyDataValue::new_boolean(lexical_value == "true")
            } else {
                AnyDataValue::new_other(lexical_value, datatype_iri)
            }
        })
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Other
    }
}

/// Implementation of [DvConverter] to handle [ValueDomain::LanguageTaggedString] values.
#[derive(Debug)]
pub(crate) struct LangStringDvConverter;
impl DvConverter for LangStringDvConverter {
    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        if dv.value_domain() == ValueDomain::LanguageTaggedString {
            let (string, lang) = dv.to_language_tagged_string_unchecked();
            Some(two_strings_to_one(string.as_str(), lang.as_str()))
        } else {
            None
        }
    }

    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        one_string_to_two(string)
            .map(|(value, language)| AnyDataValue::new_language_tagged_string(value, language))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }
}
