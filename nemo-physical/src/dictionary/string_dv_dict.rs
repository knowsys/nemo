//! A [`DvDict`] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{AddResult, DvDict, StringDictionary};
use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use crate::dictionary::StringPairBasedDvDictionary;
use std::{fmt::Debug, marker::PhantomData};

/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::String`] values.
pub(crate) type StringDvDictionary = StringBasedDvDictionary<StringDvConverter>;
/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::Iri`] values.
pub(crate) type IriDvDictionary = StringBasedDvDictionary<IriDvConverter>;
/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::Other`] values.
pub(crate) type OtherDvDictionary = StringPairBasedDvDictionary<OtherDvConverter>;
/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::LanguageTaggedString`] values.
pub(crate) type LangStringDvDictionary = StringPairBasedDvDictionary<LangStringDvConverter>;

/// Trait to encapsulate (static) functions for converting datavalues to strings
/// and vice versa. The mapping must therefore be invertible, but otherwise it
/// can be arbitrary. Implementations may choose which datavalues to support.
pub(crate) trait DvConverter: Debug {
    /// Converts a datavalue to a string, if supported.
    fn dict_string(dv: &AnyDataValue) -> Option<String>;
    /// Converts a datavalue to a string pair, when supported.
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]>;
    /// Converts a string to a datavalue, if supported.
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue>;
    /// Converts a string pair to a datavalue, if supported.
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue>;
    /// Each converter supports exactly one domain, returned by this function.
    fn supported_value_domain() -> ValueDomain;
}

/// Implementation of [`DvConverter`] to handle [`AnyDataValue::String`] values.
#[derive(Debug)]
pub(crate) struct StringDvConverter;
impl DvConverter for StringDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        dv.to_plain_string()
    }

    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        None
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(string.to_string()))
    }

    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        None
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::PlainString
    }
}

/// Implementation of [`DvConverter`] to handle [`AnyDataValue::Iri`] values.
#[derive(Debug)]
pub(crate) struct IriDvConverter;
impl DvConverter for IriDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        dv.to_iri()
    }

    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        None
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_iri(string.to_string()))
    }

    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        None
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Iri
    }
}

/// TODO remove this function
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

/// TODO remove this function
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

/// Implementation of [`DvConverter`] to handle [`AnyDataValue::Other`] values.
///
/// FIXME: This currently also handles booleans, but our API is not designed for having several
/// value domains in one dictionary, so the supported_value_domain() is just Other. Should not
/// hurt much once we do not have "isOther" as a check in DV dicts, but is still not a clean solution.
#[derive(Debug)]
pub(crate) struct OtherDvConverter;
impl DvConverter for OtherDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        if dv.value_domain() == ValueDomain::Other || dv.value_domain() == ValueDomain::Boolean {
            Some(two_strings_to_one(
                dv.lexical_value().as_str(),
                dv.datatype_iri().as_str(),
            ))
        } else {
            None
        }
    }

    #[inline(always)]
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        Some([dv.lexical_value(), dv.datatype_iri()])
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        /* one_string_to_two(string).map(|(lexical_value, datatype_iri)| {
            if datatype_iri.as_str() == "http://www.w3.org/2001/XMLSchema#boolean" {
                AnyDataValue::new_boolean(lexical_value == "true")
            } else {
                AnyDataValue::new_other(lexical_value, datatype_iri)
            }
        }) */
        None
    }

    #[inline(always)]
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        if second == "http://www.w3.org/2001/XMLSchema#boolean" {
            Some(AnyDataValue::new_boolean(first == "true"))
        } else {
            Some(AnyDataValue::new_other(
                first.to_string(),
                second.to_string(),
            ))
        }
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Other
    }
}

/// Implementation of [`DvConverter`] to handle [`AnyDataValue::LangStringDataValue`] values.
#[derive(Debug)]
pub(crate) struct LangStringDvConverter;
impl DvConverter for LangStringDvConverter {
    #[inline(always)]
    fn dict_string(dv: &AnyDataValue) -> Option<String> {
        None
        /* if dv.value_domain() == ValueDomain::LanguageTaggedString {
            let (string, lang) = dv.to_language_tagged_string_unchecked();
            Some(two_strings_to_one(string.as_str(), lang.as_str()))
        } else {
            None
        } */
    }

    #[inline(always)]
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        let (string, lang) = dv.to_language_tagged_string_unchecked();
        Some([string, lang])
    }

    #[inline(always)]
    fn string_to_datavalue(string: &str) -> Option<AnyDataValue> {
        /* one_string_to_two(string)
        .map(|(value, language)| AnyDataValue::new_language_tagged_string(value, language)) */
        None
    }

    #[inline(always)]
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_language_tagged_string(
            first.to_string(),
            second.to_string(),
        ))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }
}

/// A generic [`DvDict`] dictionary based on converting datavalues to strings. The
/// type parameter defines how the conversion is to be done, making sure that we have
/// compile-time knowledge about this.
#[derive(Debug)]
pub(crate) struct StringBasedDvDictionary<C: DvConverter> {
    string_dict: StringDictionary,
    _phantom: PhantomData<C>,
}

impl<C: DvConverter> StringBasedDvDictionary<C> {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<C: DvConverter> Default for StringBasedDvDictionary<C> {
    fn default() -> Self {
        StringBasedDvDictionary {
            string_dict: StringDictionary::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C: DvConverter> DvDict for StringBasedDvDictionary<C> {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some(s) = C::dict_string(&dv) {
            self.string_dict.add_str(s.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        panic!("string-based dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        panic!("string-based dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        if let Some(s) = C::dict_string(dv) {
            self.string_dict.str_to_id(s.as_str())
        } else {
            None
        }
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        if let Some(string) = self.string_dict.id_to_string(id) {
            C::string_to_datavalue(string.as_str())
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.string_dict.len()
    }

    fn is_iri(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::Iri && self.string_dict.knows_id(id)
    }

    fn is_plain_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::PlainString && self.string_dict.knows_id(id)
    }

    fn is_lang_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::LanguageTaggedString
            && self.string_dict.knows_id(id)
    }

    fn is_null(&self, _id: usize) -> bool {
        false
    }

    fn mark_dv(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some(s) = C::dict_string(&dv) {
            self.string_dict.mark_str(s.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn has_marked(&self) -> bool {
        self.string_dict.has_marked()
    }
}

#[cfg(test)]
mod test {

    use crate::{
        datavalues::AnyDataValue,
        dictionary::{AddResult, DvDict, KNOWN_ID_MARK},
    };

    use super::{IriDvDictionary, LangStringDvDictionary, OtherDvDictionary, StringDvDictionary};

    #[test]
    fn string_dict_add_and_mark() {
        let mut dict = StringDvDictionary::new();

        let dv1 = AnyDataValue::new_plain_string("http://example.org".to_string());
        let dv2 = AnyDataValue::new_plain_string("another string".to_string());
        let dv_wrongtype = AnyDataValue::new_iri("http://example.org".to_string());

        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(
            dict.add_datavalue(dv_wrongtype.clone()),
            AddResult::Rejected
        );

        assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

        assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
        assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
        assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

        assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
        assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn iri_dict_add_and_mark() {
        let mut dict = IriDvDictionary::new();

        let dv1 = AnyDataValue::new_iri("http://example.org".to_string());
        let dv2 = AnyDataValue::new_iri("http://example.org/another".to_string());
        let dv_wrongtype = AnyDataValue::new_plain_string("http://example.org".to_string());

        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(
            dict.add_datavalue(dv_wrongtype.clone()),
            AddResult::Rejected
        );

        assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

        assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
        assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
        assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

        assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
        assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn other_dict_add_and_mark() {
        let mut dict = OtherDvDictionary::new();

        fn long_string() -> String {
            let length = 500;
            let mut result = String::with_capacity(length);
            for _i in 0..length {
                result.push('X');
            }
            result
        }

        let dv1 = AnyDataValue::new_other(
            "abc".to_string(),
            "http://example.org/mydatatype".to_string(),
        );
        let dv2 = AnyDataValue::new_other(
            "".to_string(),
            "http://example.org/str<a>n\\gemydatatype".to_string(),
        );
        let dv3 = AnyDataValue::new_other(
            "string witt < and > and \\".to_string(),
            "http://example.org/verylong/".to_string() + long_string().as_str(),
        );
        let dv_wrongtype = AnyDataValue::new_plain_string("http://example.org".to_string());

        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(
            dict.add_datavalue(dv_wrongtype.clone()),
            AddResult::Rejected
        );
        assert_eq!(dict.add_datavalue(dv3.clone()), AddResult::Fresh(1));

        assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

        assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
        assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
        assert_eq!(dict.datavalue_to_id(&dv3), Some(1));
        assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

        assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
        assert_eq!(dict.id_to_datavalue(1), Some(dv3.clone()));
        assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn langstring_dict_add_and_mark() {
        let mut dict = LangStringDvDictionary::new();

        fn long_string() -> String {
            let length = 500;
            let mut result = String::with_capacity(length);
            for _i in 0..length {
                result.push('X');
            }
            result
        }

        let dv1 = AnyDataValue::new_language_tagged_string("Hallo".to_string(), "de".to_string());
        let dv2 =
            AnyDataValue::new_language_tagged_string("hello".to_string(), "en-GB".to_string());
        let dv3 = AnyDataValue::new_language_tagged_string(
            "string witt < and > and \\".to_string(),
            "verylonglanguage-".to_string() + long_string().as_str(),
        );
        let dv_wrongtype = AnyDataValue::new_plain_string("http://example.org".to_string());

        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(
            dict.add_datavalue(dv_wrongtype.clone()),
            AddResult::Rejected
        );
        assert_eq!(dict.add_datavalue(dv3.clone()), AddResult::Fresh(1));

        assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

        assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
        assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
        assert_eq!(dict.datavalue_to_id(&dv3), Some(1));
        assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

        assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
        assert_eq!(dict.id_to_datavalue(1), Some(dv3.clone()));
        assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
        assert_eq!(dict.len(), 2);
    }
}
