//! A [DvDict] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{AddResult, DvDict, StringDictionary};
use crate::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    management::bytesized::ByteSized,
};
use std::{fmt::Debug, marker::PhantomData};

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

/// Combine two strings into one in an invertible way.
/// If either of the strings is typically shorter than 127 bytes, it
/// should be given as `string2` to enable more efficient coding.
#[inline(always)]
pub(crate) fn two_strings_to_one(string1: &str, string2: &str) -> String {
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
pub(crate) fn one_string_to_two(string: &str) -> Option<(String, String)> {
    if string.is_empty() {
        return None;
    }
    let string1: String;
    let string2: String;
    let marker = string.as_bytes()[0] as usize;
    if marker > 0 {
        let iri_string = string.get(1..marker + 1)?;
        string2 = iri_string.into();
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

/// Implementation of [DvDict] that will only handle [ValueDomain::PlainString] values.
pub(crate) type StringDvDictionary = StringBasedDvDictionary<StringDvConverter>;

/// A generic [DvDict] dictionary based on converting datavalues to strings. The
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

impl<C: DvConverter> ByteSized for StringBasedDvDictionary<C> {
    fn size_bytes(&self) -> u64 {
        let size = self.string_dict.size_bytes();
        log::debug!(
            "StringBasedDvDictionary for {:?} with {} entries: {} bytes",
            C::supported_value_domain(),
            self.len(),
            size
        );
        size
    }
}

#[cfg(test)]
mod test {

    use crate::{
        datavalues::AnyDataValue,
        dictionary::{AddResult, DvDict, KNOWN_ID_MARK},
    };

    use crate::dictionary::string_dv_dict::StringDvDictionary;

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
}
