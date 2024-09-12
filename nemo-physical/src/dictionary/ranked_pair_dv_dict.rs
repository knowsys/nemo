//! A [DvDict] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{ranked_pair_dictionary::StringPairDictionary, AddResult, DvDict};
use crate::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    management::bytesized::ByteSized,
};
use std::{fmt::Debug, marker::PhantomData};

/// Trait to encapsulate (static) functions for converting datavalues to pairs of
/// strings and vice versa. The mapping must therefore be invertible, but otherwise it
/// can be arbitrary. Implementations may choose which datavalues to support.
pub(crate) trait DvPairConverter: Debug {
    /// Converts a datavalue to a string, if supported.
    fn dict_pair(dv: &AnyDataValue) -> Option<(String, String)>;
    /// Converts a pair of strings to a datavalue, if supported.
    fn pair_to_datavalue(frequent: &str, rare: &str) -> Option<AnyDataValue>;
    /// Each converter supports exactly one domain, returned by this function.
    fn supported_value_domain() -> ValueDomain;
}

/// Implementation of [DvDict] that will only handle [ValueDomain::Other] values.
pub(crate) type OtherSplittingDvDictionary = RankedPairBasedDvDictionary<OtherDvPairConverter>;

/// Implementation of [DvPairConverter] to handle [ValueDomain::Other] values.
///
/// FIXME: This currently also handles booleans, but our API is not designed for having several
/// value domains in one dictionary, so the supported_value_domain() is just Other. Should not
/// hurt much once we do not have "isOther" as a check in DV dicts, but is still not a clean solution.
#[derive(Debug)]
pub(crate) struct OtherDvPairConverter;
impl DvPairConverter for OtherDvPairConverter {
    #[inline(always)]
    fn dict_pair(dv: &AnyDataValue) -> Option<(String, String)> {
        if dv.value_domain() == ValueDomain::Other
            || dv.value_domain() == ValueDomain::Boolean
            || dv.value_domain() == ValueDomain::UnsignedLong
        {
            Some((dv.datatype_iri(), dv.lexical_value()))
        } else {
            None
        }
    }

    #[inline(always)]
    fn pair_to_datavalue(frequent: &str, rare: &str) -> Option<AnyDataValue> {
        if frequent == "http://www.w3.org/2001/XMLSchema#boolean" {
            Some(AnyDataValue::new_boolean(rare == "true"))
        } else {
            Some(AnyDataValue::new_other(
                rare.to_string(),
                frequent.to_string(),
            ))
        }
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Other
    }
}

/// Implementation of [DvDict] that will only handle [ValueDomain::LanguageTaggedString] values.
pub(crate) type LangStringSplittingDvDictionary =
    RankedPairBasedDvDictionary<LangStringDvPairConverter>;
/// Implementation of [DvPairConverter] to handle [ValueDomain::LanguageTaggedString] values.
#[derive(Debug)]
pub(crate) struct LangStringDvPairConverter;
impl DvPairConverter for LangStringDvPairConverter {
    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn dict_pair(dv: &AnyDataValue) -> Option<(String, String)> {
        if dv.value_domain() == ValueDomain::LanguageTaggedString {
            let (string, lang) = dv.to_language_tagged_string_unchecked();
            Some((lang, string))
        } else {
            None
        }
    }

    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn pair_to_datavalue(frequent: &str, rare: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_language_tagged_string(
            rare.to_string(),
            frequent.to_string(),
        ))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }
}

/// Implementation of [DvDict] that will only handle [ValueDomain::Iri] values.
pub(crate) type IriSplittingDvDictionary = RankedPairBasedDvDictionary<IriDvPairConverter>;

/// Implementation of [DvPairConverter] to handle [ValueDomain::Iri] values.
#[derive(Debug)]
pub(crate) struct IriDvPairConverter;
impl IriDvPairConverter {
    /// Finds the last position in UTF-8 str slice (given as `&[u8]` bytes) where the characters '/' or '#' occur,
    /// and returns the successor of that position. If the character is not found, 0 is returned.
    /// The method avoids any UTF decoding, because it is unnecessary for characters in the ASCII range (<128).
    #[inline(always)]
    fn rfind_hashslash_plus(s: &[u8]) -> usize {
        let mut pos: usize = s.len();
        let mut iter = s.iter().copied();
        while let Some(ch) = iter.next_back() {
            if ch == b'/' || ch == b'#' {
                return pos;
            }
            pos -= 1;
        }
        pos
    }
}
impl DvPairConverter for IriDvPairConverter {
    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn dict_pair(dv: &AnyDataValue) -> Option<(String, String)> {
        if dv.value_domain() == ValueDomain::Iri {
            let iri_string = dv.to_iri_unchecked();
            let splitpos = Self::rfind_hashslash_plus(iri_string.as_bytes());
            if splitpos == 0 {
                Some(("".to_owned(), iri_string))
            } else {
                Some((
                    iri_string[0..splitpos].to_owned(),
                    iri_string[splitpos..].to_owned(),
                ))
            }
        } else {
            None
        }
    }

    /// Function to use with StringBasedDvDictionary
    #[inline(always)]
    fn pair_to_datavalue(frequent: &str, rare: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_iri(frequent.to_owned() + rare))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Iri
    }
}

/// A generic [DvDict] dictionary based on converting datavalues to strings. The
/// type parameter defines how the conversion is to be done, making sure that we have
/// compile-time knowledge about this.
#[derive(Debug)]
pub(crate) struct RankedPairBasedDvDictionary<C: DvPairConverter> {
    string_pair_dict: StringPairDictionary,
    _phantom: PhantomData<C>,
}

impl<C: DvPairConverter> RankedPairBasedDvDictionary<C> {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<C: DvPairConverter> Default for RankedPairBasedDvDictionary<C> {
    fn default() -> Self {
        RankedPairBasedDvDictionary {
            string_pair_dict: StringPairDictionary::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C: DvPairConverter> DvDict for RankedPairBasedDvDictionary<C> {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some((frequent, rare)) = C::dict_pair(&dv) {
            self.string_pair_dict
                .add_pair(frequent.as_str(), rare.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        panic!("string-pair dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        panic!("string-pair dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        if let Some((frequent, rare)) = C::dict_pair(&dv) {
            self.string_pair_dict
                .pair_to_id(frequent.as_str(), rare.as_str())
        } else {
            None
        }
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        if let Some((frequent, rare)) = self.string_pair_dict.id_to_pair(id) {
            C::pair_to_datavalue(frequent.as_str(), rare.as_str())
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.string_pair_dict.len()
    }

    fn is_iri(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::Iri && self.string_pair_dict.knows_id(id)
    }

    fn is_plain_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::PlainString
            && self.string_pair_dict.knows_id(id)
    }

    fn is_lang_string(&self, id: usize) -> bool {
        C::supported_value_domain() == ValueDomain::LanguageTaggedString
            && self.string_pair_dict.knows_id(id)
    }

    fn is_null(&self, _id: usize) -> bool {
        false
    }

    fn mark_dv(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some((frequent, rare)) = C::dict_pair(&dv) {
            self.string_pair_dict
                .mark_pair(frequent.as_str(), rare.as_str())
        } else {
            AddResult::Rejected
        }
    }

    fn has_marked(&self) -> bool {
        self.string_pair_dict.has_marked()
    }
}

impl<C: DvPairConverter> ByteSized for RankedPairBasedDvDictionary<C> {
    fn size_bytes(&self) -> u64 {
        // Code for debugging/profiling dictionary
        // let result = self.string_pair_dict.size_bytes();
        // println!(
        //     "Ranked Pair DV Dict for {:?}: size {}",
        //     C::supported_value_domain(),
        //     result
        // );
        // result
        self.string_pair_dict.size_bytes()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datavalues::AnyDataValue,
        dictionary::{
            ranked_pair_dv_dict::{IriSplittingDvDictionary, OtherSplittingDvDictionary},
            AddResult, DvDict, KNOWN_ID_MARK,
        },
    };

    #[test]
    fn iri_dict_add_and_mark() {
        let mut dict = IriSplittingDvDictionary::new();

        let dv1 = AnyDataValue::new_iri("http://example.org".to_string());
        let dv2 = AnyDataValue::new_iri("http://example.org/another".to_string());
        let dv3 = AnyDataValue::new_iri("http://example.org/yet_another".to_string());
        let dv4 = AnyDataValue::new_iri("http://example.org#yet_another".to_string());
        let dv_wrongtype = AnyDataValue::new_plain_string("http://example.org".to_string());

        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(
            dict.add_datavalue(dv_wrongtype.clone()),
            AddResult::Rejected
        );

        assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.add_datavalue(dv3.clone()), AddResult::Fresh(1));
        assert_eq!(dict.add_datavalue(dv4.clone()), AddResult::Fresh(2));
        assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
        assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

        assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
        assert_eq!(dict.datavalue_to_id(&dv3), Some(1));
        assert_eq!(dict.datavalue_to_id(&dv4), Some(2));
        assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
        assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

        assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
        assert_eq!(dict.id_to_datavalue(1), Some(dv3.clone()));
        assert_eq!(dict.id_to_datavalue(2), Some(dv4.clone()));
        assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
        assert_eq!(dict.len(), 3);
    }

    #[test]
    fn other_dict_add_and_mark() {
        let mut dict = OtherSplittingDvDictionary::new();

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

    // #[test]
    // fn langstring_dict_add_and_mark() {
    //     let mut dict = LangStringDvDictionary::new();

    //     fn long_string() -> String {
    //         let length = 500;
    //         let mut result = String::with_capacity(length);
    //         for _i in 0..length {
    //             result.push('X');
    //         }
    //         result
    //     }

    //     let dv1 = AnyDataValue::new_language_tagged_string("Hallo".to_string(), "de".to_string());
    //     let dv2 =
    //         AnyDataValue::new_language_tagged_string("hello".to_string(), "en-GB".to_string());
    //     let dv3 = AnyDataValue::new_language_tagged_string(
    //         "string witt < and > and \\".to_string(),
    //         "verylonglanguage-".to_string() + long_string().as_str(),
    //     );
    //     let dv_wrongtype = AnyDataValue::new_plain_string("http://example.org".to_string());

    //     assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Fresh(0));
    //     assert_eq!(dict.add_datavalue(dv1.clone()), AddResult::Known(0));
    //     assert_eq!(
    //         dict.add_datavalue(dv_wrongtype.clone()),
    //         AddResult::Rejected
    //     );
    //     assert_eq!(dict.add_datavalue(dv3.clone()), AddResult::Fresh(1));

    //     assert_eq!(dict.mark_dv(dv2.clone()), AddResult::Fresh(KNOWN_ID_MARK));
    //     assert_eq!(dict.mark_dv(dv_wrongtype.clone()), AddResult::Rejected);

    //     assert_eq!(dict.datavalue_to_id(&dv1), Some(0));
    //     assert_eq!(dict.datavalue_to_id(&dv_wrongtype), None);
    //     assert_eq!(dict.datavalue_to_id(&dv3), Some(1));
    //     assert_eq!(dict.datavalue_to_id(&dv2), Some(KNOWN_ID_MARK));

    //     assert_eq!(dict.id_to_datavalue(0), Some(dv1.clone()));
    //     assert_eq!(dict.id_to_datavalue(1), Some(dv3.clone()));
    //     assert_eq!(dict.id_to_datavalue(KNOWN_ID_MARK), None);
    //     assert_eq!(dict.len(), 2);
    // }
}
