//! A [DvDict] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{AddResult, DvDict, StringDictionary};
use crate::datavalues::{AnyDataValue, ValueDomain};
use std::{fmt::Debug, marker::PhantomData};

use crate::dictionary::dv_converter::{DvConverter, IriDvConverter, StringDvConverter};
#[cfg(not(feature = "stringpairdictionary"))]
use crate::dictionary::dv_converter::{LangStringDvConverter, OtherDvConverter};

/// Implementation of [DvDict] that will only handle [ValueDomain::PlainString] values.
pub(crate) type StringDvDictionary = StringBasedDvDictionary<StringDvConverter>;
/// Implementation of [DvDict] that will only handle [ValueDomain::Iri] values.
pub(crate) type IriDvDictionary = StringBasedDvDictionary<IriDvConverter>;

/// Implementation of [DvDict] that will only handle [ValueDomain::Other] values.
#[cfg(not(feature = "stringpairdictionary"))]
pub(crate) type OtherDvDictionary = StringBasedDvDictionary<OtherDvConverter>;
/// Implementation of [DvDict] that will only handle [ValueDomain::LanguageTaggedString] values.
#[cfg(not(feature = "stringpairdictionary"))]
pub(crate) type LangStringDvDictionary = StringBasedDvDictionary<LangStringDvConverter>;

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

#[cfg(test)]
mod test {

    use crate::{
        datavalues::AnyDataValue,
        dictionary::{AddResult, DvDict, KNOWN_ID_MARK},
    };

    use crate::dictionary::string_dv_dict::{IriDvDictionary, StringDvDictionary};
    #[cfg(not(feature = "stringpairdictionary"))]
    use crate::dictionary::string_dv_dict::{LangStringDvDictionary, OtherDvDictionary};
    #[cfg(feature = "stringpairdictionary")]
    use crate::dictionary::string_pair_dv_dict::{LangStringDvDictionary, OtherDvDictionary};

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
