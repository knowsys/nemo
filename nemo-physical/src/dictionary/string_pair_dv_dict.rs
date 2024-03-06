//! A [`DvDict`] implementation based on converting data values to strings.
//! The dictionary implementations in this module are typically restricted to certain
//! types of datavalues (e.g., strings or IRIs), which allows them to use more direct
//! string representations without any risk of confusion.

use super::{AddResult, DvDict, StringPairDictionary};
use crate::datavalues::{AnyDataValue, ValueDomain};
use crate::dictionary::pair_dv_converter::{
    LangStringDvConverter, OtherDvConverter, PairDvConverter,
};
use std::{fmt::Debug, marker::PhantomData};

#[cfg(feature = "stringpairdictionary")]
/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::Other`] values.
pub(crate) type OtherDvDictionary = StringPairBasedDvDictionary<OtherDvConverter>;
#[cfg(feature = "stringpairdictionary")]
/// Implementation of [`DvDict`] that will only handle [`AnyDataValue::LanguageTaggedString`] values.
pub(crate) type LangStringDvDictionary = StringPairBasedDvDictionary<LangStringDvConverter>;

/// A generic [`DvDict`] dictionary based on converting datavalues to strings. The
/// type parameter defines how the conversion is to be done, making sure that we have
/// compile-time knowledge about this.
#[derive(Debug)]
pub(crate) struct StringPairBasedDvDictionary<C: PairDvConverter> {
    string_pair_dict: StringPairDictionary,
    _phantom: PhantomData<C>, // do I need this??
}

impl<C: PairDvConverter> StringPairBasedDvDictionary<C> {
    /// Construct a new and empty dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<C: PairDvConverter> Default for StringPairBasedDvDictionary<C> {
    fn default() -> Self {
        StringPairBasedDvDictionary {
            string_pair_dict: StringPairDictionary::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C: PairDvConverter> DvDict for StringPairBasedDvDictionary<C> {
    fn add_datavalue(&mut self, dv: AnyDataValue) -> AddResult {
        if let Some([first, second]) = C::dict_string_pair(&dv) {
            self.string_pair_dict.add_str_pair(&first, &second)
        } else {
            AddResult::Rejected
        }
    }

    fn fresh_null(&mut self) -> (AnyDataValue, usize) {
        panic!("string-pair-based dictionaries cannot make fresh nulls");
    }

    fn fresh_null_id(&mut self) -> usize {
        panic!("string-pair-based dictionaries cannot make fresh nulls");
    }

    fn datavalue_to_id(&self, dv: &AnyDataValue) -> Option<usize> {
        if let Some([first, second]) = C::dict_string_pair(&dv) {
            self.string_pair_dict.str_pair_to_id(&first, &second)
        } else {
            None
        }
    }

    fn id_to_datavalue(&self, id: usize) -> Option<AnyDataValue> {
        if let Some([first, second]) = self.string_pair_dict.id_to_string_pair(id) {
            C::string_pair_to_datavalue(first.as_str(), second.as_str())
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
        if let Some([first, second]) = C::dict_string_pair(&dv) {
            self.string_pair_dict.mark_str_pair(&first, &second)
        } else {
            AddResult::Rejected
        }
    }

    fn has_marked(&self) -> bool {
        self.string_pair_dict.has_marked()
    }
}

#[cfg(test)]
#[cfg(feature = "stringpairdictionary")]
mod test {

    use crate::dictionary::string_pair_dv_dict::{LangStringDvDictionary, OtherDvDictionary};
    use crate::{
        datavalues::AnyDataValue,
        dictionary::{AddResult, DvDict, KNOWN_ID_MARK},
    };

    //use super::{LangStringDvDictionary, OtherDvDictionary};

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
