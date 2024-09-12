//! A [DvDict] implementation based on converting IRI data values to strings.

use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use super::string_dv_dict::{one_string_to_two, two_strings_to_one, DvConverter, StringBasedDvDictionary};

/// Implementation of [DvDict] that will only handle [ValueDomain::LanguageTaggedString] values.
pub(crate) type LangStringDvDictionary = StringBasedDvDictionary<LangStringDvConverter>;

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


#[cfg(test)]
mod test {
    use crate::{datavalues::AnyDataValue, dictionary::{string_langstring_dv_dict::LangStringDvDictionary, AddResult, DvDict, KNOWN_ID_MARK}};

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