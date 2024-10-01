//! A [DvDict] implementation based on converting language-tagged string data values to pairs of strings.

use super::ranked_pair_dv_dict::{DvPairConverter, RankedPairBasedDvDictionary};
use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};

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

#[cfg(test)]
mod test {

    use crate::{
        datavalues::AnyDataValue,
        dictionary::{
            ranked_pair_langstring_dv_dict::LangStringSplittingDvDictionary, AddResult, DvDict,
            KNOWN_ID_MARK,
        },
    };

    #[test]
    fn langstring_dict_add_and_mark() {
        let mut dict = LangStringSplittingDvDictionary::new();

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
