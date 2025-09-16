//! A [DvDict] implementation based on converting other data values to pairs of strings.

use super::ranked_pair_dv_dict::{DvPairConverter, RankedPairBasedDvDictionary};
use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};

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

#[cfg(test)]
mod test {
    use crate::{
        datavalues::AnyDataValue,
        dictionary::{
            AddResult, DvDict, KNOWN_ID_MARK, ranked_pair_other_dv_dict::OtherSplittingDvDictionary,
        },
    };

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
}
