//! A [DvDict] implementation based on converting IRI data values to strings.

use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use super::string_dv_dict::{one_string_to_two, two_strings_to_one, DvConverter, StringBasedDvDictionary};

/// Implementation of [DvDict] that will only handle [ValueDomain::Other] values.
pub(crate) type OtherDvDictionary = StringBasedDvDictionary<OtherDvConverter>;

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


#[cfg(test)]
mod test {
    use crate::{datavalues::AnyDataValue, dictionary::{string_other_dv_dict::OtherDvDictionary, AddResult, DvDict, KNOWN_ID_MARK}};

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
}