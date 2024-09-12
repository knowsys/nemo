//! A [DvDict] implementation based on converting IRI data values to strings.

use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use super::string_dv_dict::{DvConverter, StringBasedDvDictionary};

/// Implementation of [DvDict] that will only handle [ValueDomain::Iri] values.
pub(crate) type IriDvDictionary = StringBasedDvDictionary<IriDvConverter>;

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

#[cfg(test)]
mod test {
    use crate::{datavalues::AnyDataValue, dictionary::{string_iri_dv_dict::IriDvDictionary, AddResult, DvDict, KNOWN_ID_MARK}};


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
}