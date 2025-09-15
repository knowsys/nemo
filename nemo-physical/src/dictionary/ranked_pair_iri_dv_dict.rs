//! A [DvDict] implementation based on converting IRI data values to pairs of strings.

use super::ranked_pair_dv_dict::{DvPairConverter, RankedPairBasedDvDictionary};
use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};

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

#[cfg(test)]
mod test {
    use crate::{
        datavalues::AnyDataValue,
        dictionary::{
            AddResult, DvDict, KNOWN_ID_MARK, ranked_pair_iri_dv_dict::IriSplittingDvDictionary,
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
}
