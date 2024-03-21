use super::DvDict;
use crate::datavalues::{AnyDataValue, DataValue, ValueDomain};
use std::fmt::Debug;

/// Trait to encapsulate (static) functions for converting datavalues to strings
/// and vice versa. The mapping must therefore be invertible, but otherwise it
/// can be arbitrary. Implementations may choose which datavalues to support.
pub(crate) trait PairDvConverter: Debug {
    /// Converts a datavalue to a string pair, when supported.
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]>;
    /// Converts a string pair to a datavalue, if supported.
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue>;
    /// Each converter supports exactly one domain, returned by this function.
    fn supported_value_domain() -> ValueDomain;
}

/// Implementation of [PairDvConverter] to handle [AnyDataValue::Other] values.
///
/// FIXME: This currently also handles booleans, but our API is not designed for having several
/// value domains in one dictionary, so the supported_value_domain() is just Other. Should not
/// hurt much once we do not have "isOther" as a check in DV dicts, but is still not a clean solution.
#[derive(Debug)]
pub(crate) struct OtherDvConverter;
impl PairDvConverter for OtherDvConverter {
    #[inline(always)]
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        Some([dv.lexical_value(), dv.datatype_iri()])
    }

    /// Function to use with StringPairBasedDvDictionary
    #[inline(always)]
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        if second == "http://www.w3.org/2001/XMLSchema#boolean" {
            Some(AnyDataValue::new_boolean(first == "true"))
        } else {
            Some(AnyDataValue::new_other(
                first.to_string(),
                second.to_string(),
            ))
        }
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::Other
    }
}

/// Implementation of [PairDvConverter] to handle [AnyDataValue::LangStringDataValue] values.
#[derive(Debug)]
pub(crate) struct LangStringDvConverter;
impl PairDvConverter for LangStringDvConverter {
    #[inline(always)]
    fn dict_string_pair(dv: &AnyDataValue) -> Option<[String; 2]> {
        let (string, lang) = dv.to_language_tagged_string_unchecked();
        Some([string, lang])
    }

    /// Function to use with StringPairBasedDvDictionary
    #[inline(always)]
    fn string_pair_to_datavalue(first: &str, second: &str) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_language_tagged_string(
            first.to_string(),
            second.to_string(),
        ))
    }

    fn supported_value_domain() -> ValueDomain {
        ValueDomain::LanguageTaggedString
    }
}
