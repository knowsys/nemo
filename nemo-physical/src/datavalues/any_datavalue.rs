//! This module provides implementations [`super::DataValue`]s that can represent any
//! datavalue that we support.
 
use super::{DataValue,ValueDomain,DoubleDataValue,StringDataValue,IriDataValue,LongDataValue,UnsignedLongDataValue,LangStringDataValue};

/// Enum that can represent arbitrary [`DataValue`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyDataValue {
    String(StringDataValue),
    LanguageTaggedString(LangStringDataValue),
    Iri(IriDataValue),
    Double(DoubleDataValue),
    UnsignedLong(UnsignedLongDataValue),
    //NonNegativeLong(LongDataValue),
    //UnsignedInt(LongDataValue),
    //NonNegativeInt(LongDataValue),
    Long(LongDataValue),
    //Int(LongDataValue),
//    /// Domain of all data values not covered by the remaining domains
//    Other,
}

impl DataValue for AnyDataValue {
    fn datatype_iri(&self) -> String {
        match self {
            AnyDataValue::String(dv) => dv.datatype_iri(),
            _ => panic!("Unsupported")
        }
    }

    fn lexical_value(&self) -> String {
        match self {
            AnyDataValue::String(dv) => dv.lexical_value(),
            _ => panic!("Unsupported")
        }
    }

    fn value_domain(&self) -> ValueDomain {
        match self {
            AnyDataValue::String(dv) => dv.value_domain(),
            _ => panic!("Unsupported")
        }
    }

    fn to_string_unchecked(&self) -> String {
        match self {
            AnyDataValue::String(dv) => dv.to_string_unchecked(),
            _ => panic!("Unsupported")
        }
    }
}

#[cfg(test)]
mod test {
    use super::AnyDataValue;
    use crate::datavalues::{DataValue,ValueDomain,StringDataValue};

    #[test]
    fn test_string() {
        let value = "Hello world";
        let dv = AnyDataValue::String(StringDataValue::new(value.to_string()));

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(dv.datatype_iri(), "http://www.w3.org/2001/XMLSchema#string".to_string());
        assert_eq!(dv.value_domain(), ValueDomain::String);

        assert_eq!(dv.to_string(), Some(value.to_string()));
        assert_eq!(dv.to_string_unchecked(), value.to_string());
    }
}