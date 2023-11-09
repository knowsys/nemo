//! This module provides implementations [`super::DataValue`]s that can represent any
//! datavalue that we support.

use delegate::delegate;

use super::{DataValue,ValueDomain,DoubleDataValue,StringDataValue,
    IriDataValue,LongDataValue,UnsignedLongDataValue,
    LangStringDataValue, OtherDataValue, NonFiniteFloatError};

/// Enum that can represent arbitrary [`DataValue`]s.
#[derive(Debug, Clone)]
pub enum AnyDataValue {
    String(StringDataValue),
    LanguageTaggedString(LangStringDataValue),
    Iri(IriDataValue),
    Double(DoubleDataValue),
    UnsignedLong(UnsignedLongDataValue),
    Long(LongDataValue),
    Other(OtherDataValue),
}

impl AnyDataValue {
    /// Construct a datavalue that represents the given number.
    pub fn new_integer_from_i64(value: i64) -> Self {
        AnyDataValue::Long(LongDataValue::new(value))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_integer_from_u64(value: u64) -> Self {
        AnyDataValue::UnsignedLong(UnsignedLongDataValue::new(value))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_double_from_f64(value: f64) -> Result<AnyDataValue, NonFiniteFloatError> {
        match DoubleDataValue::new(value) {
            Ok(dv) => Ok(AnyDataValue::Double(dv)),
            Err(e) => Err(e),
        }
    }

    /// Construct a datavalue in [`ValueDomain::String`] that represents the given string.
    pub fn new_string(value: String) -> Self {
        AnyDataValue::String(StringDataValue::new(value))
    }

    /// Construct a datavalue in [`ValueDomain::IRI`] that represents the given IRI.
    pub fn new_iri(value: String) -> Self {
        AnyDataValue::Iri(IriDataValue::new(value))
    }

    /// Construct a datavalue in [`ValueDomain::LanguageTaggedString`] that represents a string with a language tag.
    pub fn new_language_tagged_string(value: String,lang_tag: String) -> Self {
        AnyDataValue::LanguageTaggedString(LangStringDataValue::new(value, lang_tag))
    }

    /// Construct a datavalue in [`ValueDomain::Other`] specified by the given lexical value and datatype IRI.
    pub fn new_other(lexical_value: String,datatype_iri: String) -> Self {
        AnyDataValue::Other(OtherDataValue::new(lexical_value, datatype_iri))
    }
}

impl DataValue for AnyDataValue {
    delegate! {
        to match self {
            AnyDataValue::String(dv)=> dv,
            AnyDataValue::LanguageTaggedString(dv) => dv,
            AnyDataValue::Iri(dv) => dv,
            AnyDataValue::Double(dv) => dv,
            AnyDataValue::UnsignedLong(dv) => dv,
            AnyDataValue::Long(dv) => dv,
            AnyDataValue::Other(dv) => dv,
        } {
            fn datatype_iri(&self) -> String;
            fn lexical_value(&self) -> String;
            fn value_domain(&self) -> ValueDomain;
            fn to_string(&self) -> Option<String>;
            fn to_string_unchecked(&self) -> String;
            fn to_language_tagged_string(&self) -> Option<(String, String)>;
            fn to_language_tagged_string_unchecked(&self) -> (String, String);
            fn to_iri(&self) -> Option<String>;
            fn to_iri_unchecked(&self) -> String;
            fn to_f64(&self) -> Option<f64>;
            fn to_f64_unchecked(&self) -> f64;
            fn fits_into_i64(&self) -> bool;
            fn fits_into_i32(&self) -> bool;
            fn fits_into_u64(&self) -> bool;
            fn fits_into_u32(&self) -> bool;
            fn to_i64(&self) -> Option<i64>;
            fn to_i64_unchecked(&self) -> i64;
            fn to_i32(&self) -> Option<i32>;
            fn to_i32_unchecked(&self) -> i32;
            fn to_u64(&self) -> Option<u64>;
            fn to_u64_unchecked(&self) -> u64;
            fn to_u32(&self) -> Option<u32>;
            fn to_u32_unchecked(&self) -> u32;
            fn tuple_element(&self, index: usize) -> Option<&dyn DataValue>;
            fn tuple_len(&self) -> Option<usize>;
            fn tuple_len_unchecked(&self) -> usize;
            fn tuple_element_unchecked(&self, _index: usize) -> &dyn DataValue;
        }
    }
}

#[cfg(test)]
mod test {
    use super::AnyDataValue;
    use crate::datavalues::{DataValue,ValueDomain};

    #[test]
    fn test_string() {
        let value = "Hello world";
        let dv = AnyDataValue::new_string(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(dv.datatype_iri(), "http://www.w3.org/2001/XMLSchema#string".to_string());
        assert_eq!(dv.value_domain(), ValueDomain::String);

        assert_eq!(dv.to_string(), Some(value.to_string()));
        assert_eq!(dv.to_string_unchecked(), value.to_string());
    }

    #[test]
    fn test_iri() {
        let value = "http://example.org/nemo";
        let dv = AnyDataValue::new_iri(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(dv.datatype_iri(), "http://www.w3.org/2001/XMLSchema#anyURI".to_string());
        assert_eq!(dv.value_domain(), ValueDomain::Iri);

        assert_eq!(dv.to_iri(), Some(value.to_string()));
        assert_eq!(dv.to_iri_unchecked(), value.to_string());
    }

    #[test]
    fn test_double() {
        let value: f64 = 2.34e3;
        let double = AnyDataValue::new_double_from_f64(value).unwrap();

        assert_eq!(double.lexical_value(), value.to_string());
        assert_eq!(double.datatype_iri(), "http://www.w3.org/2001/XMLSchema#double".to_string());
        assert_eq!(double.value_domain(), ValueDomain::Double);

        assert_eq!(double.to_f64(), Some(value));
        assert_eq!(double.to_f64_unchecked(), value);
    }

    #[test]
    fn test_long() {
        let long1 = AnyDataValue::new_integer_from_i64(42);

        assert_eq!(long1.lexical_value(), "42".to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#int".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::NonNegativeInt);

        assert_eq!(long1.fits_into_i32(), true);
        assert_eq!(long1.fits_into_u32(), true);
        assert_eq!(long1.fits_into_i64(), true);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), Some(42));
        assert_eq!(long1.to_i32_unchecked(), 42);
        assert_eq!(long1.to_u32(), Some(42));
        assert_eq!(long1.to_u32_unchecked(), 42);
        assert_eq!(long1.to_i64(), Some(42));
        assert_eq!(long1.to_i64_unchecked(), 42);
        assert_eq!(long1.to_u64(), Some(42));
        assert_eq!(long1.to_u64_unchecked(), 42);
    }

    #[test]
    fn test_unsigned_long() {
        let value: u64 =  u64::MAX;
        let long1 = AnyDataValue::new_integer_from_u64(value);

        assert_eq!(long1.lexical_value(), value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::UnsignedLong);

        assert_eq!(long1.fits_into_i32(), false);
        assert_eq!(long1.fits_into_u32(), false);
        assert_eq!(long1.fits_into_i64(), false);
        assert_eq!(long1.fits_into_u64(), true);

        assert_eq!(long1.to_i32(), None);
        assert_eq!(long1.to_u32(), None);
        assert_eq!(long1.to_i64(), None);
        assert_eq!(long1.to_u64(), Some(value));
        assert_eq!(long1.to_u64_unchecked(), value);
    }

    #[test]
    fn test_lang_string() {
        let value = "Hello world";
        let lang = "en-GB";
        let dv = AnyDataValue::new_language_tagged_string(value.to_string(), lang.to_string());

        assert_eq!(dv.lexical_value(), "Hello world@en-GB");
        assert_eq!(dv.datatype_iri(), "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string());
        assert_eq!(dv.value_domain(), ValueDomain::LanguageTaggedString);

        assert_eq!(dv.to_language_tagged_string(), Some((value.to_string(), lang.to_string())));
        assert_eq!(dv.to_language_tagged_string_unchecked(), (value.to_string(), lang.to_string()));
    }

    #[test]
    fn test_other() {
        let value = "0FB7";
        let datatype_iri = "http://www.w3.org/2001/XMLSchema#hexBinary";
        let dv = AnyDataValue::new_other(value.to_string(), datatype_iri.to_string());

        assert_eq!(dv.lexical_value(), value);
        assert_eq!(dv.datatype_iri(), datatype_iri);
        assert_eq!(dv.value_domain(), ValueDomain::Other);

        assert_eq!(dv.to_string(), None);
        assert_eq!(dv.to_iri(), None);
        assert_eq!(dv.to_language_tagged_string(), None);
        // ...
    }
}