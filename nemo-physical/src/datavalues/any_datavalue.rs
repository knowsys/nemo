//! This module provides implementations [`super::DataValue`]s that can represent any
//! datavalue that we support.

use delegate::delegate;

use super::{
    DataValue, DoubleDataValue, IriDataValue, LangStringDataValue, LongDataValue,
    NonFiniteFloatError, OtherDataValue, StringDataValue, UnsignedLongDataValue, ValueDomain,
};

/// Enum that can represent arbitrary [`DataValue`]s.
#[derive(Debug, Clone)]
pub enum AnyDataValue {
    /// Variant for representing [`DataValue`]s in [`ValueDomain::String`].
    String(StringDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::LanguageTaggedString`].
    LanguageTaggedString(LangStringDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::Iri`].
    Iri(IriDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::Double`].
    Double(DoubleDataValue),
    /// Variant for representing [`DataValue`]s that represent numbers in the u64 range.
    /// Note that this has some overlap with values of [`AnyDataValue::Long`], which is accounted
    /// for in the [`Eq`] implementation.
    UnsignedLong(UnsignedLongDataValue),
    /// Variant for representing [`DataValue`]s that represent numbers in the i64 range.
    /// Note that this has some overlap with values of [`AnyDataValue::UnsignedLong`], which is accounted
    /// for in the [`Eq`] implementation.
    Long(LongDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::Other`].
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
    pub fn new_language_tagged_string(value: String, lang_tag: String) -> Self {
        AnyDataValue::LanguageTaggedString(LangStringDataValue::new(value, lang_tag))
    }

    /// Construct a datavalue in [`ValueDomain::Other`] specified by the given lexical value and datatype IRI.
    pub fn new_other(lexical_value: String, datatype_iri: String) -> Self {
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

impl PartialEq for AnyDataValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AnyDataValue::String(dv), AnyDataValue::String(dv_other)) => dv == dv_other,
            (AnyDataValue::Iri(dv), AnyDataValue::Iri(dv_other)) => dv == dv_other,
            (
                AnyDataValue::LanguageTaggedString(dv),
                AnyDataValue::LanguageTaggedString(dv_other),
            ) => dv == dv_other,
            (AnyDataValue::Double(dv), AnyDataValue::Double(dv_other)) => dv == dv_other,
            (AnyDataValue::Long(_), _) => {
                other.fits_into_i64() && other.to_i64_unchecked() == self.to_i64_unchecked()
            }
            (AnyDataValue::UnsignedLong(_), _) => {
                other.fits_into_u64() && other.to_u64_unchecked() == self.to_u64_unchecked()
            }
            (AnyDataValue::Other(dv), AnyDataValue::Other(dv_other)) => dv == dv_other,
            _ => false,
        }
    }
}

impl Eq for AnyDataValue {}

#[cfg(test)]
mod test {
    use super::AnyDataValue;
    use crate::datavalues::{DataValue, ValueDomain};

    #[test]
    fn test_string() {
        let value = "Hello world";
        let dv = AnyDataValue::new_string(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::String);

        assert_eq!(dv.to_string(), Some(value.to_string()));
        assert_eq!(dv.to_string_unchecked(), value.to_string());
    }

    #[test]
    fn test_iri() {
        let value = "http://example.org/nemo";
        let dv = AnyDataValue::new_iri(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#anyURI".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::Iri);

        assert_eq!(dv.to_iri(), Some(value.to_string()));
        assert_eq!(dv.to_iri_unchecked(), value.to_string());
    }

    #[test]
    fn test_double() {
        let value: f64 = 2.34e3;
        let dv = AnyDataValue::new_double_from_f64(value).unwrap();

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#double".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::Double);

        assert_eq!(dv.to_f64(), Some(value));
        assert_eq!(dv.to_f64_unchecked(), value);
    }

    #[test]
    fn test_long() {
        let dv = AnyDataValue::new_integer_from_i64(42);

        assert_eq!(dv.lexical_value(), "42".to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#int".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::NonNegativeInt);

        assert_eq!(dv.fits_into_i32(), true);
        assert_eq!(dv.fits_into_u32(), true);
        assert_eq!(dv.fits_into_i64(), true);
        assert_eq!(dv.fits_into_u64(), true);

        assert_eq!(dv.to_i32(), Some(42));
        assert_eq!(dv.to_i32_unchecked(), 42);
        assert_eq!(dv.to_u32(), Some(42));
        assert_eq!(dv.to_u32_unchecked(), 42);
        assert_eq!(dv.to_i64(), Some(42));
        assert_eq!(dv.to_i64_unchecked(), 42);
        assert_eq!(dv.to_u64(), Some(42));
        assert_eq!(dv.to_u64_unchecked(), 42);
    }

    #[test]
    fn test_unsigned_long() {
        let value: u64 = u64::MAX;
        let dv = AnyDataValue::new_integer_from_u64(value);

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#unsignedLong".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::UnsignedLong);

        assert_eq!(dv.fits_into_i32(), false);
        assert_eq!(dv.fits_into_u32(), false);
        assert_eq!(dv.fits_into_i64(), false);
        assert_eq!(dv.fits_into_u64(), true);

        assert_eq!(dv.to_i32(), None);
        assert_eq!(dv.to_u32(), None);
        assert_eq!(dv.to_i64(), None);
        assert_eq!(dv.to_u64(), Some(value));
        assert_eq!(dv.to_u64_unchecked(), value);
    }

    #[test]
    fn test_lang_string() {
        let value = "Hello world";
        let lang = "en-GB";
        let dv = AnyDataValue::new_language_tagged_string(value.to_string(), lang.to_string());

        assert_eq!(dv.lexical_value(), "Hello world@en-GB");
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::LanguageTaggedString);

        assert_eq!(
            dv.to_language_tagged_string(),
            Some((value.to_string(), lang.to_string()))
        );
        assert_eq!(
            dv.to_language_tagged_string_unchecked(),
            (value.to_string(), lang.to_string())
        );
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

    #[test]
    fn test_eq() {
        let dv_f64 = AnyDataValue::new_double_from_f64(42.0).unwrap();
        let dv_i64 = AnyDataValue::new_integer_from_i64(42);
        let dv_u64 = AnyDataValue::new_integer_from_u64(42);
        let dv_u64_2 = AnyDataValue::new_integer_from_u64(43);
        let dv_string = AnyDataValue::new_string("42".to_string());
        let dv_iri = AnyDataValue::new_iri("42".to_string());
        let dv_lang_string =
            AnyDataValue::new_language_tagged_string("42".to_string(), "en-GB".to_string());
        let dv_other = AnyDataValue::new_other(
            "42".to_string(),
            "http://www.w3.org/2001/XMLSchema#hexBinary".to_string(),
        );

        assert_eq!(dv_f64, dv_f64);
        assert_ne!(dv_f64, dv_i64);
        assert_ne!(dv_f64, dv_u64);
        assert_ne!(dv_f64, dv_u64_2);
        assert_ne!(dv_f64, dv_string);
        assert_ne!(dv_f64, dv_iri);
        assert_ne!(dv_f64, dv_lang_string);
        assert_ne!(dv_f64, dv_other);

        assert_ne!(dv_u64, dv_f64);
        assert_eq!(dv_u64, dv_i64);
        assert_eq!(dv_u64, dv_u64);
        assert_ne!(dv_u64, dv_u64_2);
        assert_ne!(dv_u64, dv_string);
        assert_ne!(dv_u64, dv_iri);
        assert_ne!(dv_u64, dv_lang_string);
        assert_ne!(dv_u64, dv_other);

        assert_ne!(dv_i64, dv_f64);
        assert_eq!(dv_i64, dv_i64);
        assert_eq!(dv_i64, dv_u64);
        assert_ne!(dv_i64, dv_u64_2);
        assert_ne!(dv_i64, dv_string);
        assert_ne!(dv_i64, dv_iri);
        assert_ne!(dv_i64, dv_lang_string);
        assert_ne!(dv_i64, dv_other);

        assert_ne!(dv_string, dv_f64);
        assert_ne!(dv_string, dv_i64);
        assert_ne!(dv_string, dv_u64);
        assert_ne!(dv_string, dv_u64_2);
        assert_eq!(dv_string, dv_string);
        assert_ne!(dv_string, dv_iri);
        assert_ne!(dv_string, dv_lang_string);
        assert_ne!(dv_string, dv_other);

        assert_ne!(dv_iri, dv_f64);
        assert_ne!(dv_iri, dv_i64);
        assert_ne!(dv_iri, dv_u64);
        assert_ne!(dv_iri, dv_u64_2);
        assert_ne!(dv_iri, dv_string);
        assert_eq!(dv_iri, dv_iri);
        assert_ne!(dv_iri, dv_lang_string);
        assert_ne!(dv_iri, dv_other);

        assert_ne!(dv_lang_string, dv_f64);
        assert_ne!(dv_lang_string, dv_i64);
        assert_ne!(dv_lang_string, dv_u64);
        assert_ne!(dv_lang_string, dv_u64_2);
        assert_ne!(dv_lang_string, dv_string);
        assert_ne!(dv_lang_string, dv_iri);
        assert_eq!(dv_lang_string, dv_lang_string);
        assert_ne!(dv_lang_string, dv_other);

        assert_ne!(dv_other, dv_f64);
        assert_ne!(dv_other, dv_i64);
        assert_ne!(dv_other, dv_u64);
        assert_ne!(dv_other, dv_u64_2);
        assert_ne!(dv_other, dv_string);
        assert_ne!(dv_other, dv_iri);
        assert_ne!(dv_other, dv_lang_string);
        assert_eq!(dv_other, dv_other);
    }
}
