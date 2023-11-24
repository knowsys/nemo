//! This module provides implementations [`super::DataValue`]s that can represent any
//! datavalue that we support.

use std::str::FromStr;

use delegate::delegate;

use super::{
    DataValue, DataValueCreationError, DoubleDataValue, IriDataValue, LangStringDataValue,
    LongDataValue, OtherDataValue, StringDataValue, UnsignedLongDataValue, ValueDomain,
};

// Initial part of IRI in all XML Schema types:
const XSD_PREFIX: &str = "http://www.w3.org/2001/XMLSchema#";

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
    pub fn new_double_from_f64(value: f64) -> Result<AnyDataValue, DataValueCreationError> {
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

    /// Construct a normalized datavalue in an appropriate [`ValueDomain`] from the given lexical value and datatype IRI.
    /// Known RDF and XML Schema datatypes are taken into account.
    pub fn new_from_typed_literal(
        lexical_value: String,
        datatype_iri: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        // Macro for parsing strings as i64 and checking bounds of specific integer types
        macro_rules! parse_integer {
            ($lexical_value:expr$(;$min:expr;$max:expr;$typename:expr)?) => {
                match i64::from_str(&$lexical_value) {
                    Ok(value) => {
                        $(if value < $min || value > $max {
                            return Err(DataValueCreationError::IntegerRange{min: $min, max: $max, value: value, datatype_name: $typename.to_string()})
                        })?
                        Ok(Self::new_integer_from_i64(value))
                    },
                    Err(e) => Err(DataValueCreationError::from(e)),
                }
            }
        }

        if let Some(xsd_type) = datatype_iri.strip_prefix(XSD_PREFIX) {
            match xsd_type {
                "string" => Ok(Self::new_string(lexical_value)),
                "long" => parse_integer!(lexical_value),
                "int" => parse_integer!(lexical_value; i32::MIN as i64; i32::MAX as i64; "xsd:int"),
                "short" => parse_integer!(lexical_value; -32768; 32767; "xsd:short"),
                "byte" => parse_integer!(lexical_value; -128; 127; "xsd:byte"),
                "unsignedInt" => parse_integer!(lexical_value; 0; 4294967295; "xsd:unsignedInt"),
                "unsignedShort" => parse_integer!(lexical_value; 0; 65535; "xsd:unsignedShort"),
                "unsignedByte" => parse_integer!(lexical_value; 0; 255; "xsd:unsignedByte"),
                "unsignedLong" => {
                    // We still prefer to use i64 if possible:
                    match u64::from_str(&lexical_value) {
                        Ok(value) => {
                            if let Ok(i64value) = i64::try_from(value) {
                                Ok(Self::new_integer_from_i64(i64value))
                            } else {
                                Ok(Self::new_integer_from_u64(value))
                            }
                        }
                        Err(e) => Err(DataValueCreationError::from(e)),
                    }
                }
                "double" => match f64::from_str(&lexical_value) {
                    Ok(value) => Self::new_double_from_f64(value),
                    Err(e) => Err(DataValueCreationError::from(e)),
                },
                _ => Ok(Self::new_other(lexical_value, datatype_iri)),
            }
        } else {
            Ok(Self::new_other(lexical_value, datatype_iri))
        }
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
    use super::{AnyDataValue, XSD_PREFIX};
    use crate::datavalues::{DataValue, DataValueCreationError, ValueDomain};

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

    #[test]
    fn test_new_from_string_literal() {
        let string_res = AnyDataValue::new_from_typed_literal(
            "Hello World.".to_string(),
            XSD_PREFIX.to_owned() + "string",
        );

        assert_eq!(
            string_res,
            Ok(AnyDataValue::new_string("Hello World.".to_string()))
        );
    }

    #[test]
    fn test_new_from_long_literal() {
        let long_res1 = AnyDataValue::new_from_typed_literal(
            "+4398046511104".to_string(),
            XSD_PREFIX.to_owned() + "long",
        );
        let long_res2 = AnyDataValue::new_from_typed_literal(
            "invalid".to_string(),
            XSD_PREFIX.to_owned() + "long",
        );
        let long_res3 = AnyDataValue::new_from_typed_literal(
            "999999999999999999999999999999999".to_string(),
            XSD_PREFIX.to_owned() + "long",
        );

        assert_eq!(
            long_res1,
            Ok(AnyDataValue::new_integer_from_i64(4398046511104))
        );
        assert!(matches!(
            long_res2,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
        assert!(matches!(
            long_res3,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
    }

    #[test]
    fn test_new_from_int_literal() {
        let int_res1 = AnyDataValue::new_from_typed_literal(
            "016777216".to_string(),
            XSD_PREFIX.to_owned() + "int",
        );
        let int_res2 = AnyDataValue::new_from_typed_literal(
            "+4398046511104".to_string(),
            XSD_PREFIX.to_owned() + "int",
        );

        assert_eq!(int_res1, Ok(AnyDataValue::new_integer_from_i64(16777216)));
        assert!(matches!(
            int_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_short_literal() {
        let short_res1 = AnyDataValue::new_from_typed_literal(
            "-2345".to_string(),
            XSD_PREFIX.to_owned() + "short",
        );
        let short_res2 = AnyDataValue::new_from_typed_literal(
            "16777216".to_string(),
            XSD_PREFIX.to_owned() + "short",
        );

        assert_eq!(short_res1, Ok(AnyDataValue::new_integer_from_i64(-2345)));
        assert!(matches!(
            short_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_byte_literal() {
        let byte_res1 =
            AnyDataValue::new_from_typed_literal("-35".to_string(), XSD_PREFIX.to_owned() + "byte");
        let byte_res2 = AnyDataValue::new_from_typed_literal(
            "-200".to_string(),
            XSD_PREFIX.to_owned() + "byte",
        );

        assert_eq!(byte_res1, Ok(AnyDataValue::new_integer_from_i64(-35)));
        assert!(matches!(
            byte_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_ulong_literal() {
        let ulong_res1 = AnyDataValue::new_from_typed_literal(
            "+018446744073709551574".to_string(),
            XSD_PREFIX.to_owned() + "unsignedLong",
        );
        let ulong_res2 = AnyDataValue::new_from_typed_literal(
            "-4".to_string(),
            XSD_PREFIX.to_owned() + "unsignedLong",
        );
        let ulong_res3 = AnyDataValue::new_from_typed_literal(
            "36893488147419103232".to_string(),
            XSD_PREFIX.to_owned() + "unsignedLong",
        );

        assert_eq!(
            ulong_res1,
            Ok(AnyDataValue::new_integer_from_u64(18446744073709551574))
        );
        assert!(matches!(
            ulong_res2,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
        assert!(matches!(
            ulong_res3,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
    }

    #[test]
    fn test_new_from_uint_literal() {
        let uint_res1 = AnyDataValue::new_from_typed_literal(
            "0004294967254".to_string(),
            XSD_PREFIX.to_owned() + "unsignedInt",
        );
        let uint_res2 = AnyDataValue::new_from_typed_literal(
            "-4".to_string(),
            XSD_PREFIX.to_owned() + "unsignedInt",
        );

        assert_eq!(
            uint_res1,
            Ok(AnyDataValue::new_integer_from_i64(4294967254))
        );
        assert!(matches!(
            uint_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_ushort_literal() {
        let ushort_res1 = AnyDataValue::new_from_typed_literal(
            "2345".to_string(),
            XSD_PREFIX.to_owned() + "unsignedShort",
        );
        let ushort_res2 = AnyDataValue::new_from_typed_literal(
            "-4".to_string(),
            XSD_PREFIX.to_owned() + "unsignedShort",
        );

        assert_eq!(ushort_res1, Ok(AnyDataValue::new_integer_from_i64(2345)));
        assert!(matches!(
            ushort_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_ubyte_literal() {
        let ubyte_res1 = AnyDataValue::new_from_typed_literal(
            "255".to_string(),
            XSD_PREFIX.to_owned() + "unsignedByte",
        );
        let ubyte_res2 = AnyDataValue::new_from_typed_literal(
            "-4".to_string(),
            XSD_PREFIX.to_owned() + "unsignedByte",
        );

        assert_eq!(ubyte_res1, Ok(AnyDataValue::new_integer_from_i64(255)));
        assert!(matches!(
            ubyte_res2,
            Err(DataValueCreationError::IntegerRange { .. })
        ));
    }

    #[test]
    fn test_new_from_double_literal() {
        let double_res1 = AnyDataValue::new_from_typed_literal(
            "-223.56".to_string(),
            XSD_PREFIX.to_owned() + "double",
        );
        let double_res2 = AnyDataValue::new_from_typed_literal(
            "invalid".to_string(),
            XSD_PREFIX.to_owned() + "double",
        );

        assert_eq!(double_res1, AnyDataValue::new_double_from_f64(-223.56));
        assert!(matches!(
            double_res2,
            Err(DataValueCreationError::FloatNotParsed { .. })
        ));
    }
}
