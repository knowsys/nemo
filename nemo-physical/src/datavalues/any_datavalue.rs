//! This module provides implementations [`super::DataValue`]s that can represent any
//! datavalue that we support.

use std::{num::IntErrorKind, str::FromStr};

use delegate::delegate;

use crate::{
    datatypes::{Float, Double, StorageValueT},
    dictionary::DvDict,
    management::database::Dict,
};

use super::{
    boolean::BooleanDataValue, errors::InternalDataValueCreationError,
    float_datavalues::FloatDataValue, DataValue, DataValueCreationError, DoubleDataValue,
    IriDataValue, LangStringDataValue, LongDataValue, OtherDataValue, StringDataValue,
    UnsignedLongDataValue, ValueDomain,
};

// Initial part of IRI in all XML Schema types:
const XSD_PREFIX: &str = "http://www.w3.org/2001/XMLSchema#";

/// Supported kinds of arbitrary size numbers.
/// The variants we consider are taken from the XML Schema
/// datatypes. Fixed-size integer types, such as long, are
/// supported too, but don't need this enum.
#[derive(Debug, Clone, Copy)]
enum DecimalType {
    Decimal,
    Integer,
    NonNegativeInteger,
    PositiveInteger,
    NonPositiveInteger,
    NegativeInteger,
}
impl DecimalType {
    fn datatype_iri(&self) -> String {
        match self {
            DecimalType::Decimal => XSD_PREFIX.to_owned() + "decimal",
            DecimalType::Integer => XSD_PREFIX.to_owned() + "integer",
            DecimalType::NonNegativeInteger => XSD_PREFIX.to_owned() + "nonNegativeInteger",
            DecimalType::PositiveInteger => XSD_PREFIX.to_owned() + "positiveInteger",
            DecimalType::NonPositiveInteger => XSD_PREFIX.to_owned() + "nonPositiveInteger",
            DecimalType::NegativeInteger => XSD_PREFIX.to_owned() + "negativeInteger",
        }
    }
}

/// Enum that can represent arbitrary [`DataValue`]s.
#[derive(Debug, Clone)]
pub enum AnyDataValue {
    /// Variant for representing [`DataValue`]s in [`ValueDomain::String`].
    String(StringDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::LanguageTaggedString`].
    LanguageTaggedString(LangStringDataValue),
    /// Variant for representing [`DataValue`]s in [`ValueDomain::Iri`].
    Iri(IriDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Float].
    Float(FloatDataValue),
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
    /// Variant for representing [DataValue]s in [ValueDomain::Boolean].
    Boolean(BooleanDataValue),
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
    pub fn new_float_from_f32(value: f32) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue::Float(FloatDataValue::from_f32(value)?))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_double_from_f64(value: f64) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue::Double(DoubleDataValue::from_f64(value)?))
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

    /// Construct a datavalue in [ValueDomain::Boolean].
    pub fn new_boolean(value: bool) -> Self {
        AnyDataValue::Boolean(BooleanDataValue::new(value))
    }

    /// Construct a datavalue in [`ValueDomain::Other`] specified by the given lexical value and datatype IRI.
    pub fn new_other(lexical_value: String, datatype_iri: String) -> Self {
        AnyDataValue::Other(OtherDataValue::new(lexical_value, datatype_iri))
    }

    /// Construct a datavalue from its physical representation as a [`StorageValueT`], using
    /// the given dictionary to resolve IDs.
    pub(crate) fn new_from_storage_value(
        sv: StorageValueT,
        dict: &Dict,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match sv {
            StorageValueT::Id32(id) => {
                if let Some(value) = dict.id_to_datavalue(usize::try_from(id).unwrap()) {
                    Ok(value)
                } else {
                    Err(DataValueCreationError::InternalError(Box::new(
                        InternalDataValueCreationError::DictionaryIdNotFound(
                            usize::try_from(id).unwrap(),
                        ),
                    )))
                }
            }
            StorageValueT::Id64(id) => {
                if let Some(value) = dict.id_to_datavalue(usize::try_from(id).unwrap()) {
                    Ok(value)
                } else {
                    Err(DataValueCreationError::InternalError(Box::new(
                        InternalDataValueCreationError::DictionaryIdNotFound(
                            usize::try_from(id).unwrap(),
                        ),
                    )))
                }
            }
            StorageValueT::Int64(num) => Ok(AnyDataValue::new_integer_from_i64(num)),
            StorageValueT::Float(_) => Err(DataValueCreationError::InternalError(Box::new(
                InternalDataValueCreationError::SinglePrecisionFloat,
            ))),
            StorageValueT::Double(num) => {
                Ok(AnyDataValue::new_double_from_f64(num.into()).unwrap())
            }
        }
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
                            return Err(DataValueCreationError::IntegerRange{min: $min, max: $max, value, datatype_name: $typename.to_string()})
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
                "decimal" => {
                    Self::new_from_decimal_type_literal(lexical_value, DecimalType::Decimal)
                }
                "integer" => {
                    Self::new_from_decimal_type_literal(lexical_value, DecimalType::Integer)
                }
                "positiveInteger" => {
                    Self::new_from_decimal_type_literal(lexical_value, DecimalType::PositiveInteger)
                }
                "nonNegativeInteger" => Self::new_from_decimal_type_literal(
                    lexical_value,
                    DecimalType::NonNegativeInteger,
                ),
                "negativeInteger" => {
                    Self::new_from_decimal_type_literal(lexical_value, DecimalType::NegativeInteger)
                }
                "nonPositiveInteger" => Self::new_from_decimal_type_literal(
                    lexical_value,
                    DecimalType::NonPositiveInteger,
                ),
                "double" => Self::new_from_double_literal(lexical_value),
                "boolean" => match lexical_value.as_str() {
                    "true" | "1" => Ok(Self::new_boolean(true)),
                    "false" | "0" => Ok(Self::new_boolean(false)),
                    _ => Err(DataValueCreationError::BooleanNotParsed {
                        lexical_value: lexical_value,
                    }),
                },
                _ => Ok(Self::new_other(lexical_value, datatype_iri)),
            }
        } else {
            Ok(Self::new_other(lexical_value, datatype_iri))
        }
    }

    /// Construct a normalized datavalue in an appropriate [`ValueDomain`] from the given string representation
    /// of a (arbitrarily large) integer.
    pub fn new_from_integer_literal(
        lexical_value: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Self::new_from_decimal_type_literal(lexical_value, DecimalType::Integer)
    }

    /// Construct a normalized datavalue in an appropriate [`ValueDomain`] from the given string representation
    /// of a double precision floating-point number.
    pub fn new_from_double_literal(
        lexical_value: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match f64::from_str(&lexical_value) {
            Ok(value) => Self::new_double_from_f64(value),
            Err(e) => Err(DataValueCreationError::from(e)),
        }
    }

    /// Construct a normalized datavalue in an appropriate [`ValueDomain`] for a "big decimal"
    /// or "big integer" [`DecimalType`].
    fn new_from_decimal_type_literal(
        lexical_value: String,
        decimal_type: DecimalType,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match i64::from_str(&lexical_value) {
            Ok(value) => Self::from_decimal_typed_long(value, lexical_value, decimal_type),
            Err(e) => {
                match e.kind() {
                    IntErrorKind::Empty => Err(DataValueCreationError::from(e)),
                    IntErrorKind::InvalidDigit => {
                        if let DecimalType::Decimal = decimal_type {
                            // the invalid digit might have been '.'
                            Self::parse_large_decimal_literal(lexical_value, decimal_type)
                        } else {
                            Err(DataValueCreationError::from(e))
                        }
                    }
                    IntErrorKind::PosOverflow | IntErrorKind::NegOverflow | IntErrorKind::Zero => {
                        Self::parse_large_decimal_literal(lexical_value, decimal_type)
                    }
                    _ => Err(DataValueCreationError::from(e)),
                }
            }
        }
    }

    /// Create [`AnyDataValue`] for the given i64 number after checking possible range
    /// constraints of the given [`DecimalType`].
    fn from_decimal_typed_long(
        value: i64,
        lexical_value: String,
        decimal_type: DecimalType,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match (decimal_type, value) {
            (DecimalType::PositiveInteger, v) if v <= 0 => {
                Self::decimal_parse_error(lexical_value, decimal_type)
            }
            (DecimalType::NonNegativeInteger, v) if v < 0 => {
                return Self::decimal_parse_error(lexical_value, decimal_type)
            }
            (DecimalType::NegativeInteger, v) if v >= 0 => {
                return Self::decimal_parse_error(lexical_value, decimal_type)
            }
            (DecimalType::NonPositiveInteger, v) if v > 0 => {
                return Self::decimal_parse_error(lexical_value, decimal_type)
            }
            _ => return Ok(Self::new_integer_from_i64(value)),
        }
    }

    /// Construct a normalized datavalue in an appropriate [`ValueDomain`] for a "big decimal"
    /// or "big integer" [`DecimalType`] by manually parsing the string.
    fn parse_large_decimal_literal(
        lexical_value: String,
        decimal_type: DecimalType,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        // We transscribe the lexical_value into a canonical form,
        // and record some basic numeric properties in the process.
        let mut trimmed_value = String::with_capacity(lexical_value.len());

        let mut before_sign = true;
        let mut in_leading_zeros = true;
        let mut in_fraction = false;
        let mut len_at_trailing_zeros: usize = 0;

        let mut sign_plus = true;
        let mut is_zero = true;
        let mut has_nonzero_fraction = false;
        for char in lexical_value.bytes() {
            match char {
                b'-' => {
                    if trimmed_value.is_empty() && before_sign {
                        before_sign = false;
                        sign_plus = false;
                        trimmed_value.push('-');
                    } else {
                        return Self::decimal_parse_error(lexical_value, decimal_type);
                    }
                }
                b'+' => {
                    if trimmed_value.is_empty() && before_sign {
                        before_sign = false;
                    } else {
                        return Self::decimal_parse_error(lexical_value, decimal_type);
                    }
                }
                b'1'..=b'9' => {
                    trimmed_value.push(char::from(char));
                    in_leading_zeros = false;
                    is_zero = false;
                    if in_fraction {
                        has_nonzero_fraction = true;
                        len_at_trailing_zeros = trimmed_value.len();
                    }
                }
                b'0' => {
                    before_sign = false;
                    if !in_leading_zeros {
                        trimmed_value.push('0');
                    }
                }
                b'.' => {
                    if in_fraction {
                        return Self::decimal_parse_error(lexical_value, decimal_type);
                    }
                    if in_leading_zeros {
                        trimmed_value.push('0');
                        in_leading_zeros = false;
                    }
                    in_fraction = true;
                    len_at_trailing_zeros = trimmed_value.len();
                    trimmed_value.push('.');
                }
                _ => {
                    return Self::decimal_parse_error(lexical_value, decimal_type);
                }
            }
        }
        // finally remove trailing zeros, and possibly also "."
        if in_fraction {
            trimmed_value.truncate(len_at_trailing_zeros);
        }

        if !in_fraction {
            // Even a zero fraction is not allowed in integer types
            assert!(!is_zero); // this case would parse as i64 earlier ...

            match (decimal_type, sign_plus) {
                (DecimalType::PositiveInteger, p) | (DecimalType::NonNegativeInteger, p) if !p => {
                    return Self::decimal_parse_error(lexical_value, decimal_type);
                }
                (DecimalType::NegativeInteger, p) | (DecimalType::NonPositiveInteger, p) if p => {
                    return Self::decimal_parse_error(lexical_value, decimal_type);
                }
                _ => {
                    return Ok(AnyDataValue::new_other(
                        trimmed_value,
                        XSD_PREFIX.to_owned() + "integer",
                    ));
                }
            }
        } else if let DecimalType::Decimal = decimal_type {
            if has_nonzero_fraction {
                return Ok(AnyDataValue::new_other(
                    trimmed_value,
                    XSD_PREFIX.to_owned() + "decimal",
                ));
            } else if let Ok(value) = i64::from_str(&trimmed_value) {
                return Ok(AnyDataValue::new_integer_from_i64(value));
            } else if let Ok(value) = u64::from_str(&trimmed_value) {
                return Ok(AnyDataValue::new_integer_from_u64(value));
            } else {
                return Ok(AnyDataValue::new_other(
                    trimmed_value,
                    XSD_PREFIX.to_owned() + "integer",
                ));
            }
        } else {
            return Self::decimal_parse_error(lexical_value, decimal_type);
        }
    }

    /// Returns a [`Result`] to indicate a [`DataValueCreationError`] in processing
    /// the given literal.
    fn decimal_parse_error(
        lexical_value: String,
        decimal_type: DecimalType,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Err(DataValueCreationError::InvalidLexicalValue {
            lexical_value,
            datatype_iri: decimal_type.datatype_iri(),
        })
    }

    /// Return the corresponding [StorageValueT] for this value, under the assumption that
    /// the value is known in the dictionary.
    ///
    /// # Panics
    /// Panics if the data value is of a type that is managed in the dictionary but cannot be found there.
    pub(crate) fn to_storage_value_t(&self, dictionary: &Dict) -> StorageValueT {
        match self.value_domain() {
            ValueDomain::Tuple
            | ValueDomain::UnsignedLong
            | ValueDomain::Boolean
            | ValueDomain::Other
            | ValueDomain::String
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri => {
                let dictionary_id = dictionary
                    .datavalue_to_id(self)
                    .expect("value should be known to the dictionary");
                Self::usize_to_storage_value_t(dictionary_id)
            }
            ValueDomain::Float => StorageValueT::Float(Float::from_number(self.to_f32_unchecked())),
            ValueDomain::Double => {
                StorageValueT::Double(Double::from_number(self.to_f64_unchecked()))
            }
            ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int => StorageValueT::Int64(self.to_i64_unchecked()),
        }
    }

    /// Return a [StorageValueT] that corresponds to this value, adding it to the dictionary if needed.
    pub(crate) fn to_storage_value_t_mut(&self, dictionary: &mut Dict) -> StorageValueT {
        match self.value_domain() {
            ValueDomain::Tuple
            | ValueDomain::UnsignedLong
            | ValueDomain::Boolean
            | ValueDomain::Other
            | ValueDomain::String
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri => {
                // TODO: Do we really need to clone? Typical uses of this method do not need the value after this ...
                let dictionary_id = dictionary.add_datavalue(self.clone()).value();
                Self::usize_to_storage_value_t(dictionary_id)
            }
            ValueDomain::Float => StorageValueT::Float(Float::from_number(self.to_f32_unchecked())),
            ValueDomain::Double => {
                StorageValueT::Double(Double::from_number(self.to_f64_unchecked()))
            }
            ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int => StorageValueT::Int64(self.to_i64_unchecked()),
        }
    }

    /// Convert a usize to a suitably size [StorageValueT].
    fn usize_to_storage_value_t(n: usize) -> StorageValueT {
        if let Ok(u32value) = n.try_into() {
            StorageValueT::Id32(u32value)
        } else {
            StorageValueT::Id64(n.try_into().unwrap())
        }
    }
}

impl DataValue for AnyDataValue {
    delegate! {
        to match self {
            AnyDataValue::Boolean(value) => value,
            AnyDataValue::String(value)=> value,
            AnyDataValue::LanguageTaggedString(value) => value,
            AnyDataValue::Iri(value) => value,
            AnyDataValue::Float(value) => value,
            AnyDataValue::Double(value) => value,
            AnyDataValue::UnsignedLong(value) => value,
            AnyDataValue::Long(value) => value,
            AnyDataValue::Other(value) => value,
        } {
            fn datatype_iri(&self) -> String;
            fn lexical_value(&self) -> String;
            fn value_domain(&self) -> ValueDomain;
            fn canonical_string(&self) -> String;
            fn to_string(&self) -> Option<String>;
            fn to_string_unchecked(&self) -> String;
            fn to_language_tagged_string(&self) -> Option<(String, String)>;
            fn to_language_tagged_string_unchecked(&self) -> (String, String);
            fn to_iri(&self) -> Option<String>;
            fn to_iri_unchecked(&self) -> String;
            fn to_f32(&self) -> Option<f32>;
            fn to_f32_unchecked(&self) -> f32;
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

/// The intended implementation of equality for [`AnyDataValue`] is based on identity in the value space,
/// i.e., we ask whether two values syntactically represent the same elements. In most cases, this agrees with
/// the identity of representations, especially since our values are self-normalizing in the sense that they
/// cannot capture superficial syntactic variations in the writing one and the same element (e.g., `42` vs `+42`).
///
/// This notion of equality is unrelated to any potential equality theory that may be part of the model in some
/// logical models. In particular, even though nulls might conceptually denote the same objects as other terms,
/// the nulls as syntactic elements are still not identitical and therefore will not be considered
/// equal here. It is not intended (and would be impossible) to capture any more complex, possibly inferred
/// equality at this level.
impl PartialEq for AnyDataValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AnyDataValue::String(dv), AnyDataValue::String(dv_other)) => dv == dv_other,
            (AnyDataValue::Iri(dv), AnyDataValue::Iri(dv_other)) => dv == dv_other,
            (
                AnyDataValue::LanguageTaggedString(dv),
                AnyDataValue::LanguageTaggedString(dv_other),
            ) => dv == dv_other,
            (AnyDataValue::Float(dv), AnyDataValue::Float(dv_other)) => dv == dv_other,
            (AnyDataValue::Double(dv), AnyDataValue::Double(dv_other)) => dv == dv_other,
            (AnyDataValue::Long(_), _) => {
                other.fits_into_i64() && other.to_i64_unchecked() == self.to_i64_unchecked()
            }
            (AnyDataValue::UnsignedLong(_), _) => {
                other.fits_into_u64() && other.to_u64_unchecked() == self.to_u64_unchecked()
            }
            (AnyDataValue::Boolean(dv), AnyDataValue::Boolean(dv_other)) => dv == dv_other,
            (AnyDataValue::Other(dv), AnyDataValue::Other(dv_other)) => dv == dv_other,
            _ => false,
        }
    }
}

impl Eq for AnyDataValue {}

/// A dynamically defined iterator over [`AnyDataValue`]s.
#[allow(missing_debug_implementations)]
pub struct AnyDataValueIterator<'a>(pub Box<dyn Iterator<Item = AnyDataValue> + 'a>);

#[cfg(test)]
mod test {
    use super::{AnyDataValue, XSD_PREFIX};
    use crate::datavalues::{DataValue, DataValueCreationError, ValueDomain};

    #[test]
    fn anydatavalue_string() {
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
    fn anydatavalue_iri() {
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
    fn anydatavalue_float() {
        let value = 2.3456e3;
        let dv = AnyDataValue::new_float_from_f32(value).unwrap();

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#float".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::Float);

        assert_eq!(dv.to_f32(), Some(value));
        assert_eq!(dv.to_f64(), None);
        assert_eq!(dv.to_f32_unchecked(), value);
        assert_eq!(dv, AnyDataValue::new_float_from_f32(value).unwrap());
        assert_ne!(dv, AnyDataValue::new_float_from_f32(3.14).unwrap());
        assert_ne!(dv, AnyDataValue::new_double_from_f64(2.3456e3).unwrap());
    }

    #[test]
    fn anydatavalue_double() {
        let value = 2.3456e3;
        let dv = AnyDataValue::new_double_from_f64(value).unwrap();

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#double".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::Double);

        assert_eq!(dv.to_f64(), Some(value));
        assert_eq!(dv.to_f32(), None);
        assert_eq!(dv.to_f64_unchecked(), value);
        assert_eq!(dv, AnyDataValue::new_double_from_f64(value).unwrap());
        assert_ne!(dv, AnyDataValue::new_double_from_f64(3.14).unwrap());
        assert_ne!(dv, AnyDataValue::new_float_from_f32(2.3456e3).unwrap());
    }

    #[test]
    fn anydatavalue_long() {
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

        assert_eq!(dv, AnyDataValue::new_integer_from_i64(42));
        assert_eq!(dv, AnyDataValue::new_integer_from_u64(42));
    }

    #[test]
    fn anydatavalue_unsigned_long() {
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
    fn anydatavalue_lang_string() {
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
    fn anydatavalue_other() {
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
    fn anydatavalue_eq() {
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
    fn anydatavalue_new_from_string_literal() {
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
    fn anydatavalue_new_from_decimal_literal() {
        let res1 = AnyDataValue::new_from_typed_literal(
            "+004398046511104".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res2 = AnyDataValue::new_from_typed_literal(
            "+0043980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res2a = AnyDataValue::new_from_typed_literal(
            "-3.14".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res2b = AnyDataValue::new_from_typed_literal(
            "-.123".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res2c = AnyDataValue::new_from_typed_literal(
            ".123".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res3 = AnyDataValue::new_from_typed_literal(
            "+0043980465oo111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res4 = AnyDataValue::new_from_typed_literal(
            "+000".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res5 = AnyDataValue::new_from_typed_literal(
            "-000".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res6 = AnyDataValue::new_from_typed_literal(
            "+000.00".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res6a = AnyDataValue::new_from_typed_literal(
            "18446744073709551574.00".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res6b = AnyDataValue::new_from_typed_literal(
            "+1844674407370955157418446744073709551574.0000".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res7a = AnyDataValue::new_from_typed_literal(
            "00+03".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res7b = AnyDataValue::new_from_typed_literal(
            ".+03".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );
        let res7c = AnyDataValue::new_from_typed_literal(
            "0.0.1".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );

        assert_eq!(res1, Ok(AnyDataValue::new_integer_from_i64(4398046511104)));
        assert_eq!(
            res2,
            Ok(AnyDataValue::new_other(
                "43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
        assert_eq!(
            res2a,
            Ok(AnyDataValue::new_other(
                "-3.14".to_string(),
                XSD_PREFIX.to_owned() + "decimal"
            ))
        );
        assert_eq!(
            res2b,
            Ok(AnyDataValue::new_other(
                "-0.123".to_string(),
                XSD_PREFIX.to_owned() + "decimal"
            ))
        );
        assert_eq!(
            res2c,
            Ok(AnyDataValue::new_other(
                "0.123".to_string(),
                XSD_PREFIX.to_owned() + "decimal"
            ))
        );
        assert!(matches!(
            res3,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(res4, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert_eq!(res5, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert_eq!(res6, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert_eq!(
            res6a,
            Ok(AnyDataValue::new_integer_from_u64(18446744073709551574))
        );
        assert_eq!(
            res6b,
            Ok(AnyDataValue::new_other(
                "1844674407370955157418446744073709551574".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
        assert!(matches!(
            res7a,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert!(matches!(
            res7b,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert!(matches!(
            res7c,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
    }

    #[test]
    fn anydatavalue_new_from_integer_literal() {
        let res1 = AnyDataValue::new_from_typed_literal(
            "+004398046511104".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );
        let res2 = AnyDataValue::new_from_typed_literal(
            "+0043980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );
        let res3 = AnyDataValue::new_from_typed_literal(
            "+0043980465oo111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );
        let res4 = AnyDataValue::new_from_typed_literal(
            "+000".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );
        let res5 = AnyDataValue::new_from_typed_literal(
            "-000".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );
        let res6 = AnyDataValue::new_from_typed_literal(
            "+000.00".to_string(),
            XSD_PREFIX.to_owned() + "integer",
        );

        assert_eq!(res1, Ok(AnyDataValue::new_integer_from_i64(4398046511104)));
        assert_eq!(
            res2,
            Ok(AnyDataValue::new_other(
                "43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
        assert!(matches!(
            res3,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
        assert_eq!(res4, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert_eq!(res5, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert!(matches!(
            res6,
            Err(DataValueCreationError::IntegerNotParsed { .. })
        ));
    }

    #[test]
    fn anydatavalue_new_from_non_negative_integer_literal() {
        let res_minus = AnyDataValue::new_from_typed_literal(
            "-10".to_string(),
            XSD_PREFIX.to_owned() + "nonNegativeInteger",
        );
        let res_zero = AnyDataValue::new_from_typed_literal(
            "0".to_string(),
            XSD_PREFIX.to_owned() + "nonNegativeInteger",
        );
        let res_plus = AnyDataValue::new_from_typed_literal(
            "10".to_string(),
            XSD_PREFIX.to_owned() + "nonNegativeInteger",
        );

        let res_minus2 = AnyDataValue::new_from_typed_literal(
            "-43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "nonNegativeInteger",
        );
        let res_plus2 = AnyDataValue::new_from_typed_literal(
            "43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "nonNegativeInteger",
        );

        assert!(matches!(
            res_minus,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(res_zero, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert_eq!(res_plus, Ok(AnyDataValue::new_integer_from_i64(10)));
        assert!(matches!(
            res_minus2,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(
            res_plus2,
            Ok(AnyDataValue::new_other(
                "43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
    }

    #[test]
    fn anydatavalue_new_from_positive_integer_literal() {
        let res_minus = AnyDataValue::new_from_typed_literal(
            "-10".to_string(),
            XSD_PREFIX.to_owned() + "positiveInteger",
        );
        let res_zero = AnyDataValue::new_from_typed_literal(
            "0".to_string(),
            XSD_PREFIX.to_owned() + "positiveInteger",
        );
        let res_plus = AnyDataValue::new_from_typed_literal(
            "10".to_string(),
            XSD_PREFIX.to_owned() + "positiveInteger",
        );

        let res_minus2 = AnyDataValue::new_from_typed_literal(
            "-43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "positiveInteger",
        );
        let res_plus2 = AnyDataValue::new_from_typed_literal(
            "43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "positiveInteger",
        );

        assert!(matches!(
            res_minus,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert!(matches!(
            res_zero,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(res_plus, Ok(AnyDataValue::new_integer_from_i64(10)));
        assert!(matches!(
            res_minus2,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(
            res_plus2,
            Ok(AnyDataValue::new_other(
                "43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
    }

    #[test]
    fn anydatavalue_new_from_non_positive_integer_literal() {
        let res_minus = AnyDataValue::new_from_typed_literal(
            "-10".to_string(),
            XSD_PREFIX.to_owned() + "nonPositiveInteger",
        );
        let res_zero = AnyDataValue::new_from_typed_literal(
            "0".to_string(),
            XSD_PREFIX.to_owned() + "nonPositiveInteger",
        );
        let res_plus = AnyDataValue::new_from_typed_literal(
            "10".to_string(),
            XSD_PREFIX.to_owned() + "nonPositiveInteger",
        );

        let res_minus2 = AnyDataValue::new_from_typed_literal(
            "-43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "nonPositiveInteger",
        );
        let res_plus2 = AnyDataValue::new_from_typed_literal(
            "43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "nonPositiveInteger",
        );

        assert_eq!(res_minus, Ok(AnyDataValue::new_integer_from_i64(-10)));
        assert_eq!(res_zero, Ok(AnyDataValue::new_integer_from_i64(0)));
        assert!(matches!(
            res_plus,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(
            res_minus2,
            Ok(AnyDataValue::new_other(
                "-43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
        assert!(matches!(
            res_plus2,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
    }

    #[test]
    fn anydatavalue_new_from_negative_integer_literal() {
        let res_minus = AnyDataValue::new_from_typed_literal(
            "-10".to_string(),
            XSD_PREFIX.to_owned() + "negativeInteger",
        );
        let res_zero = AnyDataValue::new_from_typed_literal(
            "0".to_string(),
            XSD_PREFIX.to_owned() + "negativeInteger",
        );
        let res_plus = AnyDataValue::new_from_typed_literal(
            "10".to_string(),
            XSD_PREFIX.to_owned() + "negativeInteger",
        );

        let res_minus2 = AnyDataValue::new_from_typed_literal(
            "-43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "negativeInteger",
        );
        let res_plus2 = AnyDataValue::new_from_typed_literal(
            "43980465111044398046511104".to_string(),
            XSD_PREFIX.to_owned() + "negativeInteger",
        );

        assert_eq!(res_minus, Ok(AnyDataValue::new_integer_from_i64(-10)));
        assert!(matches!(
            res_zero,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert!(matches!(
            res_plus,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
        assert_eq!(
            res_minus2,
            Ok(AnyDataValue::new_other(
                "-43980465111044398046511104".to_string(),
                XSD_PREFIX.to_owned() + "integer"
            ))
        );
        assert!(matches!(
            res_plus2,
            Err(DataValueCreationError::InvalidLexicalValue { .. })
        ));
    }

    #[test]
    fn anydatavalue_new_from_long_literal() {
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
    fn anydatavalue_new_from_int_literal() {
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
    fn anydatavalue_new_from_short_literal() {
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
    fn anydatavalue_new_from_byte_literal() {
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
    fn anydatavalue_new_from_ulong_literal() {
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
    fn anydatavalue_new_from_uint_literal() {
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
    fn anydatavalue_new_from_ushort_literal() {
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
    fn anydatavalue_new_from_ubyte_literal() {
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
    fn anydatavalue_new_from_double_literal() {
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
