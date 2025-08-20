//! This module provides implementations [DataValue]s that can represent any datavalue that we support.

use std::{num::IntErrorKind, str::FromStr};

use delegate::delegate;

use crate::{
    datatypes::{Double, Float, StorageValueT},
    dictionary::{AddResult, DvDict},
    management::database::Dict,
};

use super::{
    boolean_datavalue::BooleanDataValue, errors::InternalDataValueCreationError,
    float_datavalues::FloatDataValue, syntax::XSD_PREFIX, DataValue, DataValueCreationError,
    DoubleDataValue, IriDataValue, LangStringDataValue, LongDataValue, MapDataValue, NullDataValue,
    OtherDataValue, StringDataValue, TupleDataValue, UnsignedLongDataValue, ValueDomain,
};

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

/// Enum that can represent arbitrary [DataValue]s.
#[derive(Debug, Clone)]
enum AnyDataValueEnum {
    /// Variant for representing [DataValue]s in [ValueDomain::PlainString].
    PlainString(StringDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::LanguageTaggedString].
    LanguageTaggedString(LangStringDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Iri].
    Iri(IriDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Float].
    Float(FloatDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Double].
    Double(DoubleDataValue),
    /// Variant for representing [DataValue]s that represent numbers in the u64 range.
    /// Note that this has some overlap with values of [AnyDataValueEnum::Long], which is accounted
    /// for in the [Eq] implementation.
    UnsignedLong(UnsignedLongDataValue),
    /// Variant for representing [DataValue]s that represent numbers in the i64 range.
    /// Note that this has some overlap with values of [AnyDataValueEnum::UnsignedLong], which is accounted
    /// for in the [Eq] implementation.
    Long(LongDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Boolean].
    Boolean(BooleanDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Null].
    Null(NullDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Tuple].
    Tuple(TupleDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Map].
    Map(MapDataValue),
    /// Variant for representing [DataValue]s in [ValueDomain::Other].
    Other(OtherDataValue),
}

/// Type that can represent arbitrary [DataValue]s.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct AnyDataValue(AnyDataValueEnum);

impl AnyDataValue {
    /// Construct a datavalue that represents the given number.
    pub fn new_integer_from_i64(value: i64) -> Self {
        AnyDataValue(AnyDataValueEnum::Long(LongDataValue::new(value)))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_integer_from_u64(value: u64) -> Self {
        AnyDataValue(AnyDataValueEnum::UnsignedLong(UnsignedLongDataValue::new(
            value,
        )))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_float_from_f32(value: f32) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue(AnyDataValueEnum::Float(
            FloatDataValue::from_f32(value)?,
        )))
    }

    /// Construct a datavalue that represents the given number.
    pub fn new_double_from_f64(value: f64) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue(AnyDataValueEnum::Double(
            DoubleDataValue::from_f64(value)?,
        )))
    }

    /// Construct a datavalue in [ValueDomain::PlainString] that represents the given string.
    pub fn new_plain_string(value: String) -> Self {
        AnyDataValue(AnyDataValueEnum::PlainString(StringDataValue::new(value)))
    }

    /// Construct a datavalue in [ValueDomain::Iri] that represents the given IRI.
    pub fn new_iri(value: String) -> Self {
        AnyDataValue(AnyDataValueEnum::Iri(IriDataValue::new(value)))
    }

    /// Construct a datavalue in [ValueDomain::LanguageTaggedString] that represents a string with a language tag.
    pub fn new_language_tagged_string(value: String, lang_tag: String) -> Self {
        AnyDataValue(AnyDataValueEnum::LanguageTaggedString(
            LangStringDataValue::new(value, lang_tag),
        ))
    }

    /// Construct a datavalue in [ValueDomain::Boolean].
    pub fn new_boolean(value: bool) -> Self {
        AnyDataValue(AnyDataValueEnum::Boolean(BooleanDataValue::new(value)))
    }

    /// Construct a datavalue in [ValueDomain::Other] specified by the given lexical value and datatype IRI.
    pub fn new_other(lexical_value: String, datatype_iri: String) -> Self {
        AnyDataValue(AnyDataValueEnum::Other(OtherDataValue::new(
            lexical_value,
            datatype_iri,
        )))
    }

    /// Construct a datavalue from its physical representation as a [StorageValueT], using
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
                    Err(DataValueCreationError::InternalError(
                        InternalDataValueCreationError::DictionaryIdNotFound(
                            usize::try_from(id).unwrap(),
                        )
                        .to_string(),
                    ))
                }
            }
            StorageValueT::Id64(id) => {
                if let Some(value) = dict.id_to_datavalue(usize::try_from(id).unwrap()) {
                    Ok(value)
                } else {
                    Err(DataValueCreationError::InternalError(
                        InternalDataValueCreationError::DictionaryIdNotFound(
                            usize::try_from(id).unwrap(),
                        )
                        .to_string(),
                    ))
                }
            }
            StorageValueT::Int64(num) => Ok(AnyDataValue::new_integer_from_i64(num)),
            StorageValueT::Float(num) => Ok(AnyDataValue::new_float_from_f32(num.into()).unwrap()),
            StorageValueT::Double(num) => {
                Ok(AnyDataValue::new_double_from_f64(num.into()).unwrap())
            }
        }
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] from the given lexical value and datatype IRI.
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
                        $(if !($min..=$max).contains(&value) {
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
                "string" => Ok(Self::new_plain_string(lexical_value)),
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
                    _ => Err(DataValueCreationError::BooleanNotParsed { lexical_value }),
                },
                _ => Ok(Self::new_other(lexical_value, datatype_iri)),
            }
        } else {
            Ok(Self::new_other(lexical_value, datatype_iri))
        }
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] from the given string representation
    /// of a (arbitrarily large) integer.
    pub fn new_from_integer_literal(
        lexical_value: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Self::new_from_decimal_type_literal(lexical_value, DecimalType::Integer)
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] from the given string representation
    /// of a double precision floating-point number.
    pub fn new_from_double_literal(
        lexical_value: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        match f64::from_str(&lexical_value) {
            Ok(value) => Self::new_double_from_f64(value),
            Err(e) => Err(DataValueCreationError::from(e)),
        }
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] from the given string representation
    /// of a decimal number. The result can be an integer, if the decimal digits are omitted or zero.
    pub fn new_from_decimal_literal(
        lexical_value: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Self::new_from_decimal_type_literal(lexical_value, DecimalType::Decimal)
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] for a "big decimal"
    /// or "big integer" [DecimalType].
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

    /// Create [AnyDataValue] for the given i64 number after checking possible range
    /// constraints of the given [DecimalType].
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
                Self::decimal_parse_error(lexical_value, decimal_type)
            }
            (DecimalType::NegativeInteger, v) if v >= 0 => {
                Self::decimal_parse_error(lexical_value, decimal_type)
            }
            (DecimalType::NonPositiveInteger, v) if v > 0 => {
                Self::decimal_parse_error(lexical_value, decimal_type)
            }
            _ => Ok(Self::new_integer_from_i64(value)),
        }
    }

    /// Construct a normalized datavalue in an appropriate [ValueDomain] for a "big decimal"
    /// or "big integer" [DecimalType] by manually parsing the string.
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

        let mut saw_digit = false;

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
                    saw_digit = true;
                    trimmed_value.push(char::from(char));
                    in_leading_zeros = false;
                    is_zero = false;
                    if in_fraction {
                        has_nonzero_fraction = true;
                        len_at_trailing_zeros = trimmed_value.len();
                    }
                }
                b'0' => {
                    saw_digit = true;
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

        // If we didn't see any digits then this is not a decimal
        if !saw_digit {
            return Self::decimal_parse_error(lexical_value, decimal_type);
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
                    Self::decimal_parse_error(lexical_value, decimal_type)
                }
                (DecimalType::NegativeInteger, p) | (DecimalType::NonPositiveInteger, p) if p => {
                    Self::decimal_parse_error(lexical_value, decimal_type)
                }
                _ => Ok(AnyDataValue::new_other(
                    trimmed_value,
                    XSD_PREFIX.to_owned() + "integer",
                )),
            }
        } else if let DecimalType::Decimal = decimal_type {
            if has_nonzero_fraction {
                Ok(AnyDataValue::new_other(
                    trimmed_value,
                    XSD_PREFIX.to_owned() + "decimal",
                ))
            } else if let Ok(value) = i64::from_str(&trimmed_value) {
                Ok(AnyDataValue::new_integer_from_i64(value))
            } else if let Ok(value) = u64::from_str(&trimmed_value) {
                Ok(AnyDataValue::new_integer_from_u64(value))
            } else {
                Ok(AnyDataValue::new_other(
                    trimmed_value,
                    XSD_PREFIX.to_owned() + "integer",
                ))
            }
        } else {
            Self::decimal_parse_error(lexical_value, decimal_type)
        }
    }

    /// Returns a [Result] to indicate a [DataValueCreationError] in processing
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

    /// Return the corresponding [StorageValueT] for this value.
    /// Returns `None` if this value is unknown to the dictionary.
    ///
    /// # Panics
    /// Panics if the data value is of a type that is managed in the dictionary but cannot be found there.
    pub(crate) fn try_to_storage_value_t(&self, dictionary: &Dict) -> Option<StorageValueT> {
        Some(match self.value_domain() {
            ValueDomain::Tuple
            | ValueDomain::Map
            | ValueDomain::UnsignedLong
            | ValueDomain::Boolean
            | ValueDomain::Other
            | ValueDomain::PlainString
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri
            | ValueDomain::Null => {
                let dictionary_id = dictionary.datavalue_to_id(self)?;
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
        })
    }

    /// Return the corresponding [StorageValueT] for this value, under the assumption that
    /// the value is known in the dictionary.
    ///
    /// # Panics
    /// Panics if the data value is of a type that is managed in the dictionary but cannot be found there.
    pub(crate) fn to_storage_value_t(&self, dictionary: &Dict) -> StorageValueT {
        self.try_to_storage_value_t(dictionary)
            .expect("Function assumes that value is known by the dictionary")
    }

    /// Return a [StorageValueT] that corresponds to this value, adding it to the dictionary if needed.
    ///
    /// # Panics
    ///
    /// When called on a value in domain [ValueDomain::Null] that is not already present in the dictionary.
    /// The correct process in this case is to use the dictionary to create any null value on which this
    /// method will later be called. It is not possible to newly create a dictionary id for an arbitrary
    /// null value (in such a way that the same ID will be returned if an equal null value is converted).
    pub(crate) fn to_storage_value_t_dict(&self, dictionary: &mut Dict) -> StorageValueT {
        match self.value_domain() {
            ValueDomain::Tuple
            | ValueDomain::Map
            | ValueDomain::UnsignedLong
            | ValueDomain::Boolean
            | ValueDomain::Other
            | ValueDomain::PlainString
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Iri => {
                // TODO: Do we really need to clone? At least if the value is known, it should not be necessary. Maybe move into add method?
                let add_result = dictionary.add_datavalue(self.clone());

                if add_result == AddResult::Rejected {
                    panic!("Dictionary rejected the data value: {self:?}");
                }

                let dictionary_id = add_result.value();
                Self::usize_to_storage_value_t(dictionary_id)
            }
            ValueDomain::Null => {
                let dictionary_id = dictionary
                    .datavalue_to_id(self)
                    .expect("cannot create specific nulls: value must be known to the dictionary");
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

    /// Return the internal id of a [NullDataValue] for an [AnyDataValue] with
    /// [ValueDomain::Null].
    ///
    /// # Panics
    ///
    /// If the value domain is not [ValueDomain::Null].
    pub fn null_id_unchecked(&self) -> usize {
        if let AnyDataValueEnum::Null(ndv) = self.0 {
            ndv.id()
        } else {
            panic!("not a null value");
        }
    }

    /// Check whether the value represents a positive number
    pub fn is_positive_number(&self) -> bool {
        match &self.0 {
            AnyDataValueEnum::Long(value) => value.to_i64_unchecked() > 0,
            AnyDataValueEnum::UnsignedLong(value) => value.to_i64_unchecked() > 0,
            AnyDataValueEnum::Float(value) => value.to_f32_unchecked() > 0.,
            AnyDataValueEnum::Double(value) => value.to_i64_unchecked() > 0,
            _ => false,
        }
    }
}

impl DataValue for AnyDataValue {
    delegate! {
        to match &self.0 {
            AnyDataValueEnum::Boolean(value) => value,
            AnyDataValueEnum::PlainString(value)=> value,
            AnyDataValueEnum::LanguageTaggedString(value) => value,
            AnyDataValueEnum::Iri(value) => value,
            AnyDataValueEnum::Float(value) => value,
            AnyDataValueEnum::Double(value) => value,
            AnyDataValueEnum::UnsignedLong(value) => value,
            AnyDataValueEnum::Long(value) => value,
            AnyDataValueEnum::Null(value) => value,
            AnyDataValueEnum::Tuple(value) => value,
            AnyDataValueEnum::Map(value) => value,
            AnyDataValueEnum::Other(value) => value,
        } {
            fn datatype_iri(&self) -> String;
            fn lexical_value(&self) -> String;
            fn value_domain(&self) -> ValueDomain;
            fn canonical_string(&self) -> String;
            fn to_plain_string(&self) -> Option<String>;
            fn to_plain_string_unchecked(&self) -> String;
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
            fn to_boolean(&self) -> Option<bool>;
            fn to_boolean_unchecked(&self) -> bool;
            fn to_null(&self) -> Option<NullDataValue>;
            fn to_null_unchecked(&self) -> NullDataValue;
            fn label(&self) -> Option<&IriDataValue>;
            fn length(&self) -> Option<usize>;
            fn len_unchecked(&self) -> usize;
            fn tuple_element_unchecked(&self, index: usize) -> &AnyDataValue;
            fn map_keys(&self) -> Option<Box<dyn Iterator<Item = &AnyDataValue> + '_>>;
            fn map_element_unchecked(&self, key: &AnyDataValue) -> &AnyDataValue;
            }
    }
}

impl std::hash::Hash for AnyDataValue {
    delegate! {
        to match &self.0 {
            AnyDataValueEnum::Boolean(value) => value,
            AnyDataValueEnum::PlainString(value)=> value,
            AnyDataValueEnum::LanguageTaggedString(value) => value,
            AnyDataValueEnum::Iri(value) => value,
            AnyDataValueEnum::Float(value) => value,
            AnyDataValueEnum::Double(value) => value,
            AnyDataValueEnum::UnsignedLong(value) => value,
            AnyDataValueEnum::Long(value) => value,
            AnyDataValueEnum::Null(value) => value,
            AnyDataValueEnum::Tuple(value) => value,
            AnyDataValueEnum::Map(value) => value,
            AnyDataValueEnum::Other(value) => value,
        } {
            /// The hash function for [AnyDataValue] is delegated to the contained value
            /// implementations, without adding variant-specific information as the
            /// derived implementation would do. This ensures that different implementations
            /// that have the potential to represent the same value can co-exist in the
            /// enum, while respecting the Hash-Eq-Contract.
            fn hash<H: std::hash::Hasher>(&self, state: &mut H);
        }
    }
}

impl std::fmt::Display for AnyDataValue {
    delegate! {
        to match &self.0 {
            AnyDataValueEnum::Boolean(value) => value,
            AnyDataValueEnum::PlainString(value)=> value,
            AnyDataValueEnum::LanguageTaggedString(value) => value,
            AnyDataValueEnum::Iri(value) => value,
            AnyDataValueEnum::Float(value) => value,
            AnyDataValueEnum::Double(value) => value,
            AnyDataValueEnum::UnsignedLong(value) => value,
            AnyDataValueEnum::Long(value) => value,
            AnyDataValueEnum::Null(value) => value,
            AnyDataValueEnum::Tuple(value) => value,
            AnyDataValueEnum::Map(value) => value,
            AnyDataValueEnum::Other(value) => value,
        } {
            /// The fmt function for [AnyDataValue] is delegated to the contained value
            /// implementations, without adding variant-specific information as the
            /// derived implementation would do.
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
        }
    }
}

/// The intended implementation of equality for [AnyDataValue] is based on identity in the value space,
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
        match (&self.0, &other.0) {
            (AnyDataValueEnum::PlainString(dv), AnyDataValueEnum::PlainString(dv_other)) => {
                dv == dv_other
            }
            (AnyDataValueEnum::Iri(dv), AnyDataValueEnum::Iri(dv_other)) => dv == dv_other,
            (
                AnyDataValueEnum::LanguageTaggedString(dv),
                AnyDataValueEnum::LanguageTaggedString(dv_other),
            ) => dv == dv_other,
            (AnyDataValueEnum::Float(dv), AnyDataValueEnum::Float(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Double(dv), AnyDataValueEnum::Double(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Long(_), _) => {
                other.fits_into_i64() && other.to_i64_unchecked() == self.to_i64_unchecked()
            }
            (AnyDataValueEnum::UnsignedLong(_), _) => {
                other.fits_into_u64() && other.to_u64_unchecked() == self.to_u64_unchecked()
            }
            (AnyDataValueEnum::Boolean(dv), AnyDataValueEnum::Boolean(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Null(dv), AnyDataValueEnum::Null(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Tuple(dv), AnyDataValueEnum::Tuple(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Map(dv), AnyDataValueEnum::Map(dv_other)) => dv == dv_other,
            (AnyDataValueEnum::Other(dv), AnyDataValueEnum::Other(dv_other)) => dv == dv_other,
            _ => false,
        }
    }
}

impl Eq for AnyDataValue {}

impl Ord for AnyDataValue {
    /// Compare two arbitrary data values. The ordering is dominated by the [ValueDomain] of
    /// the values. If this is equal, the actual values are compared.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let dom_order = self.value_domain().cmp(&other.value_domain());
        if let std::cmp::Ordering::Equal = dom_order {
            match (&self.0, &other.0) {
                (AnyDataValueEnum::PlainString(dv), AnyDataValueEnum::PlainString(dv_other)) => {
                    dv.cmp(dv_other)
                }
                (AnyDataValueEnum::Iri(dv), AnyDataValueEnum::Iri(dv_other)) => dv.cmp(dv_other),
                (
                    AnyDataValueEnum::LanguageTaggedString(dv),
                    AnyDataValueEnum::LanguageTaggedString(dv_other),
                ) => dv.cmp(dv_other),
                (AnyDataValueEnum::Float(dv), AnyDataValueEnum::Float(dv_other)) => {
                    dv.cmp(dv_other)
                }
                (AnyDataValueEnum::Double(dv), AnyDataValueEnum::Double(dv_other)) => {
                    dv.cmp(dv_other)
                }
                (AnyDataValueEnum::Long(_), _) => {
                    self.to_i64_unchecked().cmp(&other.to_i64_unchecked())
                }
                (AnyDataValueEnum::UnsignedLong(_), _) => {
                    self.to_u64_unchecked().cmp(&other.to_u64_unchecked())
                }
                (AnyDataValueEnum::Boolean(dv), AnyDataValueEnum::Boolean(dv_other)) => {
                    dv.cmp(dv_other)
                }
                (AnyDataValueEnum::Null(dv), AnyDataValueEnum::Null(dv_other)) => dv.cmp(dv_other),
                (AnyDataValueEnum::Tuple(dv), AnyDataValueEnum::Tuple(dv_other)) => {
                    dv.cmp(dv_other)
                }
                (AnyDataValueEnum::Map(dv), AnyDataValueEnum::Map(dv_other)) => dv.cmp(dv_other),
                (AnyDataValueEnum::Other(dv), AnyDataValueEnum::Other(dv_other)) => {
                    dv.cmp(dv_other)
                }
                _ => unreachable!("no other combination of values can have equal domains"),
            }
        } else {
            dom_order
        }
    }
}

impl PartialOrd for AnyDataValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A dynamically defined iterator over [AnyDataValue]s.
#[allow(missing_debug_implementations)]
pub struct AnyDataValueIterator<'a>(pub Box<dyn Iterator<Item = AnyDataValue> + 'a>);

impl From<BooleanDataValue> for AnyDataValue {
    fn from(value: BooleanDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Boolean(value))
    }
}

impl From<FloatDataValue> for AnyDataValue {
    fn from(value: FloatDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Float(value))
    }
}

impl From<DoubleDataValue> for AnyDataValue {
    fn from(value: DoubleDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Double(value))
    }
}

impl From<LongDataValue> for AnyDataValue {
    fn from(value: LongDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Long(value))
    }
}

impl From<UnsignedLongDataValue> for AnyDataValue {
    fn from(value: UnsignedLongDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::UnsignedLong(value))
    }
}

impl From<IriDataValue> for AnyDataValue {
    fn from(value: IriDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Iri(value))
    }
}

impl From<LangStringDataValue> for AnyDataValue {
    fn from(value: LangStringDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::LanguageTaggedString(value))
    }
}

impl From<NullDataValue> for AnyDataValue {
    fn from(value: NullDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Null(value))
    }
}

impl From<OtherDataValue> for AnyDataValue {
    fn from(value: OtherDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Other(value))
    }
}

impl From<StringDataValue> for AnyDataValue {
    fn from(value: StringDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::PlainString(value))
    }
}

impl From<TupleDataValue> for AnyDataValue {
    fn from(value: TupleDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Tuple(value))
    }
}

impl From<MapDataValue> for AnyDataValue {
    fn from(value: MapDataValue) -> Self {
        AnyDataValue(AnyDataValueEnum::Map(value))
    }
}

impl TryFrom<AnyDataValue> for IriDataValue {
    type Error = &'static str;

    fn try_from(value: AnyDataValue) -> Result<Self, Self::Error> {
        match value.0 {
            AnyDataValueEnum::Iri(idv) => Ok(idv),
            _ => Err("cannot convert this data value to IriDataValue"),
        }
    }
}

#[cfg(test)]
mod test {
    use hashbrown::HashSet;

    use super::{AnyDataValue, XSD_PREFIX};
    use crate::datavalues::{
        any_datavalue::AnyDataValueEnum, DataValue, DataValueCreationError, UnsignedLongDataValue,
        ValueDomain,
    };
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    #[test]
    fn anydatavalue_string() {
        let value = "Hello world";
        let dv = AnyDataValue::new_plain_string(value.to_string());

        assert_eq!(dv.lexical_value(), value.to_string());
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::PlainString);

        assert_eq!(dv.to_plain_string(), Some(value.to_string()));
        assert_eq!(dv.to_plain_string_unchecked(), value.to_string());
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
        assert_ne!(dv, AnyDataValue::new_float_from_f32(3.41).unwrap());
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
        assert_ne!(dv, AnyDataValue::new_double_from_f64(3.41).unwrap());
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

        assert!(dv.fits_into_i32());
        assert!(dv.fits_into_u32());
        assert!(dv.fits_into_i64());
        assert!(dv.fits_into_u64());

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

        assert!(!dv.fits_into_i32());
        assert!(!dv.fits_into_u32());
        assert!(!dv.fits_into_i64());
        assert!(dv.fits_into_u64());

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

        assert_eq!(dv.lexical_value(), "Hello world@en-gb");
        assert_eq!(
            dv.datatype_iri(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_string()
        );
        assert_eq!(dv.value_domain(), ValueDomain::LanguageTaggedString);

        assert_eq!(
            dv.to_language_tagged_string(),
            Some((value.to_string(), lang.to_ascii_lowercase()))
        );
        assert_eq!(
            dv.to_language_tagged_string_unchecked(),
            (value.to_string(), lang.to_ascii_lowercase())
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

        assert_eq!(dv.to_plain_string(), None);
        assert_eq!(dv.to_iri(), None);
        assert_eq!(dv.to_language_tagged_string(), None);
        // ...
    }

    fn hash(dv: AnyDataValue) -> u64 {
        let mut hasher = DefaultHasher::new();
        dv.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn anydatavalue_eq() {
        let dv_f64 = AnyDataValue::new_double_from_f64(42.0).unwrap();
        let dv_i64 = AnyDataValue::new_integer_from_i64(42);
        let dv_u64 = AnyDataValue::new_integer_from_u64(42);
        let dv_u64_2 = AnyDataValue::new_integer_from_u64(43);
        let dv_string = AnyDataValue::new_plain_string("42".to_string());
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

        // Check that equality among hashes is the same as equality
        // among values. We expect that there are no hash collisions here.
        let dv_f64_hash = hash(dv_f64);
        let dv_i64_hash = hash(dv_i64);
        let dv_u64_hash = hash(dv_u64);
        let dv_u64_2_hash = hash(dv_u64_2);
        let dv_string_hash = hash(dv_string);
        let dv_iri_hash = hash(dv_iri);
        let dv_lang_string_hash = hash(dv_lang_string);
        let dv_other_hash = hash(dv_other);

        assert_eq!(dv_i64_hash, dv_u64_hash);
        let vec = [
            dv_f64_hash,
            dv_i64_hash,
            dv_u64_2_hash,
            dv_string_hash,
            dv_iri_hash,
            dv_lang_string_hash,
            dv_other_hash,
        ];
        let set: HashSet<_> = vec.iter().collect();
        assert_eq!(set.len(), 7);
    }

    /// Verify that hashes of similar values from different value domains are still different.
    /// This is not needed for the contract of Hash, but makes sense to avoid collissions.
    #[test]
    fn anydatavalue_hash_distinct() {
        let dv_f32 = AnyDataValue::new_float_from_f32(42.0).unwrap();
        let dv_f64 = AnyDataValue::new_double_from_f64(42.0).unwrap();
        let ulong_value = (42.0_f64).to_bits();
        let dv_u64 = AnyDataValue::new_integer_from_u64(ulong_value);

        let f32_hash = hash(dv_f32);
        let f64_hash = hash(dv_f64);
        let u64_hash = hash(dv_u64);

        assert_ne!(f32_hash, f64_hash);
        assert_ne!(f32_hash, u64_hash);
        assert_ne!(f64_hash, u64_hash);
    }

    #[test]
    fn anydatavalue_new_from_string_literal() {
        let string_res = AnyDataValue::new_from_typed_literal(
            "Hello World.".to_string(),
            XSD_PREFIX.to_owned() + "string",
        );

        assert_eq!(
            string_res,
            Ok(AnyDataValue::new_plain_string("Hello World.".to_string()))
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

    #[test]
    fn create_unsigned_long() {
        let dv = AnyDataValue::new_from_typed_literal(
            "+13000000000000000000.0".to_string(),
            XSD_PREFIX.to_owned() + "decimal",
        );

        assert_eq!(
            dv,
            Ok(AnyDataValue(AnyDataValueEnum::UnsignedLong(
                UnsignedLongDataValue::new(13000000000000000000)
            )))
        );
    }
}
