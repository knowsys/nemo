use std::num::ParseIntError;

use num::FromPrimitive;

use nemo_physical::datatypes::data_value::PhysicalString;
use nemo_physical::datatypes::Double;
use nemo_physical::error::ReadingError;
use nemo_physical::{
    datatypes::data_value::DataValueIteratorT, dictionary::value_serializer::NULL_PREFIX,
};

use crate::model::{Identifier, NumericLiteral, RdfLiteral, Term};

use super::{error::InvalidRuleTermConversion, primitive_types::PrimitiveType};

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

const LANGUAGE_STRING_PREFIX: &str = "LANGUAGE_STRING:";
const STRING_PREFIX: &str = "STRING:";
const INTEGER_PREFIX: &str = "INTEGER:";
const DECIMAL_PREFIX: &str = "DECIMAL:";
const DOUBLE_PREFIX: &str = "DOUBLE:";
const CONSTANT_PREFIX: &str = "CONSTANT:";
const DATATYPE_VALUE_PREFIX: &str = "DATATYPE_VALUE:";

/// The prefix used to indicate constants that are Nulls
pub const LOGICAL_NULL_PREFIX: &str = "__Null#";

/// An Api wrapper fot the logical string type
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalString(String);

impl From<String> for LogicalString {
    fn from(value: String) -> Self {
        LogicalString(value)
    }
}

impl From<LogicalString> for String {
    fn from(value: LogicalString) -> Self {
        value.0
    }
}

impl<'a> From<&'a LogicalString> for &'a str {
    fn from(value: &'a LogicalString) -> Self {
        &value.0
    }
}

impl std::fmt::Display for LogicalString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An Api wrapper fot the logical integer type
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LogicalInteger(i64);

impl From<i64> for LogicalInteger {
    fn from(value: i64) -> Self {
        LogicalInteger(value)
    }
}

impl From<LogicalInteger> for i64 {
    fn from(value: LogicalInteger) -> Self {
        value.0
    }
}

impl std::fmt::Display for LogicalInteger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An Api wrapper fot the logical float64 type
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LogicalFloat64(Double);

impl From<Double> for LogicalFloat64 {
    fn from(value: Double) -> Self {
        LogicalFloat64(value)
    }
}

impl From<LogicalFloat64> for Double {
    fn from(value: LogicalFloat64) -> Self {
        value.0
    }
}

impl std::fmt::Display for LogicalFloat64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LanguageString(String, String);

#[derive(Clone, Debug, PartialEq, Eq)]
struct DatatypeValue(String, String);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Decimal(i64, u64);

impl From<LogicalString> for Term {
    fn from(value: LogicalString) -> Self {
        Self::StringLiteral(value.into())
    }
}

impl From<LogicalInteger> for Term {
    fn from(value: LogicalInteger) -> Self {
        Self::NumericLiteral(NumericLiteral::Integer(value.into()))
    }
}

impl From<LogicalFloat64> for Term {
    fn from(value: LogicalFloat64) -> Self {
        Self::NumericLiteral(NumericLiteral::Double(value.into()))
    }
}

impl From<LanguageString> for PhysicalString {
    fn from(value: LanguageString) -> Self {
        format!("{LANGUAGE_STRING_PREFIX}{}@{}", value.0, value.1).into()
    }
}

impl From<LogicalString> for PhysicalString {
    fn from(value: LogicalString) -> Self {
        format!("{STRING_PREFIX}{value}").into()
    }
}

impl From<LogicalInteger> for PhysicalString {
    fn from(value: LogicalInteger) -> Self {
        format!("{INTEGER_PREFIX}{value}").into()
    }
}

impl From<Decimal> for PhysicalString {
    fn from(value: Decimal) -> Self {
        format!("{DECIMAL_PREFIX}{}.{}", value.0, value.1).into()
    }
}

impl From<LogicalFloat64> for PhysicalString {
    fn from(value: LogicalFloat64) -> Self {
        format!("{DOUBLE_PREFIX}{value}").into()
    }
}

impl From<Identifier> for PhysicalString {
    fn from(value: Identifier) -> Self {
        format!("{CONSTANT_PREFIX}{value}").into()
    }
}

impl From<DatatypeValue> for PhysicalString {
    fn from(value: DatatypeValue) -> Self {
        format!("{DATATYPE_VALUE_PREFIX}{}^^{}", value.0, value.1).into()
    }
}

impl From<LogicalInteger> for LogicalString {
    fn from(value: LogicalInteger) -> Self {
        value.0.to_string().into()
    }
}

impl From<LogicalFloat64> for LogicalString {
    fn from(value: LogicalFloat64) -> Self {
        value.0.to_string().into()
    }
}

impl TryFrom<LogicalString> for LogicalInteger {
    type Error = ParseIntError;

    fn try_from(value: LogicalString) -> Result<Self, Self::Error> {
        value.0.parse::<i64>().map(|i| i.into())
    }
}

impl TryFrom<LogicalString> for LogicalFloat64 {
    type Error = ReadingError;

    fn try_from(value: LogicalString) -> Result<Self, Self::Error> {
        let parsed = value.0.parse::<f64>()?;
        Double::new(parsed).map(|d| d.into())
    }
}

impl TryFrom<LogicalFloat64> for LogicalInteger {
    type Error = ReadingError;

    fn try_from(value: LogicalFloat64) -> Result<Self, Self::Error> {
        i64::from_f64(value.0.into())
            .map(|i| i.into())
            .ok_or(ReadingError::TypeConversionError(
                value.to_string(),
                PrimitiveType::Integer.to_string(),
            ))
    }
}

impl TryFrom<LogicalInteger> for LogicalFloat64 {
    type Error = ReadingError;

    fn try_from(value: LogicalInteger) -> Result<Self, Self::Error> {
        Double::new(
            f64::from_i64(value.0).ok_or(ReadingError::TypeConversionError(
                value.to_string(),
                PrimitiveType::Integer.to_string(),
            ))?,
        )
        .map(|d| d.into())
    }
}

impl TryFrom<Term> for LogicalString {
    type Error = InvalidRuleTermConversion;

    fn try_from(term: Term) -> Result<Self, Self::Error> {
        match term {
            Term::StringLiteral(s) => Ok(s.into()),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            }) => match datatype.as_str() {
                XSD_STRING => Ok(value.to_string().into()),
                _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::String)),
            },
            _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::String)),
        }
    }
}

impl TryFrom<Term> for LogicalInteger {
    type Error = InvalidRuleTermConversion;

    fn try_from(term: Term) -> Result<Self, Self::Error> {
        match term {
            Term::NumericLiteral(NumericLiteral::Integer(i)) => Ok(i.into()),
            Term::NumericLiteral(NumericLiteral::Decimal(i, 0)) => Ok(i.into()),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            }) => match datatype.as_str() {
                // XSD 3.4.13 integer
                // [Definition:] integer is ·derived· from decimal by
                // fixing the value of ·fractionDigits· to be 0 and
                // disallowing the trailing decimal point.
                XSD_INTEGER | XSD_DECIMAL => value
                    .parse()
                    .map(|i: i64| i.into())
                    .map_err(|_err| InvalidRuleTermConversion::new(term, PrimitiveType::Integer)),
                _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::Integer)),
            },
            _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::Integer)),
        }
    }
}

impl TryFrom<Term> for i64 {
    type Error = InvalidRuleTermConversion;

    fn try_from(value: Term) -> Result<Self, Self::Error> {
        Ok(LogicalInteger::try_from(value)?.into())
    }
}

impl TryFrom<Term> for LogicalFloat64 {
    type Error = InvalidRuleTermConversion;

    fn try_from(term: Term) -> Result<Self, Self::Error> {
        match term {
            Term::NumericLiteral(NumericLiteral::Double(d)) => Ok(d.into()),
            Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => format!("{a}.{b}")
                .parse()
                .ok()
                .and_then(|d: f64| Double::new(d).map(|d| d.into()).ok())
                .ok_or(InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
            Term::NumericLiteral(NumericLiteral::Integer(a)) => LogicalInteger(a)
                .try_into()
                .map_err(|_err| InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            }) => match datatype.as_str() {
                XSD_DOUBLE | XSD_DECIMAL => value
                    .parse()
                    .ok()
                    .and_then(|d| Double::new(d).map(|d| d.into()).ok())
                    .ok_or(InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
                XSD_INTEGER => value
                    .parse()
                    .ok()
                    .and_then(|i| LogicalInteger(i).try_into().ok())
                    .ok_or(InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
                _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
            },
            _ => Err(InvalidRuleTermConversion::new(term, PrimitiveType::Float64)),
        }
    }
}

impl TryFrom<Term> for Double {
    type Error = InvalidRuleTermConversion;

    fn try_from(value: Term) -> Result<Self, Self::Error> {
        Ok(LogicalFloat64::try_from(value)?.into())
    }
}

impl TryFrom<Term> for PhysicalString {
    type Error = InvalidRuleTermConversion;

    fn try_from(term: Term) -> Result<Self, Self::Error> {
        match term {
            // the first branch is the only one that results in an error
            Term::Variable(_) => Err(InvalidRuleTermConversion::new(term, PrimitiveType::Any)),
            Term::Constant(c) => Ok(c.into()),
            Term::NumericLiteral(NumericLiteral::Integer(i)) => Ok(LogicalInteger(i).into()),
            Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => Ok(Decimal(a, b).into()),
            Term::NumericLiteral(NumericLiteral::Double(d)) => Ok(LogicalFloat64(d).into()),
            Term::StringLiteral(s) => Ok(LogicalString(s).into()),
            Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
                Ok(LanguageString(value, tag).into())
            }
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            }) => match datatype.as_ref() {
                XSD_STRING => Ok(LogicalString(value.to_string()).into()),
                XSD_INTEGER => Ok(value
                    .parse()
                    .map(|v| LogicalInteger(v).into())
                    .unwrap_or(DatatypeValue(value.to_string(), datatype.to_string()).into())),
                XSD_DECIMAL => Ok(value
                    .rsplit_once('.')
                    .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
                    .or_else(|| Some((value.parse().ok()?, 0)))
                    .map(|(a, b)| Decimal(a, b).into())
                    .unwrap_or(DatatypeValue(value.to_string(), datatype.to_string()).into())),
                XSD_DOUBLE => Ok(value
                    .parse()
                    .ok()
                    .and_then(|f64| Double::new(f64).ok())
                    .map(|d| LogicalFloat64(d).into())
                    .unwrap_or(DatatypeValue(value.to_string(), datatype.to_string()).into())),
                _ => Ok(DatatypeValue(value.to_string(), datatype.to_string()).into()),
            },
        }
    }
}

/// Enum for values in the logical layer
#[derive(Debug)]
pub enum PrimitiveLogicalValueT {
    /// Any variant
    Any(Term),
    /// String variant
    String(LogicalString),
    /// Integer variant
    Integer(LogicalInteger),
    /// Float64 variant
    Float64(LogicalFloat64),
}

impl From<Term> for PrimitiveLogicalValueT {
    fn from(value: Term) -> Self {
        Self::Any(value)
    }
}

impl From<LogicalString> for PrimitiveLogicalValueT {
    fn from(value: LogicalString) -> Self {
        Self::String(value)
    }
}

impl From<LogicalInteger> for PrimitiveLogicalValueT {
    fn from(value: LogicalInteger) -> Self {
        Self::Integer(value)
    }
}

impl From<LogicalFloat64> for PrimitiveLogicalValueT {
    fn from(value: LogicalFloat64) -> Self {
        Self::Float64(value)
    }
}

pub(super) type DefaultAnyIterator<'a> = Box<dyn Iterator<Item = Term> + 'a>;
pub(super) type DefaultStringIterator<'a> = Box<dyn Iterator<Item = LogicalString> + 'a>;
pub(super) type DefaultIntegerIterator<'a> = Box<dyn Iterator<Item = LogicalInteger> + 'a>;
pub(super) type DefaultFloat64Iterator<'a> = Box<dyn Iterator<Item = LogicalFloat64> + 'a>;
pub(super) type DefaultSerializedIterator<'a> = Box<dyn Iterator<Item = String> + 'a>;

/// Iterator over one kind of possible logical values
#[allow(missing_debug_implementations)]
pub enum PrimitiveLogicalValueIteratorT<'a> {
    /// Any variant
    Any(DefaultAnyIterator<'a>),
    /// String variant
    String(DefaultStringIterator<'a>),
    /// Integer variant
    Integer(DefaultIntegerIterator<'a>),
    /// Float64 variant
    Float64(DefaultFloat64Iterator<'a>),
}

impl<'a> Iterator for PrimitiveLogicalValueIteratorT<'a> {
    type Item = PrimitiveLogicalValueT;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Any(iter) => Some(PrimitiveLogicalValueT::Any(iter.next()?)),
            Self::String(iter) => Some(PrimitiveLogicalValueT::String(iter.next()?)),
            Self::Integer(iter) => Some(PrimitiveLogicalValueT::Integer(iter.next()?)),
            Self::Float64(iter) => Some(PrimitiveLogicalValueT::Float64(iter.next()?)),
        }
    }
}

pub(super) struct AnyOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = PhysicalString> + 'a>,
}

impl<'a> AnyOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl From<PhysicalString> for Term {
    fn from(s: PhysicalString) -> Self {
        // unwrap physical string
        let s: String = s.into();
        match s {
                s if s.starts_with(LANGUAGE_STRING_PREFIX) => {
                    let (value, tag) = s[LANGUAGE_STRING_PREFIX.len()..]
                        .rsplit_once('@')
                        .expect("Physical Value should be well-formatted.");
                    Term::RdfLiteral(RdfLiteral::LanguageString {
                        value: value.to_string(),
                        tag: tag.to_string(),
                    })
                }
                s if s.starts_with(STRING_PREFIX) => {
                    Term::StringLiteral(s[STRING_PREFIX.len()..].to_string())
                }
                s if s.starts_with(INTEGER_PREFIX) => {
                    Term::NumericLiteral(NumericLiteral::Integer(
                        s[INTEGER_PREFIX.len()..]
                            .parse()
                            .expect("Physical Value should be well-formatted."),
                    ))
                }
                s if s.starts_with(DECIMAL_PREFIX) => {
                    let (a, b) = s[DECIMAL_PREFIX.len()..]
                        .rsplit_once('.')
                        .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
                        .expect("Physical Value should be well-formatted.");
                    Term::NumericLiteral(NumericLiteral::Decimal(a, b))
                }
                s if s.starts_with(DOUBLE_PREFIX) => Term::NumericLiteral(NumericLiteral::Double(
                    s[DOUBLE_PREFIX.len()..]
                        .parse()
                        .ok()
                        .and_then(|f64| Double::new(f64).ok())
                        .expect("Physical Value should be well-formatted."),
                )),
                s if s.starts_with(CONSTANT_PREFIX) => {
                    Term::Constant(s[CONSTANT_PREFIX.len()..].to_string().into())
                }
                s if s.starts_with(DATATYPE_VALUE_PREFIX) => {
                    let (value, datatype) = s[DATATYPE_VALUE_PREFIX.len()..]
                        .rsplit_once("^^")
                        .expect("Physical Value should be well-formatted.");
                    Term::RdfLiteral(RdfLiteral::DatatypeValue {
                        value: value.to_string(),
                        datatype: datatype.to_string(),
                    })
                }
                s if s.starts_with(NULL_PREFIX) => Term::Constant(format!("{LOGICAL_NULL_PREFIX}{}", &s[NULL_PREFIX.len()..]).into()),
                _ => unreachable!("The physical strings should take one of the previous forms. Apparently we forgot to handle terms like: {s:?}"),
            }
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultAnyIterator<'a> {
    fn from(source: AnyOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| s.into()))
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultSerializedIterator<'a> {
    fn from(source: AnyOutputMapper<'a>) -> Self {
        // NOTE: depending on performance change, maybe implement shortcut here to not construct Term first;
        // I prefer the current solution at the moment since it is easier to maintain
        Box::new(DefaultAnyIterator::from(source).map(|term| {
            let mapped_term = match term {
                // for numeric literals we do not use the standard display but convert them to a proper
                // rdf literal first
                Term::NumericLiteral(NumericLiteral::Integer(i)) => {
                    Term::RdfLiteral(RdfLiteral::DatatypeValue {
                        value: i.to_string(),
                        datatype: XSD_INTEGER.to_string(),
                    })
                }
                Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
                    Term::RdfLiteral(RdfLiteral::DatatypeValue {
                        value: format!("{a}.{b}").to_string(),
                        datatype: XSD_DECIMAL.to_string(),
                    })
                }
                Term::NumericLiteral(NumericLiteral::Double(d)) => {
                    Term::RdfLiteral(RdfLiteral::DatatypeValue {
                        value: format!("{:E}", f64::from(d)),
                        datatype: XSD_DOUBLE.to_string(),
                    })
                }
                _ => term,
            };

            mapped_term.to_string()
        }))
    }
}

pub(super) struct StringOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = PhysicalString> + 'a>,
}

impl<'a> StringOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl From<PhysicalString> for LogicalString {
    fn from(s: PhysicalString) -> Self {
        // unwrap physical string
        let s: String = s.into();
        match s {
            s if s.starts_with(LANGUAGE_STRING_PREFIX) => {
                let (value, _tag) = s[LANGUAGE_STRING_PREFIX.len()..]
                    .rsplit_once('@')
                    .expect("Physical Value should be well-formatted.");
                value.to_string().into()
            }
            s if s.starts_with(STRING_PREFIX) => {
                s[STRING_PREFIX.len()..].to_string().into()
            }
            _ => unreachable!("The physical strings should take one of the previous forms. Apparently we forgot to handle terms like: {s:?}"),
        }
    }
}

impl<'a> From<StringOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: StringOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| s.into()))
    }
}

impl<'a> From<StringOutputMapper<'a>> for DefaultSerializedIterator<'a> {
    fn from(source: StringOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| LogicalString::from(s).into()))
    }
}

pub(super) struct IntegerOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = i64> + 'a>,
}

impl<'a> IntegerOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::I64(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<IntegerOutputMapper<'a>> for DefaultIntegerIterator<'a> {
    fn from(source: IntegerOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|i| i.into()))
    }
}

impl<'a> From<IntegerOutputMapper<'a>> for DefaultSerializedIterator<'a> {
    fn from(source: IntegerOutputMapper<'a>) -> Self {
        Box::new(
            source
                .physical_iter
                .map(|i| LogicalInteger::from(i).to_string()),
        )
    }
}

pub(super) struct Float64OutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = Double> + 'a>,
}

impl<'a> Float64OutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::Double(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<Float64OutputMapper<'a>> for DefaultFloat64Iterator<'a> {
    fn from(source: Float64OutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|d| d.into()))
    }
}

impl<'a> From<Float64OutputMapper<'a>> for DefaultSerializedIterator<'a> {
    fn from(source: Float64OutputMapper<'a>) -> Self {
        Box::new(
            source
                .physical_iter
                .map(|d| LogicalFloat64::from(d).to_string()),
        )
    }
}

#[cfg(test)]
mod test {
    use std::assert_eq;

    use super::*;

    #[test]
    fn input_mapping() {
        let string = LogicalString::from("my string".to_string());
        let integer = LogicalInteger::from(42);
        let double = LogicalFloat64::from(Double::new(3.41).unwrap());
        let constant = Term::Constant("my constant".to_string().into());
        let string_literal = Term::StringLiteral("string literal".to_string());
        let num_int_literal = Term::NumericLiteral(NumericLiteral::Integer(45));
        let num_decimal_literal = Term::NumericLiteral(NumericLiteral::Decimal(4, 2));
        let num_whole_decimal_literal = Term::NumericLiteral(NumericLiteral::Decimal(42, 0));
        let num_double_literal =
            Term::NumericLiteral(NumericLiteral::Double(Double::new(2.99).unwrap()));
        let language_string_literal = Term::RdfLiteral(RdfLiteral::LanguageString {
            value: "language string".to_string(),
            tag: "en".to_string(),
        });
        let random_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "some random datavalue".to_string(),
            datatype: "a datatype that I totally did not just make up".to_string(),
        });
        let string_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "string datavalue".to_string(),
            datatype: XSD_STRING.to_string(),
        });
        let integer_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "73".to_string(),
            datatype: XSD_INTEGER.to_string(),
        });
        let decimal_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });
        let signed_decimal_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "+1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });
        let negative_decimal_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "-1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });
        let pointless_decimal_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });
        let signed_pointless_decimal_datavalue_literal =
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "+23".to_string(),
                datatype: XSD_DECIMAL.to_string(),
            });
        let negative_pointless_decimal_datavalue_literal =
            Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "-23".to_string(),
                datatype: XSD_DECIMAL.to_string(),
            });
        let double_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "3.33".to_string(),
            datatype: XSD_DOUBLE.to_string(),
        });
        let large_integer_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "9950000000000000000".to_string(),
            datatype: XSD_INTEGER.to_string(),
        });
        let large_decimal_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "9950000000000000001".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });

        let expected_string: PhysicalString = format!("{STRING_PREFIX}my string").into();
        let expected_integer: PhysicalString = format!("{INTEGER_PREFIX}42").into();
        let expected_double: PhysicalString = format!("{DOUBLE_PREFIX}3.41").into();
        let expected_constant: PhysicalString = format!("{CONSTANT_PREFIX}my constant").into();
        let expected_string_literal: PhysicalString =
            format!("{STRING_PREFIX}string literal").into();
        let expected_num_int_literal: PhysicalString = format!("{INTEGER_PREFIX}45").into();
        let expected_num_decimal_literal: PhysicalString = format!("{DECIMAL_PREFIX}4.2").into();
        let expected_num_double_literal: PhysicalString = format!("{DOUBLE_PREFIX}2.99").into();
        let expected_language_string_literal: PhysicalString =
            format!("{LANGUAGE_STRING_PREFIX}language string@en").into();
        let expected_random_datavalue_literal: PhysicalString = format!("{DATATYPE_VALUE_PREFIX}some random datavalue^^a datatype that I totally did not just make up").into();
        let expected_string_datavalue_literal: PhysicalString =
            format!("{STRING_PREFIX}string datavalue").into();
        let expected_integer_datavalue_literal: PhysicalString =
            format!("{INTEGER_PREFIX}73").into();
        let expected_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}1.23").into();
        let expected_signed_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}1.23").into();
        let expected_negative_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}-1.23").into();
        let expected_pointless_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}23.0").into();
        let expected_signed_pointless_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}23.0").into();
        let expected_negative_pointless_decimal_datavalue_literal: PhysicalString =
            format!("{DECIMAL_PREFIX}-23.0").into();
        let expected_double_datavalue_literal: PhysicalString =
            format!("{DOUBLE_PREFIX}3.33").into();
        let expected_large_integer_literal: PhysicalString =
            format!("{DATATYPE_VALUE_PREFIX}9950000000000000000^^{XSD_INTEGER}").into();
        let expected_large_decimal_literal: PhysicalString =
            format!("{DATATYPE_VALUE_PREFIX}9950000000000000001^^{XSD_DECIMAL}").into();

        assert_eq!(PhysicalString::from(string), expected_string);
        assert_eq!(PhysicalString::from(integer), expected_integer);
        assert_eq!(PhysicalString::from(double), expected_double);
        assert_eq!(
            PhysicalString::try_from(constant).unwrap(),
            expected_constant
        );
        assert_eq!(
            PhysicalString::try_from(string_literal.clone()).unwrap(),
            expected_string_literal
        );
        assert_eq!(
            PhysicalString::try_from(string_literal).unwrap(),
            expected_string_literal
        );
        assert_eq!(
            PhysicalString::try_from(num_int_literal.clone()).unwrap(),
            expected_num_int_literal
        );
        assert_eq!(i64::try_from(num_int_literal).unwrap(), 45);
        assert_eq!(
            LogicalInteger::try_from(num_whole_decimal_literal).unwrap(),
            LogicalInteger(42)
        );
        assert_eq!(
            LogicalInteger::try_from(signed_pointless_decimal_datavalue_literal.clone()).unwrap(),
            LogicalInteger(23)
        );
        assert_eq!(
            PhysicalString::try_from(num_decimal_literal).unwrap(),
            expected_num_decimal_literal
        );
        assert_eq!(
            PhysicalString::try_from(num_double_literal.clone()).unwrap(),
            expected_num_double_literal
        );
        assert_eq!(
            Double::try_from(num_double_literal).unwrap(),
            Double::new(2.99).unwrap()
        );
        assert_eq!(
            PhysicalString::try_from(language_string_literal.clone()).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            PhysicalString::try_from(language_string_literal).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            PhysicalString::try_from(random_datavalue_literal).unwrap(),
            expected_random_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(string_datavalue_literal.clone()).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(string_datavalue_literal).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(integer_datavalue_literal.clone()).unwrap(),
            expected_integer_datavalue_literal
        );
        assert_eq!(i64::try_from(integer_datavalue_literal).unwrap(), 73);
        assert_eq!(
            PhysicalString::try_from(decimal_datavalue_literal).unwrap(),
            expected_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(signed_decimal_datavalue_literal).unwrap(),
            expected_signed_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(negative_decimal_datavalue_literal).unwrap(),
            expected_negative_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(pointless_decimal_datavalue_literal).unwrap(),
            expected_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(signed_pointless_decimal_datavalue_literal).unwrap(),
            expected_signed_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(negative_pointless_decimal_datavalue_literal).unwrap(),
            expected_negative_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PhysicalString::try_from(double_datavalue_literal.clone()).unwrap(),
            expected_double_datavalue_literal
        );
        assert_eq!(
            Double::try_from(double_datavalue_literal).unwrap(),
            Double::new(3.33).unwrap()
        );
        assert_eq!(
            PhysicalString::try_from(large_integer_literal).unwrap(),
            expected_large_integer_literal
        );
        assert_eq!(
            PhysicalString::try_from(large_decimal_literal).unwrap(),
            expected_large_decimal_literal
        );
    }

    #[test]
    fn api_output_mapping() {
        let phys_any_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{INTEGER_PREFIX}42"),
                format!("{DOUBLE_PREFIX}3.41"),
                format!("{CONSTANT_PREFIX}my constant"),
                format!("{STRING_PREFIX}string literal"),
                format!("{INTEGER_PREFIX}45"),
                format!("{DECIMAL_PREFIX}4.2"),
                format!("{DOUBLE_PREFIX}2.99"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{DATATYPE_VALUE_PREFIX}some random datavalue^^a datatype that I totally did not just make up"),
                format!("{STRING_PREFIX}string datavalue"),
                format!("{INTEGER_PREFIX}73"),
                format!("{DECIMAL_PREFIX}1.23"),
                format!("{DOUBLE_PREFIX}3.33"),
                format!("{NULL_PREFIX}1000001"),
            ].into_iter().map(PhysicalString::from)
        ));

        let phys_string_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{STRING_PREFIX}42"),
                format!("{STRING_PREFIX}3.41"),
                format!("{STRING_PREFIX}string literal"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{STRING_PREFIX}string datavalue"),
            ]
            .into_iter()
            .map(PhysicalString::from),
        ));

        let phys_int_iter = DataValueIteratorT::I64(Box::new([42, 45, 73].into_iter()));

        let phys_double_iter = DataValueIteratorT::Double(Box::new(
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
            .into_iter(),
        ));

        let any_out: DefaultAnyIterator = AnyOutputMapper::new(phys_any_iter).into();
        let string_out: DefaultStringIterator = StringOutputMapper::new(phys_string_iter).into();
        let int_out: DefaultIntegerIterator = IntegerOutputMapper::new(phys_int_iter).into();
        let double_out: DefaultFloat64Iterator = Float64OutputMapper::new(phys_double_iter).into();

        let any_vec: Vec<Term> = any_out.collect();
        let string_vec: Vec<LogicalString> = string_out.collect();
        let integer_vec: Vec<LogicalInteger> = int_out.collect();
        let double_vec: Vec<LogicalFloat64> = double_out.collect();

        assert_eq!(
            any_vec,
            vec![
                Term::StringLiteral("my string".to_string()),
                Term::NumericLiteral(NumericLiteral::Integer(42)),
                Term::NumericLiteral(NumericLiteral::Double(Double::new(3.41).unwrap())),
                Term::Constant("my constant".to_string().into()),
                Term::StringLiteral("string literal".to_string()),
                Term::NumericLiteral(NumericLiteral::Integer(45)),
                Term::NumericLiteral(NumericLiteral::Decimal(4, 2)),
                Term::NumericLiteral(NumericLiteral::Double(Double::new(2.99).unwrap())),
                Term::RdfLiteral(RdfLiteral::LanguageString {
                    value: "language string".to_string(),
                    tag: "en".to_string(),
                }),
                Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    value: "some random datavalue".to_string(),
                    datatype: "a datatype that I totally did not just make up".to_string(),
                }),
                Term::StringLiteral("string datavalue".to_string()),
                Term::NumericLiteral(NumericLiteral::Integer(73)),
                Term::NumericLiteral(NumericLiteral::Decimal(1, 23)),
                Term::NumericLiteral(NumericLiteral::Double(Double::new(3.33).unwrap())),
                Term::Constant(format!("{LOGICAL_NULL_PREFIX}1000001").into()),
            ]
        );

        assert_eq!(
            string_vec,
            [
                "my string",
                "42",
                "3.41",
                "string literal",
                "language string",
                "string datavalue",
            ]
            .into_iter()
            .map(String::from)
            .map(LogicalString::from)
            .collect::<Vec<_>>(),
        );

        assert_eq!(
            integer_vec,
            [42, 45, 73]
                .into_iter()
                .map(LogicalInteger::from)
                .collect::<Vec<_>>()
        );

        assert_eq!(
            double_vec,
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
            .into_iter()
            .map(LogicalFloat64::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn serialized_output_mapping() {
        let phys_any_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{INTEGER_PREFIX}42"),
                format!("{DOUBLE_PREFIX}3.41"),
                format!("{CONSTANT_PREFIX}my constant"),
                format!("{STRING_PREFIX}string literal"),
                format!("{INTEGER_PREFIX}45"),
                format!("{DECIMAL_PREFIX}4.2"),
                format!("{DOUBLE_PREFIX}2.99"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{DATATYPE_VALUE_PREFIX}some random datavalue^^a datatype that I totally did not just make up"),
                format!("{STRING_PREFIX}string datavalue"),
                format!("{INTEGER_PREFIX}73"),
                format!("{DECIMAL_PREFIX}1.23"),
                format!("{DOUBLE_PREFIX}3.33"),
                format!("{NULL_PREFIX}1000001"),
            ]
            .into_iter().map(|s| s.into())));

        let phys_string_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{STRING_PREFIX}42"),
                format!("{STRING_PREFIX}3.41"),
                format!("{STRING_PREFIX}string literal"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{STRING_PREFIX}string datavalue"),
            ]
            .into_iter()
            .map(|s| s.into()),
        ));

        let phys_int_iter = DataValueIteratorT::I64(Box::new([42, 45, 73].into_iter()));

        let phys_double_iter = DataValueIteratorT::Double(Box::new(
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
            .into_iter(),
        ));

        let any_out: DefaultSerializedIterator = AnyOutputMapper::new(phys_any_iter).into();
        let string_out: DefaultSerializedIterator =
            StringOutputMapper::new(phys_string_iter).into();
        let int_out: DefaultSerializedIterator = IntegerOutputMapper::new(phys_int_iter).into();
        let double_out: DefaultSerializedIterator =
            Float64OutputMapper::new(phys_double_iter).into();

        let any_vec: Vec<String> = any_out.collect();
        let string_vec: Vec<String> = string_out.collect();
        let integer_vec: Vec<String> = int_out.collect();
        let double_vec: Vec<String> = double_out.collect();

        assert_eq!(
            any_vec,
            [
                r#""my string""#,
                r#""42"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
                r#""3.41E0"^^<http://www.w3.org/2001/XMLSchema#double>"#,
                r#"my constant"#,
                r#""string literal""#,
                r#""45"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
                r#""4.2"^^<http://www.w3.org/2001/XMLSchema#decimal>"#,
                r#""2.99E0"^^<http://www.w3.org/2001/XMLSchema#double>"#,
                r#""language string"@en"#,
                r#""some random datavalue"^^<a datatype that I totally did not just make up>"#,
                r#""string datavalue""#,
                r#""73"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
                r#""1.23"^^<http://www.w3.org/2001/XMLSchema#decimal>"#,
                r#""3.33E0"^^<http://www.w3.org/2001/XMLSchema#double>"#,
                r#"<__Null#1000001>"#,
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );

        assert_eq!(
            string_vec,
            [
                "my string",
                "42",
                "3.41",
                "string literal",
                "language string",
                "string datavalue",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );

        assert_eq!(
            integer_vec,
            [42, 45, 73]
                .into_iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            double_vec,
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
            .into_iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>(),
        );
    }
}
