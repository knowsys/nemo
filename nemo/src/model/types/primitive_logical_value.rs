use nemo_physical::datatypes::Double;
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

/// Enum for values in the logical layer
#[derive(Debug)]
pub enum PrimitiveLogicalValueT {
    /// Any variant
    Any(Term),
    /// String variant
    String(String),
    /// Integer variant
    Integer(i64),
    /// Float64 variant
    Float64(Double),
}

impl From<Term> for PrimitiveLogicalValueT {
    fn from(value: Term) -> Self {
        Self::Any(value)
    }
}

impl From<String> for PrimitiveLogicalValueT {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for PrimitiveLogicalValueT {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<Double> for PrimitiveLogicalValueT {
    fn from(value: Double) -> Self {
        Self::Float64(value)
    }
}

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

pub(super) type DefaultAnyIterator<'a> = Box<dyn Iterator<Item = Term> + 'a>;
pub(super) type DefaultStringIterator<'a> = Box<dyn Iterator<Item = String> + 'a>;
pub(super) type DefaultIntegerIterator<'a> = Box<dyn Iterator<Item = i64> + 'a>;
pub(super) type DefaultFloat64Iterator<'a> = Box<dyn Iterator<Item = Double> + 'a>;

pub(super) struct AnyOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = String> + 'a>,
}

impl<'a> AnyOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultAnyIterator<'a> {
    fn from(source: AnyOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| {
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
                s if s.starts_with(NULL_PREFIX) => Term::Constant(format!("__Null#{}", s[NULL_PREFIX.len()..].to_string()).into()),
                _ => unreachable!("The physical strings should take one of the previous forms. Apparently we forgot to handle terms like: {s:?}"),
            }
        }))
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultStringIterator<'a> {
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
    physical_iter: Box<dyn Iterator<Item = String> + 'a>,
}

impl<'a> StringOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<StringOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: StringOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| {
            match s {
                s if s.starts_with(STRING_PREFIX) => {
                    s[STRING_PREFIX.len()..].to_string()
                }
                _ => unreachable!("The physical strings should take one of the previous forms. Apparently we forgot to handle terms like: {s:?}"),
            }
        }))
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
        source.physical_iter
    }
}

impl<'a> From<IntegerOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: IntegerOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|i| i.to_string()))
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
        source.physical_iter
    }
}

impl<'a> From<Float64OutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: Float64OutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|d| d.to_string()))
    }
}

/// Map a language string into its physical any representation
pub fn language_string_to_physical_string(s: String, tag: String) -> String {
    format!("{LANGUAGE_STRING_PREFIX}{s}@{tag}")
}

/// Map a logical string into its physical any representation
pub fn logical_string_to_physical_string(s: String) -> String {
    format!("{STRING_PREFIX}{s}")
}

/// Map a logical integer into its physical any representation
pub fn logical_integer_to_physical_string(i: i64) -> String {
    format!("{INTEGER_PREFIX}{i}")
}

/// Map a logical decimal into its physical any representation
pub fn logical_decimal_to_physical_string(before_comma: i64, after_comma: u64) -> String {
    format!("{DECIMAL_PREFIX}{before_comma}.{after_comma}")
}

/// Map a logical double into its physical any representation
pub fn logical_double_to_physical_string(d: Double) -> String {
    format!("{DOUBLE_PREFIX}{d}")
}

/// Map a logical constant into its physical any representation
pub fn logical_constant_to_physical_string(c: Identifier) -> String {
    format!("{CONSTANT_PREFIX}{c}")
}

/// Map a logical datatype_value into its physical any representation
pub fn logical_datatype_value_to_physical_string(value: String, datatype: String) -> String {
    format!("{DATATYPE_VALUE_PREFIX}{value}^^{datatype}")
}

/// Map an rdf term expected to be a string into its physical representation
pub fn any_string_to_physical_string(
    string_term: Term,
) -> Result<String, InvalidRuleTermConversion> {
    match string_term {
        Term::StringLiteral(s) => Ok(logical_string_to_physical_string(s)),
        Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
            Ok(language_string_to_physical_string(value, tag))
        }
        Term::RdfLiteral(RdfLiteral::DatatypeValue {
            ref value,
            ref datatype,
        }) => match datatype.as_str() {
            XSD_STRING => Ok(logical_string_to_physical_string(value.to_string())),
            _ => Err(InvalidRuleTermConversion::new(
                string_term,
                PrimitiveType::String,
            )),
        },
        _ => Err(InvalidRuleTermConversion::new(
            string_term,
            PrimitiveType::String,
        )),
    }
}

/// Map an rdf term expected to be an integer into its physical representation
pub fn any_integer_to_physical_integer(
    integer_term: Term,
) -> Result<i64, InvalidRuleTermConversion> {
    match integer_term {
        Term::NumericLiteral(NumericLiteral::Integer(i)) => Ok(i),
        Term::RdfLiteral(RdfLiteral::DatatypeValue {
            ref value,
            ref datatype,
        }) => match datatype.as_str() {
            XSD_INTEGER => value.parse().map_err(|_err| {
                InvalidRuleTermConversion::new(integer_term, PrimitiveType::Integer)
            }),
            _ => Err(InvalidRuleTermConversion::new(
                integer_term,
                PrimitiveType::Integer,
            )),
        },
        _ => Err(InvalidRuleTermConversion::new(
            integer_term,
            PrimitiveType::Integer,
        )),
    }
}

/// Map an rdf term expected to be a double into its physical representation
pub fn any_double_to_physical_double(
    double_term: Term,
) -> Result<Double, InvalidRuleTermConversion> {
    match double_term {
        Term::NumericLiteral(NumericLiteral::Double(d)) => Ok(d),
        Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
            Ok(Double::from_number(format!("{a}.{b}").parse().unwrap()))
        }
        Term::NumericLiteral(NumericLiteral::Integer(a)) => Ok(Double::from_number(a as f64)),
        Term::RdfLiteral(RdfLiteral::DatatypeValue {
            ref value,
            ref datatype,
        }) => match datatype.as_str() {
            XSD_DOUBLE => value.parse().ok().and_then(|d| Double::new(d).ok()).ok_or(
                InvalidRuleTermConversion::new(double_term, PrimitiveType::Float64),
            ),
            _ => Err(InvalidRuleTermConversion::new(
                double_term,
                PrimitiveType::Float64,
            )),
        },
        _ => Err(InvalidRuleTermConversion::new(
            double_term,
            PrimitiveType::Float64,
        )),
    }
}

/// Map any rdf term into its physical representation
pub fn any_term_to_physical_string(term: Term) -> Result<String, InvalidRuleTermConversion> {
    match term {
        Term::Variable(_) => {
            panic!("Expecting ground term for conversion to DataValueT")
        }
        Term::Constant(c) => Ok(logical_constant_to_physical_string(c)),
        Term::NumericLiteral(NumericLiteral::Integer(i)) => {
            Ok(logical_integer_to_physical_string(i))
        }
        Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
            Ok(logical_decimal_to_physical_string(a, b))
        }
        Term::NumericLiteral(NumericLiteral::Double(d)) => Ok(logical_double_to_physical_string(d)),
        Term::StringLiteral(s) => Ok(logical_string_to_physical_string(s)),
        Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
            Ok(language_string_to_physical_string(value, tag))
        }
        Term::RdfLiteral(RdfLiteral::DatatypeValue {
            ref value,
            ref datatype,
        }) => match datatype.as_ref() {
            XSD_STRING => Ok(logical_string_to_physical_string(value.to_string())),
            XSD_INTEGER => Ok(logical_integer_to_physical_string(value.parse().map_err(
                |_err| InvalidRuleTermConversion::new(term, PrimitiveType::Any),
            )?)),
            XSD_DECIMAL => {
                let (a, b) = value
                    .rsplit_once('.')
                    .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
                    .ok_or(InvalidRuleTermConversion::new(term, PrimitiveType::Any))?;

                Ok(logical_decimal_to_physical_string(a, b))
            }
            XSD_DOUBLE => Ok(logical_double_to_physical_string(
                value
                    .parse()
                    .ok()
                    .and_then(|f64| Double::new(f64).ok())
                    .ok_or(InvalidRuleTermConversion::new(term, PrimitiveType::Any))?,
            )),
            _ => Ok(logical_datatype_value_to_physical_string(
                value.to_string(),
                datatype.to_string(),
            )),
        },
    }
}
