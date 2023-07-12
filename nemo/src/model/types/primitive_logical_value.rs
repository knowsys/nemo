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

/// The prefix used to indicate constants that are Nulls
pub const LOGICAL_NULL_PREFIX: &str = "__Null#";

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
                s if s.starts_with(NULL_PREFIX) => Term::Constant(format!("{LOGICAL_NULL_PREFIX}{}", s[NULL_PREFIX.len()..].to_string()).into()),
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
                s if s.starts_with(LANGUAGE_STRING_PREFIX) => {
                    let (value, _tag) = s[LANGUAGE_STRING_PREFIX.len()..]
                        .rsplit_once('@')
                        .expect("Physical Value should be well-formatted.");
                    value.to_string()
                }
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
            XSD_DOUBLE | XSD_DECIMAL => value.parse().ok().and_then(|d| Double::new(d).ok()).ok_or(
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

#[cfg(test)]
mod test {
    use std::assert_eq;

    use super::*;

    #[test]
    fn input_mapping() {
        let string = "my string".to_string();
        let integer = 42i64;
        let double = Double::new(3.41).unwrap();
        let constant = Term::Constant("my constant".to_string().into());
        let string_literal = Term::StringLiteral("string literal".to_string());
        let num_int_literal = Term::NumericLiteral(NumericLiteral::Integer(45));
        let num_decimal_literal = Term::NumericLiteral(NumericLiteral::Decimal(4, 2));
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
        let double_datavalue_literal = Term::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "3.33".to_string(),
            datatype: XSD_DOUBLE.to_string(),
        });

        let expected_string = format!("{STRING_PREFIX}my string");
        let expected_integer = format!("{INTEGER_PREFIX}42");
        let expected_double = format!("{DOUBLE_PREFIX}3.41");
        let expected_constant = format!("{CONSTANT_PREFIX}my constant");
        let expected_string_literal = format!("{STRING_PREFIX}string literal");
        let expected_num_int_literal = format!("{INTEGER_PREFIX}45");
        let expected_num_decimal_literal = format!("{DECIMAL_PREFIX}4.2");
        let expected_num_double_literal = format!("{DOUBLE_PREFIX}2.99");
        let expected_language_string_literal =
            format!("{LANGUAGE_STRING_PREFIX}language string@en");
        let expected_random_datavalue_literal = format!("{DATATYPE_VALUE_PREFIX}some random datavalue^^a datatype that I totally did not just make up");
        let expected_string_datavalue_literal = format!("{STRING_PREFIX}string datavalue");
        let expected_integer_datavalue_literal = format!("{INTEGER_PREFIX}73");
        let expected_decimal_datavalue_literal = format!("{DECIMAL_PREFIX}1.23");
        let expected_double_datavalue_literal = format!("{DOUBLE_PREFIX}3.33");

        assert_eq!(logical_string_to_physical_string(string), expected_string);
        assert_eq!(
            logical_integer_to_physical_string(integer),
            expected_integer
        );
        assert_eq!(logical_double_to_physical_string(double), expected_double);
        assert_eq!(
            any_term_to_physical_string(constant).unwrap(),
            expected_constant
        );
        assert_eq!(
            any_term_to_physical_string(string_literal.clone()).unwrap(),
            expected_string_literal
        );
        assert_eq!(
            any_string_to_physical_string(string_literal).unwrap(),
            expected_string_literal
        );
        assert_eq!(
            any_term_to_physical_string(num_int_literal.clone()).unwrap(),
            expected_num_int_literal
        );
        assert_eq!(
            any_integer_to_physical_integer(num_int_literal).unwrap(),
            45
        );
        assert_eq!(
            any_term_to_physical_string(num_decimal_literal).unwrap(),
            expected_num_decimal_literal
        );
        assert_eq!(
            any_term_to_physical_string(num_double_literal.clone()).unwrap(),
            expected_num_double_literal
        );
        assert_eq!(
            any_double_to_physical_double(num_double_literal).unwrap(),
            Double::new(2.99).unwrap()
        );
        assert_eq!(
            any_term_to_physical_string(language_string_literal.clone()).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            any_string_to_physical_string(language_string_literal).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            any_term_to_physical_string(random_datavalue_literal).unwrap(),
            expected_random_datavalue_literal
        );
        assert_eq!(
            any_term_to_physical_string(string_datavalue_literal.clone()).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            any_string_to_physical_string(string_datavalue_literal).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            any_term_to_physical_string(integer_datavalue_literal.clone()).unwrap(),
            expected_integer_datavalue_literal
        );
        assert_eq!(
            any_integer_to_physical_integer(integer_datavalue_literal).unwrap(),
            73
        );
        assert_eq!(
            any_term_to_physical_string(decimal_datavalue_literal).unwrap(),
            expected_decimal_datavalue_literal
        );
        assert_eq!(
            any_term_to_physical_string(double_datavalue_literal.clone()).unwrap(),
            expected_double_datavalue_literal
        );
        assert_eq!(
            any_double_to_physical_double(double_datavalue_literal).unwrap(),
            Double::new(3.33).unwrap()
        );
    }

    #[test]
    fn api_output_mapping() {
        let phys_any_iter = DataValueIteratorT::String(Box::new([
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
        ].into_iter()));

        let phys_string_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{STRING_PREFIX}42"),
                format!("{STRING_PREFIX}3.41"),
                format!("{STRING_PREFIX}string literal"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{STRING_PREFIX}string datavalue"),
            ]
            .into_iter(),
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
        let string_vec: Vec<String> = string_out.collect();
        let integer_vec: Vec<i64> = int_out.collect();
        let double_vec: Vec<Double> = double_out.collect();

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
            .collect::<Vec<_>>(),
        );

        assert_eq!(integer_vec, vec![42, 45, 73]);

        assert_eq!(
            double_vec,
            [
                Double::new(3.41).unwrap(),
                Double::new(2.99).unwrap(),
                Double::new(3.33).unwrap(),
            ]
        );
    }

    #[test]
    fn serialized_output_mapping() {
        let phys_any_iter = DataValueIteratorT::String(Box::new([
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
        ].into_iter()));

        let phys_string_iter = DataValueIteratorT::String(Box::new(
            [
                format!("{STRING_PREFIX}my string"),
                format!("{STRING_PREFIX}42"),
                format!("{STRING_PREFIX}3.41"),
                format!("{STRING_PREFIX}string literal"),
                format!("{LANGUAGE_STRING_PREFIX}language string@en"),
                format!("{STRING_PREFIX}string datavalue"),
            ]
            .into_iter(),
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

        let any_out: DefaultStringIterator = AnyOutputMapper::new(phys_any_iter).into();
        let string_out: DefaultStringIterator = StringOutputMapper::new(phys_string_iter).into();
        let int_out: DefaultStringIterator = IntegerOutputMapper::new(phys_int_iter).into();
        let double_out: DefaultStringIterator = Float64OutputMapper::new(phys_double_iter).into();

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
