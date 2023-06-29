//! This module defines logical types.

use std::fmt::Display;
use std::str::FromStr;

use crate::builder_proxy::{
    LogicalAnyColumnBuilderProxy, LogicalColumnBuilderProxy, LogicalFloat64ColumnBuilderProxy,
    LogicalIntegerColumnBuilderProxy, LogicalStringColumnBuilderProxy,
};
use crate::io::parser::ParseError;
use nemo_physical::builder_proxy::PhysicalBuilderProxyEnum;
use nemo_physical::datatypes::{DataTypeName, DataValueT, Double};

use super::model::{Identifier, NumericLiteral, RdfLiteral, Term};

use thiserror::Error;

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! generate_logical_type_enum {
    ($(($variant_name:ident, $string_repr: literal)),+) => {
        /// An enum capturing the logical type names and funtionality related to parsing and translating into and from physical types
        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
        pub enum LogicalTypeEnum {
            $(
                /// $variant_name
                $variant_name
            ),+
        }

        impl LogicalTypeEnum {
            const _VARIANTS: [Self; count!($($variant_name)+)] = [
                $(Self::$variant_name),+
            ];

            /// Returns a list of the syntactic representations of valid types.
            pub fn type_representations() -> Vec<&'static str> {
                vec![$($string_repr),+]
            }
        }

        impl Display for LogicalTypeEnum {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Self::$variant_name => write!(f, "{}", $string_repr)),+
                }
            }
        }

        impl FromStr for LogicalTypeEnum {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($string_repr => Ok(Self::$variant_name)),+,
                    _ => Err(Self::Err::ParseUnknownType(s.to_string()))
                }
            }
        }
    };
}

generate_logical_type_enum!(
    (Any, "any"),
    (String, "string"),
    (Integer, "integer"),
    (Float64, "float64")
);

impl PartialOrd for LogicalTypeEnum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self {
            Self::Any => {
                if matches!(other, Self::Any) {
                    Some(std::cmp::Ordering::Equal)
                } else {
                    Some(std::cmp::Ordering::Greater)
                }
            }
            Self::String => match other {
                Self::Any => Some(std::cmp::Ordering::Less),
                Self::String => Some(std::cmp::Ordering::Equal),
                _ => None,
            },
            Self::Integer => match other {
                Self::Any => None, // TODO: should be the following once reasoning supports casting: Some(std::cmp::Ordering::Less),
                Self::Integer => Some(std::cmp::Ordering::Equal),
                _ => None,
            },
            Self::Float64 => match other {
                Self::Any => None, // TODO: should be the following once reasoning supports casting: Some(std::cmp::Ordering::Less),
                Self::Float64 => Some(std::cmp::Ordering::Equal),
                _ => None,
            },
        }
    }
}

impl Default for LogicalTypeEnum {
    fn default() -> Self {
        Self::Any
    }
}

impl From<LogicalTypeEnum> for DataTypeName {
    fn from(source: LogicalTypeEnum) -> Self {
        match source {
            LogicalTypeEnum::Any => Self::String,
            LogicalTypeEnum::String => Self::String,
            LogicalTypeEnum::Integer => Self::I64,
            LogicalTypeEnum::Float64 => Self::Double,
        }
    }
}

// TODO: probably put this closer to the parser and also have a default prefix for xsd:
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

impl LogicalTypeEnum {
    /// Convert a given ground term to a DataValueT fitting the current logical type
    pub fn ground_term_to_data_value_t(&self, gt: Term) -> Result<DataValueT, TypeError> {
        let result = match self {
            Self::Any => {
                match gt {
                    Term::Variable(_) => {
                        panic!("Expecting ground term for conversion to DataValueT")
                    }
                    Term::Constant(Identifier(s)) => {
                        if s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                            DataValueT::String(s)
                        } else {
                            DataValueT::String(format!("<{s}>"))
                        }
                    }
                    // TODO: maybe implement display on numeric literal instead?
                    Term::NumericLiteral(NumericLiteral::Integer(i)) => {
                        DataValueT::String(format!("\"{i}\"^^<{XSD_INTEGER}>"))
                    }
                    Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
                        DataValueT::String(format!("\"{a}.{b}\"^^<{XSD_DECIMAL}>"))
                    }
                    Term::NumericLiteral(NumericLiteral::Double(d)) => {
                        DataValueT::String(format!("\"{d}\"^^<{XSD_DOUBLE}>"))
                    }
                    Term::StringLiteral(s) => DataValueT::String(format!("\"{s}\"")),
                    Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
                        DataValueT::String(format!("\"{value}\"@{tag}"))
                    }
                    Term::RdfLiteral(RdfLiteral::DatatypeValue { value, datatype }) => {
                        match datatype.as_ref() {
                            XSD_STRING => DataValueT::String(format!("\"{value}\"")),
                            _ => DataValueT::String(format!("\"{value}\"^^<{datatype}>")),
                        }
                    }
                }
            }
            Self::String => match gt {
                Term::StringLiteral(s) => DataValueT::String(format!("\"{s}\"")),
                Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
                    DataValueT::String(format!("\"{value}\"@{tag}"))
                }
                Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    ref value,
                    ref datatype,
                }) => match datatype.as_str() {
                    XSD_STRING => DataValueT::String(format!("\"{value}\"")),
                    _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
                },
                _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
            },
            Self::Integer => match gt {
                Term::NumericLiteral(NumericLiteral::Integer(i)) => DataValueT::I64(i),
                Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    ref value,
                    ref datatype,
                }) => match datatype.as_str() {
                    XSD_INTEGER => DataValueT::I64(
                        value
                            .parse()
                            .map_err(|_err| TypeError::InvalidRuleTermConversion(gt, *self))?,
                    ),
                    _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
                },
                _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
            },
            Self::Float64 => match gt {
                Term::NumericLiteral(NumericLiteral::Double(d)) => DataValueT::Double(d),
                Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
                    DataValueT::Double(Double::from_number(
                        format!("{a}.{b}").parse().unwrap()
                    ))
                }
                Term::NumericLiteral(NumericLiteral::Integer(a)) => {
                    DataValueT::Double(Double::from_number(a as f64))
                }
                Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    ref value,
                    ref datatype,
                }) => match datatype.as_str() {
                    XSD_DOUBLE | XSD_DECIMAL | XSD_INTEGER => DataValueT::Double(
                        value
                            .parse()
                            .ok()
                            .and_then(|d| Double::new(d).ok())
                            .ok_or(TypeError::InvalidRuleTermConversion(gt, *self))?,
                    ),
                    _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
                },
                _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
            },
        };

        Ok(result)
    }

    /// Whether this logical type can be used to perform numeric operations.
    pub fn allows_numeric_operations(&self) -> bool {
        match self {
            Self::Any => false,
            Self::String => false,
            Self::Integer => true,
            Self::Float64 => true,
        }
    }

    /// Wrap physical builder proxy into logical equivalent
    pub fn wrap_physical_column_builder<'a: 'b, 'b>(
        self,
        physical: &'b mut PhysicalBuilderProxyEnum<'a>,
    ) -> Box<dyn LogicalColumnBuilderProxy<'a, 'b> + 'b> {
        match self {
            Self::Any => Box::new(LogicalAnyColumnBuilderProxy::new(physical)),
            Self::String => Box::new(LogicalStringColumnBuilderProxy::new(physical)),
            Self::Integer => Box::new(LogicalIntegerColumnBuilderProxy::new(physical)),
            Self::Float64 => Box::new(LogicalFloat64ColumnBuilderProxy::new(physical)),
        }
    }
}

/// Errors that can occur during type checking
#[derive(Error, Debug)]
pub enum TypeError {
    /// Conflicting type declarations
    #[error("Conflicting type declarations. Predicate \"{0}\" at position {1} has been inferred to have the conflicting types {2} and {3}.")]
    InvalidRuleConflictingTypes(String, usize, LogicalTypeEnum, LogicalTypeEnum),
    /// Conflicting type conversions
    #[error("Conflicting type declarations. The term \"{0}\" cannot be converted to a {1}.")]
    InvalidRuleTermConversion(Term, LogicalTypeEnum),
    /// Comparison of a non-numeric type
    #[error("Invalid type declarations. Comparison operator can only be used with numeric types.")]
    InvalidRuleNonNumericComparison,
}
