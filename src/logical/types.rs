//! This module defines logical types.

use std::fmt::Display;
use std::str::FromStr;

use crate::io::parser::ParseError;
use crate::physical::datatypes::{DataTypeName, DataValueT};

use super::model::{Identifier, NumericLiteral, RdfLiteral, Term};

use thiserror::Error;

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! generate_logical_type_enum {
    ($($variant_name:ident),+) => {
        /// An enum capturing the logical type names and funtionality related to parsing and translating into and from physical types
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum LogicalTypeEnum {
            $(
                /// $variant_name
                $variant_name
            ),+
        }

        impl LogicalTypeEnum {
            const VARIANTS: [Self; count!($($variant_name)+)] = [
                $(Self::$variant_name),+
            ];
        }

        impl Display for LogicalTypeEnum {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Self::$variant_name => write!(f, stringify!($variant_name))),+
                }
            }
        }

        impl FromStr for LogicalTypeEnum {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $(stringify!($variant_name) => Ok(Self::$variant_name)),+,
                    _ => Err(Self::Err::ParseUnknownType(s.to_string(), Self::VARIANTS.into()))
                }
            }
        }
    };
}

generate_logical_type_enum!(RdfsResource, UnsignedInteger, Double);

impl Default for LogicalTypeEnum {
    fn default() -> Self {
        Self::RdfsResource
    }
}

impl From<LogicalTypeEnum> for DataTypeName {
    fn from(source: LogicalTypeEnum) -> Self {
        match source {
            LogicalTypeEnum::RdfsResource => Self::String,
            LogicalTypeEnum::UnsignedInteger => Self::U64,
            LogicalTypeEnum::Double => Self::Double,
        }
    }
}

impl LogicalTypeEnum {
    /// Convert a given ground term to a DataValueT fitting the current logical type
    pub fn ground_term_to_data_value_t(&self, gt: Term) -> Result<DataValueT, TypeError> {
        let result = match self {
            Self::RdfsResource => {
                match gt {
                    Term::Variable(_) => {
                        panic!("Expecting ground term for conversion to DataValueT")
                    }
                    Term::Constant(Identifier(s)) => DataValueT::String(s),
                    // TODO: maybe implement display on numeric literal instead?
                    Term::NumericLiteral(NumericLiteral::Integer(i)) => {
                        DataValueT::String(i.to_string())
                    }
                    Term::NumericLiteral(NumericLiteral::Decimal(a, b)) => {
                        DataValueT::String(format!("{a}.{b}"))
                    }
                    Term::NumericLiteral(NumericLiteral::Double(d)) => {
                        DataValueT::String(d.to_string())
                    }
                    Term::RdfLiteral(RdfLiteral::LanguageString { value, tag }) => {
                        DataValueT::String(format!("{value}@{tag}"))
                    }
                    Term::RdfLiteral(RdfLiteral::DatatypeValue { value, datatype }) => {
                        DataValueT::String(format!("{value}^^{datatype}"))
                    }
                }
            }
            Self::UnsignedInteger => match gt {
                Term::NumericLiteral(NumericLiteral::Integer(i)) => {
                    DataValueT::U64(i.try_into().unwrap())
                }
                _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
            },
            Self::Double => match gt {
                Term::NumericLiteral(NumericLiteral::Double(d)) => DataValueT::Double(d),
                _ => return Err(TypeError::InvalidRuleTermConversion(gt, *self)),
            },
        };

        Ok(result)
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
}
