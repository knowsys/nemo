//! This module defines logical types.

use std::fmt::Display;
use std::str::FromStr;

use crate::error::Error;
use crate::physical::datatypes::{DataTypeName, DataValueT};

use super::model::{Identifier, NumericLiteral, RdfLiteral, Term};

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
            type Err = Error;

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
    pub fn ground_term_to_data_value_t(&self, gt: Term) -> Result<DataValueT, Error> {
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
                _ => return Err(Error::InvalidRuleTermConversion(gt, *self)),
            },
            Self::Double => match gt {
                Term::NumericLiteral(NumericLiteral::Double(d)) => DataValueT::Double(d),
                _ => return Err(Error::InvalidRuleTermConversion(gt, *self)),
            },
        };

        Ok(result)
    }

    /// Whether this logical type can be used to perform numeric operations.
    pub fn allows_numeric_operations(&self) -> bool {
        match self {
            LogicalTypeEnum::RdfsResource => false,
            LogicalTypeEnum::UnsignedInteger => true,
            LogicalTypeEnum::Double => true,
        }
    }
}
