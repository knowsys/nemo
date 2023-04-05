//! This module defines logical types.

use std::fmt::Display;
use std::str::FromStr;

use crate::error::Error;
use crate::physical::datatypes::DataTypeName;

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

        impl Default for LogicalTypeEnum {
            fn default() -> Self {
                Self::RdfsResource
            }
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

generate_logical_type_enum!(RdfsResource, UnsignedInteger);

impl From<LogicalTypeEnum> for DataTypeName {
    fn from(source: LogicalTypeEnum) -> Self {
        match source {
            LogicalTypeEnum::RdfsResource => Self::String,
            LogicalTypeEnum::UnsignedInteger => Self::U64,
        }
    }
}
