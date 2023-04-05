//! This module defines logical types.

use std::fmt::Display;
use std::str::FromStr;

use crate::error::Error;

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! generate_logical_type_enum {
    ($($variant_name:ident),+) => {
        /// An enum capturing the logical type names and funtionality related to parsing and translating into and from physical types
        #[derive(Copy, Clone, Debug)]
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

generate_logical_type_enum!(RdfsResource, UnsignedInteger);
