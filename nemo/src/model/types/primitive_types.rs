use std::fmt::Display;
use std::str::FromStr;

use crate::io::parser::ParseError;

use crate::model::NestedType;

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! generate_logical_type_enum {
    ($(($variant_name:ident, $string_repr: literal)),+) => {
        /// An enum capturing the logical type names and funtionality related to parsing and translating into and from physical types
        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
        pub(crate) enum PrimitiveType {
            $(
                /// $variant_name
                $variant_name
            ),+
        }

        impl PrimitiveType {
            const _VARIANTS: [Self; count!($($variant_name)+)] = [
                $(Self::$variant_name),+
            ];

            /// Returns a list of the syntactic representations of valid types.
            pub fn type_representations() -> Vec<&'static str> {
                vec![$($string_repr),+]
            }
        }

        impl Display for PrimitiveType {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Self::$variant_name => write!(f, "{}", $string_repr)),+
                }
            }
        }

        impl FromStr for PrimitiveType {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($string_repr => Ok(Self::$variant_name)),+,
                    _ => panic!("types no longer used, will soon go") //Err(Self::Err::ParseUnknownType(s.to_string()))
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

impl PartialOrd for PrimitiveType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self {
            Self::Any => match other {
                Self::Any => Some(std::cmp::Ordering::Equal),
                Self::String => Some(std::cmp::Ordering::Greater),
                _ => None, // TODO: should be the following once reasoning supports casting: Some(std::cmp::Ordering::Greater),
            },
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

impl Default for PrimitiveType {
    fn default() -> Self {
        Self::Any
    }
}

impl TryFrom<NestedType> for PrimitiveType {
    type Error = ();

    fn try_from(value: NestedType) -> Result<Self, Self::Error> {
        match value {
            NestedType::Tuple(_) => Err(()),
            NestedType::Primitive(p) => Ok(p),
        }
    }
}

impl PrimitiveType {
    // TODO: I think this should be the PartialCmp between types but as long as we do not
    // have casting we still want to forbid e.g. integer and string merges while reasoning
    // HOWEVER for data that occurs in facts or sources we can decide for the max type up
    // front and read them accordingly
    /// Get the more general type out of two types (not necessarily castable but can be used to
    /// determine how data should be read)
    pub fn max_type(&self, other: &PrimitiveType) -> PrimitiveType {
        match self {
            PrimitiveType::Any => PrimitiveType::Any,
            PrimitiveType::String => match other {
                PrimitiveType::String => PrimitiveType::String,
                _ => PrimitiveType::Any,
            },
            PrimitiveType::Integer => match other {
                PrimitiveType::Integer => PrimitiveType::Integer,
                _ => PrimitiveType::Any,
            },
            PrimitiveType::Float64 => match other {
                PrimitiveType::Float64 => PrimitiveType::Float64,
                _ => PrimitiveType::Any,
            },
        }
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
}
