use std::fmt::Display;
use std::str::FromStr;

use crate::builder_proxy::{
    LogicalAnyColumnBuilderProxy, LogicalColumnBuilderProxyT, LogicalFloat64ColumnBuilderProxy,
    LogicalIntegerColumnBuilderProxy, LogicalStringColumnBuilderProxy,
};
use crate::io::parser::ParseError;
use nemo_physical::builder_proxy::PhysicalBuilderProxyEnum;
use nemo_physical::datatypes::data_value::DataValueIteratorT;
use nemo_physical::datatypes::{DataTypeName, DataValueT};

use super::error::InvalidRuleTermConversion;
use super::primitive_logical_value::{
    AnyOutputMapper, DefaultSerializedIterator, Float64OutputMapper, IntegerOutputMapper,
    PrimitiveLogicalValueIteratorT, StringOutputMapper,
};
use crate::model::{Constant, NestedType};

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! generate_logical_type_enum {
    ($(($variant_name:ident, $string_repr: literal)),+) => {
        /// An enum capturing the logical type names and funtionality related to parsing and translating into and from physical types
        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
        pub enum PrimitiveType {
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

impl PartialOrd for PrimitiveType {
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

impl Default for PrimitiveType {
    fn default() -> Self {
        Self::Any
    }
}

impl From<PrimitiveType> for DataTypeName {
    fn from(source: PrimitiveType) -> Self {
        match source {
            PrimitiveType::Any => Self::String,
            PrimitiveType::String => Self::String,
            PrimitiveType::Integer => Self::I64,
            PrimitiveType::Float64 => Self::Double,
        }
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

    /// Convert a given ground term to a DataValueT fitting the current logical type
    pub fn ground_term_to_data_value_t(
        &self,
        constant: Constant,
    ) -> Result<DataValueT, InvalidRuleTermConversion> {
        let result = match self {
            Self::Any => DataValueT::String(constant.try_into()?),
            Self::String => DataValueT::String(constant.try_into()?),
            Self::Integer => DataValueT::I64(constant.try_into()?),
            Self::Float64 => DataValueT::Double(constant.try_into()?),
        };

        Ok(result)
    }

    /// Associate a physical data type to this primitive logical type
    pub fn datatype_name(&self) -> DataTypeName {
        match self {
            PrimitiveType::Any => DataTypeName::String,
            PrimitiveType::String => DataTypeName::String,
            PrimitiveType::Integer => DataTypeName::I64,
            PrimitiveType::Float64 => DataTypeName::Double,
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

    /// Wrap physical builder proxy into logical equivalent
    pub fn wrap_physical_column_builder<'a: 'b, 'b>(
        self,
        physical: &'b mut PhysicalBuilderProxyEnum<'a>,
    ) -> LogicalColumnBuilderProxyT<'a, 'b> {
        match self {
            Self::Any => {
                LogicalColumnBuilderProxyT::Any(LogicalAnyColumnBuilderProxy::new(physical))
            }
            Self::String => {
                LogicalColumnBuilderProxyT::String(LogicalStringColumnBuilderProxy::new(physical))
            }
            Self::Integer => {
                LogicalColumnBuilderProxyT::Integer(LogicalIntegerColumnBuilderProxy::new(physical))
            }
            Self::Float64 => {
                LogicalColumnBuilderProxyT::Float64(LogicalFloat64ColumnBuilderProxy::new(physical))
            }
        }
    }

    /// Convert physical data value iterator into iterator over logical types
    pub fn primitive_logical_value_iterator<'a>(
        &self,
        physical_iter: DataValueIteratorT<'a>,
    ) -> PrimitiveLogicalValueIteratorT<'a> {
        match self {
            Self::Any => {
                PrimitiveLogicalValueIteratorT::Any(AnyOutputMapper::new(physical_iter).into())
            }
            Self::String => PrimitiveLogicalValueIteratorT::String(
                StringOutputMapper::new(physical_iter).into(),
            ),
            Self::Integer => PrimitiveLogicalValueIteratorT::Integer(
                IntegerOutputMapper::new(physical_iter).into(),
            ),
            Self::Float64 => PrimitiveLogicalValueIteratorT::Float64(
                Float64OutputMapper::new(physical_iter).into(),
            ),
        }
    }

    /// Convert physical data value iterator into iterator over logical types directly transformed
    /// to their string representation to be able to take some shortcuts when performing this mapping
    pub fn serialize_output<'a>(
        &self,
        physical_iter: DataValueIteratorT<'a>,
    ) -> DefaultSerializedIterator<'a> {
        match self {
            Self::Any => AnyOutputMapper::new(physical_iter).into(),
            Self::String => StringOutputMapper::new(physical_iter).into(),
            Self::Integer => IntegerOutputMapper::new(physical_iter).into(),
            Self::Float64 => Float64OutputMapper::new(physical_iter).into(),
        }
    }
}
