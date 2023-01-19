//! This module contains traits for logical types and also some standard types.

use num::{One, Zero};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::{
    iter::{Product, Sum},
    ops::{Add, Div, Mul, Sub},
};

use crate::physical::datatypes::Field;

// TODO: have type for everything (rdfs:resource)
// Generally: support rdf types

/// Trait marking Enums representing a list of logical type names
pub trait LogicalTypeCollection: Clone + Debug + Display + FromStr {
    /// The corresponding enum that can hold the logical types in its variats
    type LogicalTypeEnum: LogicalTypeEnum;

    /// Parse string according into type respresented by self
    fn parse(&self, s: &str) -> Result<Self::LogicalTypeEnum, String>; //TODO: error type should probably not be a plain string...
}

/// Trait marking Enums wrapping a a list of logical types into respective variants
pub trait LogicalTypeEnum: Debug {
    /// The corresponding enum that only has the type names
    type LogicalTypeCollection: LogicalTypeCollection;

    /// Return name of the type of self
    fn get_type(&self) -> Self::LogicalTypeCollection;
}

/// Trait of types in logical layer
pub trait LogicalType {
    /// Type in Physical layer representing this Logical Types
    type PhysicalType;

    /// convert physical type into logical type
    fn from_physical(t: &Self::PhysicalType) -> Self;
    /// convert logical type into physical type
    fn to_physical(&self) -> Self::PhysicalType;
}

/// Generate Logical Type Enums by specifying how type names map to actual logical type implementations
#[macro_export]
macro_rules! generate_type_collection_and_enum {
    ([$collection_vis:vis] $name_collection:ident, [$enum_vis:vis] $name_enum:ident, $(($variant_name:ident, $content:ty)),+) => {
        /// Generated Logical Type Collection Type $name_collection
        #[derive(Copy, Clone, Debug)]
        $collection_vis enum $name_collection {
            $(
                /// $variant_name
                $variant_name
            ),+
        }

        impl Display for $name_collection {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Self::$variant_name => write!(f, stringify!($variant_name))),+
                }
            }
        }

        impl FromStr for $name_collection {
            type Err = String; // TODO: error type should probably not be a plain string...

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $(stringify!($variant_name) => Ok(Self::$variant_name)),+,
                    _ => Err(format!("Type {s} not known!"))
                }
            }
        }

        impl LogicalTypeCollection for $name_collection {
            type LogicalTypeEnum = $name_enum;

            fn parse(&self, s: &str) -> Result<Self::LogicalTypeEnum, String> {
                match self {
                    $(Self::$variant_name => <$content>::from_str(s).map(|ok| Self::LogicalTypeEnum::$variant_name(ok)).map_err(|err| err.to_string())),+
                }
            }
        }

        /// Generated Logical Type Enum Type $name_enum
        #[derive(Debug)]
        $enum_vis enum $name_enum {
            $(
                /// $variant_name with $content
                $variant_name($content)
            ),+
        }

        impl LogicalTypeEnum for $name_enum {
            type LogicalTypeCollection = $name_collection;

            fn get_type(&self) -> Self::LogicalTypeCollection {
                match self {
                    $(Self::$variant_name(_) => Self::LogicalTypeCollection::$variant_name),+
                }
            }
        }
    };
    ($name_collection:ident, $name_enum:ident, $(($variant_name:ident, $content:ty)),+) => {
        generate_type_collection_and_enum!([pub(self)] $name_collection, [pub(self)] $name_enum, $(($variant_name, $content)),+);
    };
}

generate_type_collection_and_enum!(
    [pub] DefaultLogicalTypeCollection,
    [pub] DefaultLogicalTypeEnum,
    (GenericEverything, GenericEverything),
    (Integer, Integer<i64>)
);

/// Type similar to rdfs:resource able to capture every value as a string
#[derive(Copy, Clone, Debug)]
pub struct GenericEverything {
    physical: u64, // TODO: also include dictionary somehow
}

impl Display for GenericEverything {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.physical) // TODO: use dictionary
    }
}

impl FromStr for GenericEverything {
    type Err = <u64 as FromStr>::Err; // TODO: adjust this once we have dictionary

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Ok(Self { physical: 0 }) // TODO: use dictionary
    }
}

impl LogicalType for GenericEverything {
    type PhysicalType = u64;

    fn from_physical(t: &Self::PhysicalType) -> Self {
        Self { physical: *t }
    }

    fn to_physical(&self) -> Self::PhysicalType {
        self.physical
    }
}

/// Trait summing up what a number should be able to do in physical layer
pub trait PhysicalNumber: Copy + Debug + Display + FromStr + Field + Ord {}
impl<T: Copy + Debug + Display + FromStr + Field + Ord> PhysicalNumber for T {}

/// Trait summing up what a number should be able to do in logical layer
trait LogicalNumber: LogicalType + Field + Ord {}
impl<T: LogicalType + Field + Ord> LogicalNumber for T {}

/// Generic Logical Integer that needs to specify its actual type when instantiated, e.g. i64
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Integer<T>
where
    T: PhysicalNumber,
{
    physical: T,
}

impl<T: PhysicalNumber> Display for Integer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.physical)
    }
}

impl<T: PhysicalNumber> FromStr for Integer<T> {
    type Err = <T as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(|int| Self { physical: int })
    }
}

impl<T: PhysicalNumber> LogicalType for Integer<T> {
    type PhysicalType = T;

    fn from_physical(t: &Self::PhysicalType) -> Self {
        Self { physical: *t }
    }

    fn to_physical(&self) -> Self::PhysicalType {
        self.physical
    }
}

impl<T: PhysicalNumber> Add for Integer<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.add(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Sub for Integer<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.sub(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Mul for Integer<T> {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.mul(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Div for Integer<T> {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.div(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Sum for Integer<T> {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Self {
            physical: iter.map(|i| i.physical).sum(),
        }
    }
}

impl<T: PhysicalNumber> Product for Integer<T> {
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Self {
            physical: iter.map(|i| i.physical).product(),
        }
    }
}

impl<T: PhysicalNumber> Zero for Integer<T> {
    fn zero() -> Self {
        Self {
            physical: T::zero(),
        }
    }

    fn is_zero(&self) -> bool {
        self.physical.is_zero()
    }
}

impl<T: PhysicalNumber> One for Integer<T> {
    fn one() -> Self {
        Self { physical: T::one() }
    }

    fn is_one(&self) -> bool {
        self.physical.is_one()
    }
}
