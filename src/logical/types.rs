//! This module contains traits for logical types and also some standard types.

use num::{One, Zero};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::{
    iter::{Product, Sum},
    ops::{Add, Div, Mul, Sub},
};

use crate::physical::datatypes::{
    DataTypeName, DataValueT, Double, Field, HasDataTypeName, WrappableInDataValueT,
};
use crate::physical::dictionary::Dictionary;

use super::model::{NumericLiteral, Term};

// TODO: Generally: support rdf types and have some kind of hierarchy
// TODO: allow type names to be specified as IRIs

/// Marker Trait for Type Parse Errors
pub trait LogicalTypeParseError: Debug + Display {
    /// Construct Error from string that was tried to parse and the supported data types that are tried to match
    fn new(failed_str: String, known_type_names: Vec<String>) -> Self;
}

#[derive(Debug)]
/// Default Logical Type Parse Error implementation holding just the string that could not be parsed and the known type names
pub struct DefaultLogicalTypeParseError {
    failed_str: String,
    known_type_names: Vec<String>,
}

impl Display for DefaultLogicalTypeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Failed to parse type {}. Known Types: {}",
            self.failed_str,
            self.known_type_names.join(", ")
        )
    }
}

impl LogicalTypeParseError for DefaultLogicalTypeParseError {
    fn new(failed_str: String, known_type_names: Vec<String>) -> Self {
        Self {
            failed_str,
            known_type_names,
        }
    }
}

/// Trait marking Enums representing a list of logical type names
pub trait LogicalTypeCollection:
    Copy + Clone + Debug + Default + Display + Eq + FromStr<Err = Self::ParseTypeNameErr>
{
    /// Type that is essentially <Self as FromStr>::Err but we need to set a trait bound on it which I don't know how to set otherwise
    type ParseTypeNameErr: LogicalTypeParseError;

    /// The corresponding enum that can hold the logical types in its variats
    type LogicalTypeEnum: LogicalTypeEnum;

    /// Parse string according to type respresented by self
    fn parse<Dict: Dictionary>(
        &self,
        s: &str,
        dict: &mut Dict,
    ) -> Result<Self::LogicalTypeEnum, String>; // TODO: error should not be a string

    /// Convert Ground Term according to type respresented by self
    fn convert_from_ground_term(&self, gt: &Term) -> Result<Self::LogicalTypeEnum, String>; // TODO: error should not be a string

    /// Convert DataValueT according to type respresented by self
    fn convert_from_data_value_t(&self, dvt: &DataValueT) -> Result<Self::LogicalTypeEnum, String>; // TODO: error should not be a string

    /// Get the corresponding physical data type name
    fn data_type_name(&self) -> DataTypeName;
}

/// Trait marking Enums wrapping a a list of logical types into respective variants
pub trait LogicalTypeEnum: Debug {
    /// The corresponding enum that only has the type names
    type LogicalTypeCollection: LogicalTypeCollection;

    /// Return name of the type of self
    fn get_type(&self) -> Self::LogicalTypeCollection;

    /// Get underlying physical value as DataValueT
    fn as_data_value_t(&self) -> DataValueT;

    /// Write type into string (inverse of parsing)
    fn write<Dict: Dictionary>(&self, dict: &Dict) -> String;
}

/// Trait of types in logical layer
pub trait LogicalType: Sized {
    /// Type in Physical layer representing this Logical Types
    type PhysicalType: HasDataTypeName;

    /// convert physical type into logical type
    fn from_physical(t: &Self::PhysicalType) -> Self;
    /// convert logical type into physical type
    fn to_physical(&self) -> Self::PhysicalType;

    /// Parse string according to type respresented by self
    fn parse<Dict: Dictionary>(s: &str, dict: &mut Dict) -> Result<Self, String>; // TODO: error should not be a string

    /// Convert Ground Term according to type respresented by self
    fn convert_from_ground_term(gt: &Term) -> Result<Self, String>; // TODO: error should not be a string

    /// Convert DataValueT according to type respresented by self
    fn convert_from_data_value_t(dvt: &DataValueT) -> Result<Self, String>; // TODO: error should not be a string

    /// Write type into string (inverse of parsing)
    fn write<Dict: Dictionary>(&self, dict: &Dict) -> String;
}

/// Generate Logical Type Enums by specifying how type names map to actual logical type implementations
#[macro_export]
macro_rules! generate_type_collection_and_enum {
    ([$collection_vis:vis] $name_collection:ident, [$enum_vis:vis] $name_enum:ident, $error_impl:ident, $(($variant_name:ident, $content:ty)),+, $default_name:ident) => {
        /// Generated Logical Type Collection Type $name_collection
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

        impl Default for $name_collection {
            fn default() -> Self {
                Self::$default_name
            }
        }

        impl FromStr for $name_collection {
            type Err = $error_impl;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $(stringify!($variant_name) => Ok(Self::$variant_name)),+,
                    _ => Err(Self::Err::new(s.to_string(), vec![$(stringify!($variant_name).to_string()),+]))
                }
            }
        }

        impl LogicalTypeCollection for $name_collection {
            type ParseTypeNameErr = <Self as FromStr>::Err;
            type LogicalTypeEnum = $name_enum;

            fn parse<Dict: Dictionary>(&self, s: &str, dict: &mut Dict) -> Result<Self::LogicalTypeEnum, String> { // TODO: error should not be a string
                match self {
                    $(Self::$variant_name => <$content>::parse(s, dict).map(|val| val.as_logical_type_enum())),+ // TODO: error should not be a string
                }
            }

            fn convert_from_ground_term(&self, gt: &Term) -> Result<Self::LogicalTypeEnum, String> { // TODO: error should not be a string
                match self {
                    $(Self::$variant_name => <$content>::convert_from_ground_term(gt).map(|val| val.as_logical_type_enum())),+ // TODO: error should not be a string
                }
            }

            fn convert_from_data_value_t(&self, dvt: &DataValueT) -> Result<Self::LogicalTypeEnum, String> { // TODO: error should not be a string
                match self {
                    $(Self::$variant_name => <$content>::convert_from_data_value_t(dvt).map(|val| val.as_logical_type_enum())),+ // TODO: error should not be a string
                }
            }

            fn data_type_name(&self) -> DataTypeName {
                match self {
                    $(Self::$variant_name => <$content as LogicalType>::PhysicalType::data_type_name()),+
                }
            }
        }

        /// Generated Logical Type Enum Type $name_enum
        #[derive(Copy, Clone, Debug)]
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

            fn as_data_value_t(&self) -> DataValueT {
                match self {
                    $(Self::$variant_name(val) => val.to_physical().wrap_in_data_value_t()),+
                }
            }

            fn write<Dict: Dictionary>(&self, dict: &Dict) -> String {
                match self {
                    $(Self::$variant_name(val) => val.write(dict)),+
                }
            }
        }

        $(
        impl $content {
            fn as_logical_type_enum(&self) -> $name_enum {
                $name_enum::$variant_name(*self)
            }
        }
        )+
    };
    ($name_collection:ident, $name_enum:ident, $(($variant_name:ident, $content:ty)),+) => {
        generate_type_collection_and_enum!([pub(self)] $name_collection, [pub(self)] $name_enum, $(($variant_name, $content)),+);
    };
}

// TODO: can we somehow mark GenericEverything as Default in the future to allow to NOT specify type declarations in rules?
generate_type_collection_and_enum!(
    [pub] DefaultLogicalTypeCollection,
    [pub] DefaultLogicalTypeEnum,
    DefaultLogicalTypeParseError,
    (GenericEverything, GenericEverything),
    (UnsignedInteger, Number<u64>),
    (Double, Number<Double>),
    GenericEverything
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

impl LogicalType for GenericEverything {
    type PhysicalType = u64;

    fn from_physical(t: &Self::PhysicalType) -> Self {
        Self { physical: *t }
    }

    fn to_physical(&self) -> Self::PhysicalType {
        self.physical
    }

    fn parse<Dict: Dictionary>(s: &str, dict: &mut Dict) -> Result<Self, String> {
        let idx_in_dict = dict.add(s.to_string());

        Ok(Self {
            physical: idx_in_dict.try_into().expect(
                "number of elements in dictionary should not overflow 64 bit unsinged integers",
            ),
        })
    }

    fn convert_from_ground_term(gt: &Term) -> Result<Self, String> {
        match gt {
            Term::Constant(constant) => Ok(Self::from_physical(&constant.to_constant_u64())),
            // TODO: handle everything
            _ => Err("only expecting constant in GenericEverything logical type at the moment (although we should support pretty much everything here)".to_string()),
        }
    }

    fn convert_from_data_value_t(dvt: &DataValueT) -> Result<Self, String> {
        match dvt {
            DataValueT::U64(constant) => Ok(Self::from_physical(constant)),
            // TODO: handle everything
            _ => Err("only expecting constant in GenericEverything logical type at the moment (although we should support pretty much everything here)".to_string()),
        }
    }

    fn write<Dict: Dictionary>(&self, dict: &Dict) -> String {
        let dict_idx = self.physical;
        dict.entry(dict_idx.try_into().expect("U64 should also fit into usize"))
            .unwrap_or_else(|| format!("<{dict_idx} should have been interned>"))
    }
}

/// Trait summing up what a number should be able to do in physical layer
pub trait PhysicalNumber: Copy + Debug + Display + FromStr + Field + Ord + HasDataTypeName {}
impl<T: Copy + Debug + Display + FromStr + Field + Ord + HasDataTypeName> PhysicalNumber for T {}

/// Trait summing up what a number should be able to do in logical layer
trait LogicalNumber: LogicalType + Field + Ord {}
impl<T: LogicalType + Field + Ord> LogicalNumber for T {}

/// Generic Logical Number that needs to specify its actual type when instantiated, e.g. i64
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Number<T>
where
    T: PhysicalNumber,
{
    physical: T,
}

impl<T: PhysicalNumber> Display for Number<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.physical)
    }
}

impl<T: PhysicalNumber> FromStr for Number<T> {
    type Err = <T as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(|int| Self { physical: int })
    }
}

impl LogicalType for Number<u64> {
    type PhysicalType = u64;

    fn from_physical(t: &Self::PhysicalType) -> Self {
        Self { physical: *t }
    }

    fn to_physical(&self) -> Self::PhysicalType {
        self.physical
    }

    fn parse<Dict: Dictionary>(s: &str, _dict: &mut Dict) -> Result<Self, String> {
        s.parse()
            .map_err(|err: <Self as FromStr>::Err| err.to_string())
    }

    // TODO: fix those conversions everywhere
    fn convert_from_ground_term(gt: &Term) -> Result<Self, String> {
        match gt {
            Term::NumericLiteral(NumericLiteral::Integer(int)) => Ok(Self::from_physical(
                &(*int)
                    .try_into()
                    .expect("we do not support singed integer at the moment"),
            )),
            // TODO: handle everything
            _ => Err("only expecting numeric literal in Number logical type".to_string()),
        }
    }

    fn convert_from_data_value_t(dvt: &DataValueT) -> Result<Self, String> {
        match dvt {
            DataValueT::U64(int) => Ok(Self::from_physical(int)),
            // TODO: handle everything
            _ => Err("only expecting numeric literal in Number logical type".to_string()),
        }
    }

    fn write<Dict: Dictionary>(&self, _dict: &Dict) -> String {
        self.to_string()
    }
}

impl LogicalType for Number<Double> {
    type PhysicalType = Double;

    fn from_physical(t: &Self::PhysicalType) -> Self {
        Self { physical: *t }
    }

    fn to_physical(&self) -> Self::PhysicalType {
        self.physical
    }

    fn parse<Dict: Dictionary>(s: &str, _dict: &mut Dict) -> Result<Self, String> {
        s.parse()
            .map_err(|err: <Self as FromStr>::Err| err.to_string())
    }

    fn convert_from_ground_term(gt: &Term) -> Result<Self, String> {
        match gt {
            Term::NumericLiteral(NumericLiteral::Double(double)) => Ok(Self::from_physical(double)),
            // TODO: handle everything
            _ => Err("only expecting numeric literal in Number logical type".to_string()),
        }
    }

    fn convert_from_data_value_t(dvt: &DataValueT) -> Result<Self, String> {
        match dvt {
            DataValueT::Double(double) => Ok(Self::from_physical(double)),
            // TODO: handle everything
            _ => Err("only expecting numeric literal in Number logical type".to_string()),
        }
    }

    fn write<Dict: Dictionary>(&self, _dict: &Dict) -> String {
        self.to_string()
    }
}

impl<T: PhysicalNumber> Add for Number<T> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.add(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Sub for Number<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.sub(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Mul for Number<T> {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.mul(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Div for Number<T> {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        Self {
            physical: self.physical.div(rhs.physical),
        }
    }
}

impl<T: PhysicalNumber> Sum for Number<T> {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Self {
            physical: iter.map(|i| i.physical).sum(),
        }
    }
}

impl<T: PhysicalNumber> Product for Number<T> {
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Self {
            physical: iter.map(|i| i.physical).product(),
        }
    }
}

impl<T: PhysicalNumber> Zero for Number<T> {
    fn zero() -> Self {
        Self {
            physical: T::zero(),
        }
    }

    fn is_zero(&self) -> bool {
        self.physical.is_zero()
    }
}

impl<T: PhysicalNumber> One for Number<T> {
    fn one() -> Self {
        Self { physical: T::one() }
    }

    fn is_one(&self) -> bool {
        self.physical.is_one()
    }
}
