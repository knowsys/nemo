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

trait LogicalTypeCollection {
    type LogicalTypeEnum: LogicalTypeEnum;

    fn parse(&self, str: &str) -> Result<Self::LogicalTypeEnum, String>; //TODO: error type shoudl probably not be a plain string...
}

trait LogicalTypeEnum {
    type LogicalTypeCollection: LogicalTypeCollection;

    fn get_type(&self) -> Self::LogicalTypeCollection;
}

trait LogicalType {
    type PhysicalType;

    fn from_physical(t: &Self::PhysicalType) -> Self;
    fn to_physical(&self) -> Self::PhysicalType;
}

macro_rules! generate_type_collection_and_enum {
    ($name_collection:ident, $name_enum:ident, $(($variant_name:ident, $content:ty)),+) => {
        enum $name_collection {
            $($variant_name),+
        }

        impl LogicalTypeCollection for $name_collection {
            type LogicalTypeEnum = $name_enum;

            fn parse(&self, str: &str) -> Result<Self::LogicalTypeEnum, String> {
                match self {
                    $(Self::$variant_name => <$content>::from_str(str).map(|ok| Self::LogicalTypeEnum::$variant_name(ok)).map_err(|err| err.to_string())),+
                }
            }
        }

        enum $name_enum {
            $($variant_name($content)),+
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
}

generate_type_collection_and_enum!(
    DefaultLogicalTypeCollection,
    DefaultLogicalTypeEnum,
    (GenericEverything, GenericEverything),
    (Integer, Integer<i64>)
);

#[derive(Debug)]
struct GenericEverything {
    physical: u64, // TODO: also include dictionary somehow
}

impl Display for GenericEverything {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.physical) // TODO: use dictionary
    }
}

impl FromStr for GenericEverything {
    type Err = <u64 as FromStr>::Err; // TODO: adjust this once we have dictionary

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

trait PhysicalNumber: Copy + Debug + Display + FromStr + Field + Ord {}
impl<T: Copy + Debug + Display + FromStr + Field + Ord> PhysicalNumber for T {}

trait LogicalNumber: LogicalType + Field + Ord {}
impl<T: LogicalType + Field + Ord> LogicalNumber for T {}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Integer<T>
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
