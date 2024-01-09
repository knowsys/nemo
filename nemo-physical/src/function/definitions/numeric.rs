//! This module defines numeric functions.

mod double;
mod float;
mod integer64;

use crate::{
    datatypes::{Double, Float},
    datavalues::{AnyDataValue, DataValue},
};

use self::{
    double::{
        numeric_absolute_double, numeric_addition_double, numeric_cos_double,
        numeric_division_double, numeric_greaterthan_double, numeric_greaterthaneq_double,
        numeric_lessthan_double, numeric_lessthaneq_double, numeric_logarithm_double,
        numeric_multiplication_double, numeric_negation_double, numeric_power_double,
        numeric_sin_double, numeric_squareroot_double, numeric_subtraction_double,
        numeric_tan_double,
    },
    float::{
        numeric_absolute_float, numeric_addition_float, numeric_cos_float, numeric_division_float,
        numeric_greaterthan_float, numeric_greaterthaneq_float, numeric_lessthan_float,
        numeric_lessthaneq_float, numeric_logarithm_float, numeric_multiplication_float,
        numeric_negation_float, numeric_power_float, numeric_sin_float, numeric_squareroot_float,
        numeric_subtraction_float, numeric_tan_float,
    },
    integer64::{
        numeric_absolute_integer64, numeric_addition_integer64, numeric_division_integer64,
        numeric_greaterthan_integer64, numeric_greaterthaneq_integer64, numeric_lessthan_integer64,
        numeric_lessthaneq_integer64, numeric_logarithm_integer64,
        numeric_multiplication_integer64, numeric_negation_integer64, numeric_power_integer64,
        numeric_squareroot_integer64, numeric_subtraction_integer64,
    },
};

use super::{BinaryFunction, UnaryFunction};

/// Numeric value
///
/// Types in this enum allow for numeric operations to be performed on them
enum NumericValue {
    Integer(i64),
    Float(Float),
    Double(Double),
}

impl NumericValue {
    fn from_any_datavalue(value: AnyDataValue) -> Option<NumericValue> {
        match value.value_domain() {
            crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::Boolean
            | crate::datavalues::ValueDomain::String
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Other
            | crate::datavalues::ValueDomain::Iri => None,
            crate::datavalues::ValueDomain::Float => Some(NumericValue::Float(Float::from_number(
                value.to_f32_unchecked(),
            ))),
            crate::datavalues::ValueDomain::Double => Some(NumericValue::Double(
                Double::from_number(value.to_f64_unchecked()),
            )),
            crate::datavalues::ValueDomain::UnsignedLong => None, // numeric, but cannot represented in NumericValue
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => {
                Some(NumericValue::Integer(value.to_i64_unchecked()))
            }
        }
    }
}

/// Defines a pair of values on which numeric functions are defined
enum NumericPair {
    Integer(i64, i64),
    Float(Float, Float),
    Double(Double, Double),
}

impl NumericPair {
    /// Checks if both parameters are numeric and of the same type.
    /// If this is the case, returns a pair of values of that type.
    /// Returs `None` otherwise.
    pub fn from_any_pair(
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<NumericPair> {
        match parameter_first.value_domain() {
            crate::datavalues::ValueDomain::String
            | crate::datavalues::ValueDomain::Null
            | crate::datavalues::ValueDomain::LanguageTaggedString
            | crate::datavalues::ValueDomain::Tuple
            | crate::datavalues::ValueDomain::Map
            | crate::datavalues::ValueDomain::Boolean
            | crate::datavalues::ValueDomain::Other
            | crate::datavalues::ValueDomain::Iri => None,
            crate::datavalues::ValueDomain::Float => Some(NumericPair::Float(
                Float::from_number(parameter_first.to_f32_unchecked()),
                Float::from_number(parameter_second.to_f32()?),
            )),
            crate::datavalues::ValueDomain::Double => Some(NumericPair::Double(
                Double::from_number(parameter_first.to_f64_unchecked()),
                Double::from_number(parameter_second.to_f64()?),
            )),
            crate::datavalues::ValueDomain::UnsignedLong => None, // numeric, but cannot be represented in StorageValues as used in NumericPair
            crate::datavalues::ValueDomain::NonNegativeLong
            | crate::datavalues::ValueDomain::UnsignedInt
            | crate::datavalues::ValueDomain::NonNegativeInt
            | crate::datavalues::ValueDomain::Long
            | crate::datavalues::ValueDomain::Int => Some(NumericPair::Integer(
                parameter_first.to_i64_unchecked(),
                parameter_second.to_i64()?,
            )),
        }
    }
}

/// Numeric addition
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericAddition;
impl BinaryFunction for NumericAddition {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_addition_integer64(first, second),
                NumericPair::Float(first, second) => numeric_addition_float(first, second),
                NumericPair::Double(first, second) => numeric_addition_double(first, second),
            };
        }

        None
    }
}

/// Numeric subtraction
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericSubtraction;
impl BinaryFunction for NumericSubtraction {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_subtraction_integer64(first, second),
                NumericPair::Float(first, second) => numeric_subtraction_float(first, second),
                NumericPair::Double(first, second) => numeric_subtraction_double(first, second),
            };
        }

        None
    }
}

/// Numeric multiplication
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericMultiplication;
impl BinaryFunction for NumericMultiplication {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => {
                    numeric_multiplication_integer64(first, second)
                }
                NumericPair::Float(first, second) => numeric_multiplication_float(first, second),
                NumericPair::Double(first, second) => numeric_multiplication_double(first, second),
            };
        }

        None
    }
}

/// Numeric division
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericDivision;
impl BinaryFunction for NumericDivision {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_division_integer64(first, second),
                NumericPair::Float(first, second) => numeric_division_float(first, second),
                NumericPair::Double(first, second) => numeric_division_double(first, second),
            };
        }

        None
    }
}

/// Logarithm of numeric values w.r.t. an arbitraty numeric base
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericLogarithm;
impl BinaryFunction for NumericLogarithm {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_logarithm_integer64(first, second),
                NumericPair::Float(first, second) => numeric_logarithm_float(first, second),
                NumericPair::Double(first, second) => numeric_logarithm_double(first, second),
            };
        }

        None
    }
}

/// Raising a numeric value to some power
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericPower;
impl BinaryFunction for NumericPower {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_power_integer64(first, second),
                NumericPair::Float(first, second) => numeric_power_float(first, second),
                NumericPair::Double(first, second) => numeric_power_double(first, second),
            };
        }

        None
    }
}

/// Absolute value of numeric values
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericAbsolute;
impl UnaryFunction for NumericAbsolute {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(value) => numeric_absolute_integer64(value),
                NumericValue::Float(value) => numeric_absolute_float(value),
                NumericValue::Double(value) => numeric_absolute_double(value),
            };
        }

        None
    }
}

/// Negation of a numeric value
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericNegation;
impl UnaryFunction for NumericNegation {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(value) => numeric_negation_integer64(value),
                NumericValue::Float(value) => numeric_negation_float(value),
                NumericValue::Double(value) => numeric_negation_double(value),
            };
        }

        None
    }
}

/// Square root of a numeric value
///
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericSquareroot;
impl UnaryFunction for NumericSquareroot {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(value) => numeric_squareroot_integer64(value),
                NumericValue::Float(value) => numeric_squareroot_float(value),
                NumericValue::Double(value) => numeric_squareroot_double(value),
            };
        }

        None
    }
}

/// Sine of a numeric value
///
/// Note: This operation is only defined for floating point numbers.
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericSine;
impl UnaryFunction for NumericSine {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_sin_float(value),
                NumericValue::Double(value) => numeric_sin_double(value),
            };
        }

        None
    }
}

/// Cosine of a numeric value
///
/// Note: This operation is only defined for floating point numbers.
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericCosine;
impl UnaryFunction for NumericCosine {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_cos_float(value),
                NumericValue::Double(value) => numeric_cos_double(value),
            };
        }

        None
    }
}

/// Tangent of a numeric value
///
/// Note: This operation is only defined for floating point numbers.
/// Does not return a value if the operation results in a value not representable by the type.
#[derive(Debug, Copy, Clone)]
pub struct NumericTangent;
impl UnaryFunction for NumericTangent {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(pair) = NumericValue::from_any_datavalue(parameter) {
            return match pair {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_tan_float(value),
                NumericValue::Double(value) => numeric_tan_double(value),
            };
        }

        None
    }
}

/// Less than comparison of two numbers
#[derive(Debug, Copy, Clone)]
pub struct NumericLessthan;
impl BinaryFunction for NumericLessthan {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_lessthan_integer64(first, second),
                NumericPair::Float(first, second) => numeric_lessthan_float(first, second),
                NumericPair::Double(first, second) => numeric_lessthan_double(first, second),
            };
        }

        None
    }
}

/// Less than or equals comparison of two numbers
#[derive(Debug, Copy, Clone)]
pub struct NumericLessthaneq;
impl BinaryFunction for NumericLessthaneq {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_lessthaneq_integer64(first, second),
                NumericPair::Float(first, second) => numeric_lessthaneq_float(first, second),
                NumericPair::Double(first, second) => numeric_lessthaneq_double(first, second),
            };
        }

        None
    }
}

/// Greater than comparison of two numbers
#[derive(Debug, Copy, Clone)]
pub struct NumericGreaterthan;
impl BinaryFunction for NumericGreaterthan {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_greaterthan_integer64(first, second),
                NumericPair::Float(first, second) => numeric_greaterthan_float(first, second),
                NumericPair::Double(first, second) => numeric_greaterthan_double(first, second),
            };
        }

        None
    }
}

/// Greater than or equals comparison of two numbers
#[derive(Debug, Copy, Clone)]
pub struct NumericGreaterthaneq;
impl BinaryFunction for NumericGreaterthaneq {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair(parameter_first, parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => {
                    numeric_greaterthaneq_integer64(first, second)
                }
                NumericPair::Float(first, second) => numeric_greaterthaneq_float(first, second),
                NumericPair::Double(first, second) => numeric_greaterthaneq_double(first, second),
            };
        }

        None
    }
}
