//! This module defines numeric functions.

mod double;
mod float;
mod integer64;

pub mod traits;

use crate::{
    datavalues::{AnyDataValue, DataValue, ValueDomain},
    storagevalues::{Double, Float, StorageTypeName},
};

use self::{
    double::{
        numeric_absolute_double, numeric_addition_double, numeric_ceil_double, numeric_cos_double,
        numeric_division_double, numeric_floor_double, numeric_greaterthan_double,
        numeric_greaterthaneq_double, numeric_lessthan_double, numeric_lessthaneq_double,
        numeric_logarithm_double, numeric_maximum_double, numeric_minimum_double,
        numeric_multiplication_double, numeric_negation_double, numeric_power_double,
        numeric_product_double, numeric_remainder_double, numeric_round_double, numeric_sin_double,
        numeric_squareroot_double, numeric_subtraction_double, numeric_sum_double,
        numeric_tan_double, numeric_tnorm_lukasiewicz_double,
    },
    float::{
        numeric_absolute_float, numeric_addition_float, numeric_ceil_float, numeric_cos_float,
        numeric_division_float, numeric_floor_float, numeric_greaterthan_float,
        numeric_greaterthaneq_float, numeric_lessthan_float, numeric_lessthaneq_float,
        numeric_logarithm_float, numeric_maximum_float, numeric_minimum_float,
        numeric_multiplication_float, numeric_negation_float, numeric_power_float,
        numeric_product_float, numeric_remainder_float, numeric_round_float, numeric_sin_float,
        numeric_squareroot_float, numeric_subtraction_float, numeric_sum_float, numeric_tan_float,
        numeric_tnorm_lukasiewicz_float,
    },
    integer64::{
        numeric_absolute_integer64, numeric_addition_integer64, numeric_bitwise_and,
        numeric_bitwise_or, numeric_bitwise_shl, numeric_bitwise_shr, numeric_bitwise_shru,
        numeric_bitwise_xor, numeric_division_integer64, numeric_greaterthan_integer64,
        numeric_greaterthaneq_integer64, numeric_lessthan_integer64, numeric_lessthaneq_integer64,
        numeric_logarithm_integer64, numeric_maximum_integer64, numeric_minimum_integer64,
        numeric_multiplication_integer64, numeric_negation_integer64, numeric_power_integer64,
        numeric_product_integer64, numeric_remainder_integer64, numeric_squareroot_integer64,
        numeric_subtraction_integer64, numeric_sum_integer64,
    },
};

use super::{BinaryFunction, FunctionTypePropagation, NaryFunction, UnaryFunction};

/// Numeric value
///
/// Types in this enum allow for numeric operations to be performed on them
enum NumericValue {
    Integer(i64),
    Float(Float),
    Double(Double),
}

impl NumericValue {
    fn from_any_datavalue(value: &AnyDataValue) -> Option<NumericValue> {
        match value.value_domain() {
            ValueDomain::Tuple
            | ValueDomain::Map
            | ValueDomain::Null
            | ValueDomain::Boolean
            | ValueDomain::PlainString
            | ValueDomain::LanguageTaggedString
            | ValueDomain::Other
            | ValueDomain::Iri => None,
            ValueDomain::Float => Some(NumericValue::Float(Float::from_number(
                value.to_f32_unchecked(),
            ))),
            ValueDomain::Double => Some(NumericValue::Double(Double::from_number(
                value.to_f64_unchecked(),
            ))),
            ValueDomain::UnsignedLong => None, // numeric, but cannot represented in NumericValue
            ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int => Some(NumericValue::Integer(value.to_i64_unchecked())),
        }
    }

    /// Convert numeric value into [Double] (if needed).
    fn convert_to_double(&self) -> Double {
        match self {
            NumericValue::Integer(value) => {
                Double::new(*value as f64).expect("value must be finite")
            }
            NumericValue::Float(value) => {
                Double::new(f32::from(*value) as f64).expect("value must be finite")
            }
            NumericValue::Double(value) => *value,
        }
    }

    /// Return `true` if `other` is of the same type and `false` otherwise.
    fn same_type(&self, other: &Self) -> bool {
        match self {
            NumericValue::Integer(_) => matches!(other, Self::Integer(_)),
            NumericValue::Float(_) => matches!(other, Self::Float(_)),
            NumericValue::Double(_) => matches!(other, Self::Double(_)),
        }
    }

    /// Return the integer contained or `None` if this is not an integer.
    fn integer(&self) -> Option<i64> {
        if let Self::Integer(result) = self {
            Some(*result)
        } else {
            None
        }
    }

    /// Return the float contained or `None` if this is not an float.
    fn float(&self) -> Option<Float> {
        if let Self::Float(result) = self {
            Some(*result)
        } else {
            None
        }
    }

    /// Return the double contained or `None` if this is not an double.
    fn double(&self) -> Option<Double> {
        if let Self::Double(result) = self {
            Some(*result)
        } else {
            None
        }
    }

    /// Return the integer contained in this object.
    ///
    /// # Panics
    /// Panics if this value is not an integer.
    fn integer_unchecked(&self) -> i64 {
        if let Self::Integer(result) = self {
            *result
        } else {
            unreachable!("expected the value to be an integer");
        }
    }

    /// Return the float contained in this object.
    ///
    /// # Panics
    /// Panics if this value is not a float.
    fn float_unchecked(&self) -> Float {
        if let Self::Float(result) = self {
            *result
        } else {
            unreachable!("expected the value to be an float");
        }
    }

    /// Return the double contained in this object.
    ///
    /// # Panics
    /// Panics if this value is not a double.
    fn double_unchecked(&self) -> Double {
        if let Self::Double(result) = self {
            *result
        } else {
            unreachable!("expected the value to be an double");
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
    /// Return a [NumericPair] or `None` if parameters are not of the same numeric type.
    pub fn _from_any_pair(
        parameter_first: &AnyDataValue,
        parameter_second: &AnyDataValue,
    ) -> Option<NumericPair> {
        let value_first = NumericValue::from_any_datavalue(parameter_first)?;
        let value_second = NumericValue::from_any_datavalue(parameter_second)?;

        Some(match value_first {
            NumericValue::Integer(value) => {
                NumericPair::Integer(value, value_second.integer_unchecked())
            }
            NumericValue::Float(value) => NumericPair::Float(value, value_second.float_unchecked()),
            NumericValue::Double(value) => {
                NumericPair::Double(value, value_second.double_unchecked())
            }
        })
    }

    /// Return a [NumericPair] or `None` if one of the parameters is not numeric.
    ///
    /// In the case where the parameters are of different type,
    /// they will both be casted to [Double].
    pub fn from_any_pair_cast(
        parameter_first: &AnyDataValue,
        parameter_second: &AnyDataValue,
    ) -> Option<NumericPair> {
        let value_first = NumericValue::from_any_datavalue(parameter_first)?;
        let value_second = NumericValue::from_any_datavalue(parameter_second)?;

        Some(if value_first.same_type(&value_second) {
            match value_first {
                NumericValue::Integer(value) => {
                    NumericPair::Integer(value, value_second.integer_unchecked())
                }
                NumericValue::Float(value) => {
                    NumericPair::Float(value, value_second.float_unchecked())
                }
                NumericValue::Double(value) => {
                    NumericPair::Double(value, value_second.double_unchecked())
                }
            }
        } else {
            NumericPair::Double(
                value_first.convert_to_double(),
                value_second.convert_to_double(),
            )
        })
    }
}

/// Defines a list of values on which numeric functions are defined
enum NumericList {
    Integer(Vec<i64>),
    Float(Vec<Float>),
    Double(Vec<Double>),
}

impl NumericList {
    /// Collects a list of [NumericValue] into a [NumericList].
    /// Returns `None` if not all elements of the list are of the same type.
    fn collect_list_same_type(values: Vec<NumericValue>) -> Option<NumericList> {
        Some(match values[0] {
            NumericValue::Integer(_) => NumericList::Integer(
                values
                    .into_iter()
                    .map(|value| value.integer())
                    .collect::<Option<Vec<i64>>>()?,
            ),
            NumericValue::Float(_) => NumericList::Float(
                values
                    .into_iter()
                    .map(|value| value.float())
                    .collect::<Option<Vec<Float>>>()?,
            ),
            NumericValue::Double(_) => NumericList::Double(
                values
                    .into_iter()
                    .map(|value| value.double())
                    .collect::<Option<Vec<Double>>>()?,
            ),
        })
    }

    /// Return a [NumericList] if all value of the same numeric type and `None` otherwise.
    pub fn from_any_list(parameters: &[AnyDataValue]) -> Option<NumericList> {
        if parameters.is_empty() {
            return None;
        }

        let values = parameters
            .iter()
            .map(NumericValue::from_any_datavalue)
            .collect::<Option<Vec<_>>>()?;

        Self::collect_list_same_type(values)
    }

    /// Return a [NumericList] and `None` if one of the input parameters is not numeric.
    ///
    /// If input parameters are of different numeric types then they will be cast to [Double].
    pub fn from_any_list_cast(parameters: &[AnyDataValue]) -> Option<NumericList> {
        if parameters.is_empty() {
            return None;
        }

        let values = parameters
            .iter()
            .map(NumericValue::from_any_datavalue)
            .collect::<Option<Vec<_>>>()?;

        let first_value = &values[0];
        let same_type = values.iter().all(|value| value.same_type(first_value));

        Some(if same_type {
            Self::collect_list_same_type(values).expect("values should be of the same type")
        } else {
            let values = values
                .into_iter()
                .map(|value| NumericValue::Double(value.convert_to_double()))
                .collect::<Vec<_>>();
            Self::collect_list_same_type(values).expect("values should all be of type double")
        })
    }
}

/// Numeric addition
///
/// Returns the sum of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericAddition;
impl BinaryFunction for NumericAddition {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_addition_integer64(first, second),
                NumericPair::Float(first, second) => numeric_addition_float(first, second),
                NumericPair::Double(first, second) => numeric_addition_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric subtraction
///
/// Returns the difference between the first and the second parameter.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericSubtraction;
impl BinaryFunction for NumericSubtraction {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_subtraction_integer64(first, second),
                NumericPair::Float(first, second) => numeric_subtraction_float(first, second),
                NumericPair::Double(first, second) => numeric_subtraction_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric multiplication
///
/// Returns the product of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericMultiplication;
impl BinaryFunction for NumericMultiplication {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric division
///
/// Returns the quotient resulting from dividing the first parameter by the second.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericDivision;
impl BinaryFunction for NumericDivision {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_division_integer64(first, second),
                NumericPair::Float(first, second) => numeric_division_float(first, second),
                NumericPair::Double(first, second) => numeric_division_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Bitwise and
///
/// For a list of integers,
/// returns the integer resulting from perfoming an "and" on their bit representation.
///
/// Returns `None` if the input parameters are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitAnd;
impl NaryFunction for BitAnd {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list(parameters) {
            match list {
                NumericList::Integer(integers) => numeric_bitwise_and(&integers),
                _ => None,
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Bitwise or
///
/// For a list of integers,
/// returns the integer resulting from perfoming an "or" on their bit representation.
///
/// Returns the zero from the integer value space if no parameters are given.
/// Returns `None` if the input parameters are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitOr;
impl NaryFunction for BitOr {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list(parameters) {
            match list {
                NumericList::Integer(integers) => numeric_bitwise_or(&integers),
                _ => None,
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Bitwise xor
///
/// For a list of integers,
/// returns the integer resulting from perfoming an "xor" on their bit representation.
///
/// Returns zero from the integer value space if no parameters are given.
/// Returns `None` if the input parameters are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitXor;
impl NaryFunction for BitXor {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list(parameters) {
            match list {
                NumericList::Integer(integers) => numeric_bitwise_xor(&integers),
                _ => None,
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Bitwise Left shift
///
/// Returns `None` if the input parameter pair are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitShiftLeft;
impl BinaryFunction for BitShiftLeft {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        match NumericPair::from_any_pair_cast(&parameter_first, &parameter_second)? {
            NumericPair::Integer(value, base) => numeric_bitwise_shl(value, base),
            _ => None,
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Bitwise arithmetic right shift
///
/// Returns `None` if the input parameter pair are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitShiftRight;
impl BinaryFunction for BitShiftRight {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        match NumericPair::from_any_pair_cast(&parameter_first, &parameter_second)? {
            NumericPair::Integer(value, base) => numeric_bitwise_shr(value, base),
            _ => None,
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Bitwise logical (unsigned) right shift
///
/// Returns `None` if the input parameter pair are not integers or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct BitShiftRightUnsigned;
impl BinaryFunction for BitShiftRightUnsigned {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        match NumericPair::from_any_pair_cast(&parameter_first, &parameter_second)? {
            NumericPair::Integer(value, base) => numeric_bitwise_shru(value, base),
            _ => None,
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Int64.bitset())
    }
}

/// Logarithm of numeric values w.r.t. an arbitraty numeric base
///
/// Returns the logarithm of the first parameter,
/// where the base is given by the second parameter.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericLogarithm;
impl BinaryFunction for NumericLogarithm {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_logarithm_integer64(first, second),
                NumericPair::Float(first, second) => numeric_logarithm_float(first, second),
                NumericPair::Double(first, second) => numeric_logarithm_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Raising a numeric value to some power
///
/// Returns the first parameter raised to the power of the second parameter.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericPower;
impl BinaryFunction for NumericPower {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_power_integer64(first, second),
                NumericPair::Float(first, second) => numeric_power_float(first, second),
                NumericPair::Double(first, second) => numeric_power_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Remainder operation
///
/// Returns the remainder of the (truncated) division `parameter_first / parameter.second`.
/// The value of the result has always the same sign as `paramter_second`.
///
/// Returns `None` if `parameter_second` is zero.
///
/// Returns `None` if the input parameters are not numeric
/// or if the result cannot be represented within the range of the result's value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericRemainder;
impl BinaryFunction for NumericRemainder {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_remainder_integer64(first, second),
                NumericPair::Float(first, second) => numeric_remainder_float(first, second),
                NumericPair::Double(first, second) => numeric_remainder_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Absolute value of numeric values
///
/// Returns the absolute value of the given parameter.
///
/// Returns `None` if the input parameter is not a numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericAbsolute;
impl UnaryFunction for NumericAbsolute {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => numeric_absolute_integer64(value),
                NumericValue::Float(value) => numeric_absolute_float(value),
                NumericValue::Double(value) => numeric_absolute_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Negation of a numeric value
///
/// Returns the multiplicative inverse of the input paramter.
///
/// Returns `None` if the input parameter is not a numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericNegation;
impl UnaryFunction for NumericNegation {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => numeric_negation_integer64(value),
                NumericValue::Float(value) => numeric_negation_float(value),
                NumericValue::Double(value) => numeric_negation_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Square root of a numeric value
///
/// Returns the square root of the given input paramter.
///
/// Returns `None` if the input parameter is not a numeric value space
/// or is negative.
#[derive(Debug, Copy, Clone)]
pub struct NumericSquareroot;
impl UnaryFunction for NumericSquareroot {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => numeric_squareroot_integer64(value),
                NumericValue::Float(value) => numeric_squareroot_float(value),
                NumericValue::Double(value) => numeric_squareroot_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Sine of a numeric value
///
/// Returns the sine of the input parameter.
///
/// Returns `None` if the input paramter is not in a floating point value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericSine;
impl UnaryFunction for NumericSine {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_sin_float(value),
                NumericValue::Double(value) => numeric_sin_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Cosine of a numeric value
///
/// Returns the cosine of the input parameter.
///
/// Returns `None` if the input paramter is not in a floating point value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericCosine;
impl UnaryFunction for NumericCosine {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_cos_float(value),
                NumericValue::Double(value) => numeric_cos_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Tangent of a numeric value
///
/// Returns the tangent of the input parameter.
///
/// Returns `None` if the input paramter is not in a floating point value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericTangent;
impl UnaryFunction for NumericTangent {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(_value) => None,
                NumericValue::Float(value) => numeric_tan_float(value),
                NumericValue::Double(value) => numeric_tan_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Rounding of a numeric value
///
/// Returns the nearest integer of the input parameter.
/// If the result is half-way between two integers, round away from 0.0.
///
/// Returns `None` if the input parameter is not from a numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericRound;
impl UnaryFunction for NumericRound {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => Some(AnyDataValue::new_integer_from_i64(value)),
                NumericValue::Float(value) => numeric_round_float(value),
                NumericValue::Double(value) => numeric_round_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Rounding up to the smallest integer less than or equal than input parameter
///
/// Returns the smallest integer less than or equal than input parameter
///
/// Returns `None` if the input parameter is not from a numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericCeil;
impl UnaryFunction for NumericCeil {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => Some(AnyDataValue::new_integer_from_i64(value)),
                NumericValue::Float(value) => numeric_ceil_float(value),
                NumericValue::Double(value) => numeric_ceil_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Rounding of a numeric value
///
/// Returns the nearest integer of the input parameter.
/// If the result is half-way between two integers, round away from 0.0.
///
/// Returns `None` if the input parameter is not from a numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericFloor;
impl UnaryFunction for NumericFloor {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if let Some(numeric_value) = NumericValue::from_any_datavalue(&parameter) {
            return match numeric_value {
                NumericValue::Integer(value) => Some(AnyDataValue::new_integer_from_i64(value)),
                NumericValue::Float(value) => numeric_floor_float(value),
                NumericValue::Double(value) => numeric_floor_double(value),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::Preserve
    }
}

/// Less than comparison of two numbers
///
/// Returns `true` from the boolean value space
/// if the first argument is smaller than the second argument,
/// and `false` otherwise.
///
/// Returns `None` if the arguments are not from the numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericLessthan;
impl BinaryFunction for NumericLessthan {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_lessthan_integer64(first, second),
                NumericPair::Float(first, second) => numeric_lessthan_float(first, second),
                NumericPair::Double(first, second) => numeric_lessthan_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Less than or equals comparison of two numbers
///
/// Returns `true` from the boolean value space
/// if the first argument is smaller than or equal to the second argument,
/// and `false` otherwise.
///
/// Returns `None` if the arguments are not from the numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericLessthaneq;
impl BinaryFunction for NumericLessthaneq {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_lessthaneq_integer64(first, second),
                NumericPair::Float(first, second) => numeric_lessthaneq_float(first, second),
                NumericPair::Double(first, second) => numeric_lessthaneq_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Greater than comparison of two numbers
///
/// Returns `true` from the boolean value space
/// if the first argument is greater than the second argument,
/// and `false` otherwise.
///
/// Returns `None` if the arguments are not from the numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericGreaterthan;
impl BinaryFunction for NumericGreaterthan {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
            return match pair {
                NumericPair::Integer(first, second) => numeric_greaterthan_integer64(first, second),
                NumericPair::Float(first, second) => numeric_greaterthan_float(first, second),
                NumericPair::Double(first, second) => numeric_greaterthan_double(first, second),
            };
        }

        None
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Greater than or equals comparison of two numbers
///
/// Returns `true` from the boolean value space
/// if the first argument is greater than or equal to the second argument,
/// and `false` otherwise.
///
/// Returns `None` if the arguments are not from the numeric value space.
#[derive(Debug, Copy, Clone)]
pub struct NumericGreaterthaneq;
impl BinaryFunction for NumericGreaterthaneq {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if let Some(pair) = NumericPair::from_any_pair_cast(&parameter_first, &parameter_second) {
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Numeric summation
///
/// Returns the sum of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or no input parameters are given.
/// Returns `None` if the result (or an intermediate result) cannot be represented
/// within the range of the numeric value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericSum;
impl NaryFunction for NumericSum {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list_cast(parameters) {
            match &list {
                NumericList::Integer(values) => numeric_sum_integer64(values),
                NumericList::Float(values) => numeric_sum_float(values),
                NumericList::Double(values) => numeric_sum_double(values),
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric product
///
/// Returns the product of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or no input parameters are given.
/// Returns `None` if the result (or an intermediate result) cannot be represented
/// within the range of the numeric value type.
#[derive(Debug, Copy, Clone)]
pub struct NumericProduct;
impl NaryFunction for NumericProduct {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list_cast(parameters) {
            match &list {
                NumericList::Integer(values) => numeric_product_integer64(values),
                NumericList::Float(values) => numeric_product_float(values),
                NumericList::Double(values) => numeric_product_double(values),
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric minimum
///
/// Returns the minimum of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct NumericMinimum;
impl NaryFunction for NumericMinimum {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list_cast(parameters) {
            match &list {
                NumericList::Integer(values) => numeric_minimum_integer64(values),
                NumericList::Float(values) => numeric_minimum_float(values),
                NumericList::Double(values) => numeric_minimum_double(values),
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Numeric maximum
///
/// Returns the maximum of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or no input parameters are given.
#[derive(Debug, Copy, Clone)]
pub struct NumericMaximum;
impl NaryFunction for NumericMaximum {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list_cast(parameters) {
            match &list {
                NumericList::Integer(values) => numeric_maximum_integer64(values),
                NumericList::Float(values) => numeric_maximum_float(values),
                NumericList::Double(values) => numeric_maximum_double(values),
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}

/// Lukasiewicz t-norm
///
/// Returns the Lukasiewicz t-norm of the given parameters.
///
/// Returns `None` if the input parameters are not numeric
/// or no input parameters are given.
/// Returns `None` if the input parameters are not of a floating point type.
#[derive(Debug, Copy, Clone)]
pub struct NumericLukasiewicz;
impl NaryFunction for NumericLukasiewicz {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(list) = NumericList::from_any_list_cast(parameters) {
            match &list {
                NumericList::Integer(_) => None,
                NumericList::Float(values) => numeric_tnorm_lukasiewicz_float(values),
                NumericList::Double(values) => numeric_tnorm_lukasiewicz_double(values),
            }
        } else {
            None
        }
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::NumericUpcast
    }
}
