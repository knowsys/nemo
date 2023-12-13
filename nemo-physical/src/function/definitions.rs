//! This module defines functions operating on [AnyDataValue].

pub mod boolean;
pub mod casting;
pub mod numeric;
pub mod string;

use delegate::delegate;

use crate::datavalues::AnyDataValue;

use self::{
    boolean::{BooleanConjunction, BooleanDisjunction, BooleanNegation},
    casting::{CastingIntoDouble, CastingIntoFloat, CastingIntoInteger64},
    numeric::{
        NumericAbsolute, NumericAddition, NumericCosine, NumericDivision, NumericGreaterthan,
        NumericGreaterthaneq, NumericLessthan, NumericLessthaneq, NumericLogarithm,
        NumericMultiplication, NumericNegation, NumericPower, NumericSine, NumericSquareroot,
        NumericSubtraction, NumericTangent,
    },
    string::{
        StringCompare, StringConcatenation, StringContains, StringLength, StringLowercase,
        StringSubstring, StringUppercase,
    },
};

/// Defines a unary function on [AnyDataValue].
pub(crate) trait UnaryFunction {
    /// Evaluate this function on the given parameter.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue>;
}

/// Enum containing all implementations of [UnaryFunction]
#[derive(Debug, Clone, Copy)]
pub enum UnaryFunctionEnum {
    BooleanNegation(BooleanNegation),
    CastingIntoInteger64(CastingIntoInteger64),
    CastingIntoDouble(CastingIntoDouble),
    CastingIntoFloat(CastingIntoFloat),
    NumericAbsolute(NumericAbsolute),
    NumericCosine(NumericCosine),
    NumericNegation(NumericNegation),
    NumericSine(NumericSine),
    NumericSquareroot(NumericSquareroot),
    NumericTangent(NumericTangent),
    StringLength(StringLength),
    StringLowercase(StringLowercase),
    StringUppercase(StringUppercase),
}

impl UnaryFunction for UnaryFunctionEnum {
    delegate! {
        to match self {
            Self::BooleanNegation(function) => function,
            Self::CastingIntoInteger64(function) => function,
            Self::CastingIntoFloat(function) => function,
            Self::CastingIntoDouble(function) => function,
            Self::NumericAbsolute(function) => function,
            Self::NumericCosine(function) => function,
            Self::NumericNegation(function) => function,
            Self::NumericSine(function) => function,
            Self::NumericSquareroot(function) => function,
            Self::NumericTangent(function) => function,
            Self::StringLength(function) => function,
            Self::StringLowercase(function) => function,
            Self::StringUppercase(function) => function,
        } {
            fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue>;
        }
    }
}

/// Defines a binary function on [AnyDataValue]
pub(crate) trait BinaryFunction {
    /// Evaluate this function on the given parameters.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue>;
}

/// Enum containing all implementations of [BinaryFunction]
#[derive(Debug, Clone, Copy)]
pub enum BinaryFunctionEnum {
    NumericAddition(NumericAddition),
    NumericSubtraction(NumericSubtraction),
    NumericMultiplication(NumericMultiplication),
    NumericDivision(NumericDivision),
    NumericLogarithm(NumericLogarithm),
    NumericPower(NumericPower),
    NumericLessthan(NumericLessthan),
    NumericLessthaneq(NumericLessthaneq),
    NumericGreaterthan(NumericGreaterthan),
    NumericGreaterthaneq(NumericGreaterthaneq),
    StringCompare(StringCompare),
    StringConcatenation(StringConcatenation),
    StringContains(StringContains),
    StringSubstring(StringSubstring),
    BooleanConjunction(BooleanConjunction),
    BooleanDisjunction(BooleanDisjunction),
}

impl BinaryFunction for BinaryFunctionEnum {
    delegate! {
        to match self {
            Self::NumericAddition(function) => function,
            Self::NumericSubtraction(function) => function,
            Self::NumericMultiplication(function) => function,
            Self::NumericDivision(function) => function,
            Self::NumericLogarithm(function) => function,
            Self::NumericPower(function) => function,
            Self::NumericLessthan(function) => function,
            Self::NumericLessthaneq(function) => function,
            Self::NumericGreaterthan(function) => function,
            Self::NumericGreaterthaneq(function) => function,
            Self::StringCompare(function) => function,
            Self::StringConcatenation(function) => function,
            Self::StringContains(function) => function,
            Self::StringSubstring(function) => function,
            Self::BooleanConjunction(function) => function,
            Self::BooleanDisjunction(function) => function,
        } {
            fn evaluate(&self, first_parameter: AnyDataValue, second_parameter: AnyDataValue) -> Option<AnyDataValue>;
        }
    }
}
