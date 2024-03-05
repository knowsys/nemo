//! This module defines functions operating on [AnyDataValue].

pub(crate) mod boolean;
pub(crate) mod casting;
pub(crate) mod checktype;
pub(crate) mod generic;
pub(crate) mod language;
pub(crate) mod numeric;
pub(crate) mod string;

use delegate::delegate;

use crate::datavalues::AnyDataValue;

use self::{
    boolean::{BooleanConjunction, BooleanDisjunction, BooleanNegation},
    casting::{CastingIntoDouble, CastingIntoFloat, CastingIntoInteger64},
    checktype::{
        CheckIsDouble, CheckIsFloat, CheckIsInteger, CheckIsIri, CheckIsNull, CheckIsNumeric,
        CheckIsString,
    },
    generic::{CanonicalString, Datatype, Equals, LexicalValue, Unequals},
    language::LanguageTag,
    numeric::{
        NumericAbsolute, NumericAddition, NumericCeil, NumericCosine, NumericDivision,
        NumericFloor, NumericGreaterthan, NumericGreaterthaneq, NumericLessthan, NumericLessthaneq,
        NumericLogarithm, NumericMultiplication, NumericNegation, NumericPower, NumericRemainder,
        NumericRound, NumericSine, NumericSquareroot, NumericSubtraction, NumericTangent,
    },
    string::{
        StringAfter, StringBefore, StringCompare, StringConcatenation, StringContains, StringEnds,
        StringLength, StringLowercase, StringStarts, StringSubstring, StringUppercase,
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
    CanonicalString(CanonicalString),
    CastingIntoInteger64(CastingIntoInteger64),
    CastingIntoDouble(CastingIntoDouble),
    CastingIntoFloat(CastingIntoFloat),
    CheckIsDouble(CheckIsDouble),
    CheckIsFloat(CheckIsFloat),
    CheckIsInteger(CheckIsInteger),
    CheckIsIri(CheckIsIri),
    CheckIsNull(CheckIsNull),
    CheckIsNumeric(CheckIsNumeric),
    CheckIsString(CheckIsString),
    Datatype(Datatype),
    LanguageTag(LanguageTag),
    LexicalValue(LexicalValue),
    NumericAbsolute(NumericAbsolute),
    NumericCeil(NumericCeil),
    NumericCosine(NumericCosine),
    NumericFloor(NumericFloor),
    NumericNegation(NumericNegation),
    NumericRound(NumericRound),
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
            Self::CanonicalString(function) => function,
            Self::CastingIntoInteger64(function) => function,
            Self::CastingIntoFloat(function) => function,
            Self::CastingIntoDouble(function) => function,
            Self::CheckIsDouble(function) => function,
            Self::CheckIsFloat(function) => function,
            Self::CheckIsInteger(function) => function,
            Self::CheckIsIri(function) => function,
            Self::CheckIsNull(function) => function,
            Self::CheckIsNumeric(function) => function,
            Self::CheckIsString(function) => function,
            Self::Datatype(function) => function,
            Self::LanguageTag(function) => function,
            Self::LexicalValue(function) => function,
            Self::NumericAbsolute(function) => function,
            Self::NumericCeil(function) => function,
            Self::NumericCosine(function) => function,
            Self::NumericFloor(function) => function,
            Self::NumericNegation(function) => function,
            Self::NumericRound(function) => function,
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
pub trait BinaryFunction {
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
    Equals(Equals),
    Unequals(Unequals),
    NumericAddition(NumericAddition),
    NumericSubtraction(NumericSubtraction),
    NumericMultiplication(NumericMultiplication),
    NumericDivision(NumericDivision),
    NumericLogarithm(NumericLogarithm),
    NumericPower(NumericPower),
    NumericRemainder(NumericRemainder),
    NumericLessthan(NumericLessthan),
    NumericLessthaneq(NumericLessthaneq),
    NumericGreaterthan(NumericGreaterthan),
    NumericGreaterthaneq(NumericGreaterthaneq),
    StringAfter(StringAfter),
    StringBefore(StringBefore),
    StringCompare(StringCompare),
    StringConcatenation(StringConcatenation),
    StringContains(StringContains),
    StringEnds(StringEnds),
    StringStarts(StringStarts),
    StringSubstring(StringSubstring),
    BooleanConjunction(BooleanConjunction),
    BooleanDisjunction(BooleanDisjunction),
}

impl BinaryFunction for BinaryFunctionEnum {
    delegate! {
        to match self {
            Self::Equals(function) => function,
            Self::Unequals(function) => function,
            Self::NumericAddition(function) => function,
            Self::NumericSubtraction(function) => function,
            Self::NumericMultiplication(function) => function,
            Self::NumericDivision(function) => function,
            Self::NumericLogarithm(function) => function,
            Self::NumericPower(function) => function,
            Self::NumericRemainder(function) => function,
            Self::NumericLessthan(function) => function,
            Self::NumericLessthaneq(function) => function,
            Self::NumericGreaterthan(function) => function,
            Self::NumericGreaterthaneq(function) => function,
            Self::StringAfter(function) => function,
            Self::StringBefore(function) => function,
            Self::StringCompare(function) => function,
            Self::StringConcatenation(function) => function,
            Self::StringContains(function) => function,
            Self::StringEnds(function) => function,
            Self::StringStarts(function) => function,
            Self::StringSubstring(function) => function,
            Self::BooleanConjunction(function) => function,
            Self::BooleanDisjunction(function) => function,
        } {
            fn evaluate(&self, first_parameter: AnyDataValue, second_parameter: AnyDataValue) -> Option<AnyDataValue>;
        }
    }
}
