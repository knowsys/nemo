//! This module defines functions operating on [AnyDataValue].

pub(crate) mod boolean;
pub(crate) mod casting;
pub(crate) mod checktype;
pub(crate) mod generic;
pub(crate) mod language;
pub(crate) mod numeric;
pub(crate) mod string;

use delegate::delegate;

use crate::{datatypes::storage_type_name::StorageTypeBitSet, datavalues::AnyDataValue};

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
        BitAnd, BitOr, BitXor, NumericAbsolute, NumericAddition, NumericCeil, NumericCosine,
        NumericDivision, NumericFloor, NumericGreaterthan, NumericGreaterthaneq, NumericLessthan,
        NumericLessthaneq, NumericLogarithm, NumericLukasiewicz, NumericMaximum, NumericMinimum,
        NumericMultiplication, NumericNegation, NumericPower, NumericProduct, NumericRemainder,
        NumericRound, NumericSine, NumericSquareroot, NumericSubtraction, NumericSum,
        NumericTangent,
    },
    string::{
        StringAfter, StringBefore, StringCompare, StringConcatenation, StringContains, StringEnds,
        StringLength, StringLowercase, StringStarts, StringSubstring, StringSubstringLength,
        StringUppercase,
    },
};

/// Specifies how storage values are propagated by a function.
pub(crate) enum FunctionTypePropagation {
    /// Possible outputs are knonw in advance
    KnownOutput(StorageTypeBitSet),
    /// Types are preserved, i.e. the output has the same types as the inputs
    /// (the function returns `None` if input values differ in type)
    Preserve,
    /// Nothing is known about the the type propagation
    _Unknown,
}

/// Defines a unary function on [AnyDataValue].
pub(crate) trait UnaryFunction {
    /// Evaluate this function on the given parameter.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue>;

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
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
            fn type_propagation(&self) -> FunctionTypePropagation;
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

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
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
    StringContains(StringContains),
    StringEnds(StringEnds),
    StringStarts(StringStarts),
    StringSubstring(StringSubstring),
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
            Self::StringContains(function) => function,
            Self::StringEnds(function) => function,
            Self::StringStarts(function) => function,
            Self::StringSubstring(function) => function,
        } {
            fn evaluate(&self, first_parameter: AnyDataValue, second_parameter: AnyDataValue) -> Option<AnyDataValue>;
            fn type_propagation(&self) -> FunctionTypePropagation;
        }
    }
}

/// Defines a ternary function on [AnyDataValue]
pub trait TernaryFunction {
    /// Evaluate this function on the given parameters.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
        parameter_third: AnyDataValue,
    ) -> Option<AnyDataValue>;

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
}

/// Enum containing all implementations of [TernaryFunction]
#[derive(Debug, Clone, Copy)]
pub enum TernaryFunctionEnum {
    StringSubstringLength(StringSubstringLength),
}

impl TernaryFunction for TernaryFunctionEnum {
    delegate! {
        to match self {
            Self::StringSubstringLength(function) => function,
        } {
            fn evaluate(&self, first_parameter: AnyDataValue, second_parameter: AnyDataValue, third_paramter: AnyDataValue) -> Option<AnyDataValue>;
            fn type_propagation(&self) -> FunctionTypePropagation;
        }
    }
}

/// Defines a n-ary function on [AnyDataValue]
pub trait NaryFunction {
    /// Evaluate this function on the given parameters.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue>;

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
}

/// Enum containing all implementations of [NaryFunction]
#[derive(Debug, Clone, Copy)]
pub enum NaryFunctionEnum {
    BitAnd(BitAnd),
    BitOr(BitOr),
    BitXor(BitXor),
    BooleanConjunction(BooleanConjunction),
    BooleanDisjunction(BooleanDisjunction),
    NumericMaximum(NumericMaximum),
    NumericMinimum(NumericMinimum),
    NumericLukasiewicz(NumericLukasiewicz),
    NumericSum(NumericSum),
    NumericProduct(NumericProduct),
    StringConcatenation(StringConcatenation),
}

impl NaryFunction for NaryFunctionEnum {
    delegate! {
        to match self {
            Self::BitAnd(function) => function,
            Self::BitOr(function) => function,
            Self::BitXor(function) => function,
            Self::BooleanConjunction(function) => function,
            Self::BooleanDisjunction(function) => function,
            Self::NumericMaximum(function) => function,
            Self::NumericMinimum(function) => function,
            Self::NumericLukasiewicz(function) => function,
            Self::NumericSum(function) => function,
            Self::NumericProduct(function) => function,
            Self::StringConcatenation(function) => function,
        } {
            fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue>;
            fn type_propagation(&self) -> FunctionTypePropagation;
        }
    }
}
