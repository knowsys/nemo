//! This module defines functions operating on [AnyDataValue].

pub(crate) mod boolean;
pub(crate) mod casting;
pub(crate) mod checktype;
pub(crate) mod datetime;
pub(crate) mod generic;
pub(crate) mod hashing;
pub(crate) mod language;
pub(crate) mod nondeterministic;
pub(crate) mod numeric;
pub(crate) mod string;

use casting::CastingIntoIri;
use nondeterministic::{FuncRand, FuncStruuid, FuncUuid};

use datetime::{
    DateTimeDay, DateTimeHours, DateTimeMinutes, DateTimeMonth, DateTimeSeconds, DateTimeTimezone,
    DateTimeTz, DateTimeYear,
};
use delegate::delegate;
use hashing::{StringMd5, StringSha1, StringSha256, StringSha384, StringSha512};
use string::StringLevenshtein;

use crate::{
    datatypes::{StorageTypeName, storage_type_name::StorageTypeBitSet},
    datavalues::AnyDataValue,
    function::definitions::language::LanguageString,
};

use self::{
    boolean::{BooleanConjunction, BooleanDisjunction, BooleanNegation},
    casting::{CastingIntoDouble, CastingIntoFloat, CastingIntoInteger64},
    checktype::{
        CheckIsDouble, CheckIsFloat, CheckIsInteger, CheckIsIri, CheckIsNull, CheckIsNumeric,
        CheckIsString,
    },
    generic::{CanonicalString, Datatype, Equals, LexicalValue, TypedLiteral, Unequals},
    language::LanguageTag,
    numeric::{
        BitAnd, BitOr, BitShiftLeft, BitShiftRight, BitShiftRightUnsigned, BitXor, NumericAbsolute,
        NumericAddition, NumericCeil, NumericCosine, NumericDivision, NumericFloor,
        NumericGreaterthan, NumericGreaterthaneq, NumericLessthan, NumericLessthaneq,
        NumericLogarithm, NumericLukasiewicz, NumericMaximum, NumericMinimum,
        NumericMultiplication, NumericNegation, NumericPower, NumericProduct, NumericRemainder,
        NumericRound, NumericSine, NumericSquareroot, NumericSubtraction, NumericSum,
        NumericTangent,
    },
    string::{
        StringAfter, StringBefore, StringCompare, StringConcatenation, StringContains, StringEnds,
        StringLangMatches, StringLength, StringLowercase, StringRegex, StringReplace,
        StringReverse, StringStarts, StringSubstring, StringSubstringLength, StringTrim,
        StringTrimEnd, StringTrimStart, StringUppercase, StringUriDecode, StringUriEncode,
    },
};

/// Specifies how storage values are propagated by a function.
#[derive(Debug, Clone, Copy)]
pub enum FunctionTypePropagation {
    /// Possible outputs are knonw in advance
    KnownOutput(StorageTypeBitSet),
    /// Types are preserved, i.e. the output has the same types as the inputs
    /// (the function returns `None` if input values differ in type)
    Preserve,
    /// If input types are numeric, cast them to the maximum type
    NumericUpcast,
    /// Nothing is known about the the type propagation
    _Unknown,
}

impl FunctionTypePropagation {
    pub(crate) fn propagate(&self, input: &[StorageTypeBitSet]) -> StorageTypeBitSet {
        match self {
            FunctionTypePropagation::KnownOutput(storage_type) => *storage_type,
            FunctionTypePropagation::Preserve => {
                let mut result_type = StorageTypeBitSet::full();
                for input_type in input {
                    result_type = result_type.intersection(*input_type);
                }

                result_type
            }
            FunctionTypePropagation::_Unknown => StorageTypeBitSet::full(),
            FunctionTypePropagation::NumericUpcast => {
                if input.is_empty() {
                    return StorageTypeBitSet::empty();
                }

                let mut contains_int = true;
                let mut contains_float = true;
                let mut contains_double = true;

                let mut mixing = false;
                let first_type = &input[0];

                for input_type in input {
                    if !input_type.contains(&StorageTypeName::Int64) {
                        contains_int = false;
                    }

                    if !input_type.contains(&StorageTypeName::Float) {
                        contains_float = false;
                    }

                    if !input_type.contains(&StorageTypeName::Double) {
                        contains_double = false;
                    }

                    if input_type != first_type || (!input_type.is_unique() && input.len() > 1) {
                        mixing = true;
                    }
                }

                let mut result_type = StorageTypeBitSet::empty();
                if contains_int {
                    result_type = result_type.union(StorageTypeName::Int64.bitset());
                }
                if contains_float {
                    result_type = result_type.union(StorageTypeName::Float.bitset());
                }
                if mixing || contains_double {
                    result_type = result_type.union(StorageTypeName::Double.bitset());
                }

                result_type
            }
        }
    }
}

/// Defines a unary function on [AnyDataValue].
pub trait UnaryFunction {
    /// Evaluate this function on the given parameter.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue>;

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
}

/// Enum containing all implementations of [UnaryFunction]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryFunctionEnum {
    /// Negation operator for Booleans
    BooleanNegation(BooleanNegation),
    /// Canonical string for value
    CanonicalString(CanonicalString),
    /// Cast to i64
    CastingIntoInteger64(CastingIntoInteger64),
    /// Cast to double
    CastingIntoDouble(CastingIntoDouble),
    /// Cast to float
    CastingIntoFloat(CastingIntoFloat),
    /// Cast to Iri
    CastingIntoIri(CastingIntoIri),
    /// Check whether value is double
    CheckIsDouble(CheckIsDouble),
    /// Check whether value is float
    CheckIsFloat(CheckIsFloat),
    /// Check whether value is integer
    CheckIsInteger(CheckIsInteger),
    /// Check whether value is Iri
    CheckIsIri(CheckIsIri),
    /// Check whether value is null
    CheckIsNull(CheckIsNull),
    /// Check whether value is numeric
    CheckIsNumeric(CheckIsNumeric),
    /// Check whether value is a string
    CheckIsString(CheckIsString),
    /// Datatype Iri for the value
    Datatype(Datatype),
    /// Language tag of language tagged string
    LanguageTag(LanguageTag),
    /// Lexical value for the value
    LexicalValue(LexicalValue),
    /// Absolute value
    NumericAbsolute(NumericAbsolute),
    /// Round up to nearest integer
    NumericCeil(NumericCeil),
    /// Cosine of value
    NumericCosine(NumericCosine),
    /// Round down to nearest integer
    NumericFloor(NumericFloor),
    /// Negation operator for numbers
    NumericNegation(NumericNegation),
    /// Round to nearest integer
    NumericRound(NumericRound),
    /// Sine of value
    NumericSine(NumericSine),
    /// Square root of value
    NumericSquareroot(NumericSquareroot),
    /// Tangent of value
    NumericTangent(NumericTangent),
    /// Length of a string
    StringLength(StringLength),
    /// Trim whitespace from both ends of a string
    StringTrim(StringTrim),
    /// Trim whitespace from the start of a string
    StringTrimStart(StringTrimStart),
    /// Trim whitespace from the end of a string
    StringTrimEnd(StringTrimEnd),
    /// Reverse of a string
    StringReverse(StringReverse),
    /// Lowercase of a string
    StringLowercase(StringLowercase),
    /// Uppercase of a string
    StringUppercase(StringUppercase),
    /// Iri-encoding of a string
    StringUriEncode(StringUriEncode),
    /// Iri-decoding of a string
    StringUriDecode(StringUriDecode),
    /// MD5 hash of a string
    StringMd5(StringMd5),
    /// SHA1 hash of a string
    StringSha1(StringSha1),
    /// SHA256 hash of a string
    StringSha256(StringSha256),
    /// SHA384 hash of a string
    StringSha384(StringSha384),
    /// SHA512 hash of a string
    StringSha512(StringSha512),
    /// Year component of a date/dateTime value
    DateTimeYear(DateTimeYear),
    /// Month component of a date/dateTime value
    DateTimeMonth(DateTimeMonth),
    /// Day component of a date/dateTime value
    DateTimeDay(DateTimeDay),
    /// Hours component of a dateTime/time value
    DateTimeHours(DateTimeHours),
    /// Minutes component of a dateTime/time value
    DateTimeMinutes(DateTimeMinutes),
    /// Seconds component of a dateTime/time value
    DateTimeSeconds(DateTimeSeconds),
    /// Timezone of a date/dateTime/time value as xsd:dayTimeDuration
    DateTimeTimezone(DateTimeTimezone),
    /// Timezone of a date/dateTime/time value as a plain string
    DateTimeTz(DateTimeTz),
}

impl UnaryFunction for UnaryFunctionEnum {
    delegate! {
        to match self {
            Self::BooleanNegation(function) => function,
            Self::CanonicalString(function) => function,
            Self::CastingIntoInteger64(function) => function,
            Self::CastingIntoFloat(function) => function,
            Self::CastingIntoDouble(function) => function,
            Self::CastingIntoIri(function) => function,
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
            Self::StringTrim(function) => function,
            Self::StringTrimStart(function) => function,
            Self::StringTrimEnd(function) => function,
            Self::StringReverse(function) => function,
            Self::StringLowercase(function) => function,
            Self::StringUppercase(function) => function,
            Self::StringUriEncode(function) => function,
            Self::StringUriDecode(function) => function,
            Self::StringMd5(function) => function,
            Self::StringSha1(function) => function,
            Self::StringSha256(function) => function,
            Self::StringSha384(function) => function,
            Self::StringSha512(function) => function,
            Self::DateTimeYear(function) => function,
            Self::DateTimeMonth(function) => function,
            Self::DateTimeDay(function) => function,
            Self::DateTimeHours(function) => function,
            Self::DateTimeMinutes(function) => function,
            Self::DateTimeSeconds(function) => function,
            Self::DateTimeTimezone(function) => function,
            Self::DateTimeTz(function) => function,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryFunctionEnum {
    /// Equality comparison
    Equals(Equals),
    /// Inequality comparison
    Unequals(Unequals),
    /// Construct a language-tagged string from value and language tag
    LanguageString(LanguageString),
    /// Addition of two numbers
    NumericAddition(NumericAddition),
    /// Subtraction of two numbers
    NumericSubtraction(NumericSubtraction),
    /// Multiplication of two numbers
    NumericMultiplication(NumericMultiplication),
    /// Division of two numbers
    NumericDivision(NumericDivision),
    /// Logarithm of number relative to base
    NumericLogarithm(NumericLogarithm),
    /// Exponentiation of number relative to base
    NumericPower(NumericPower),
    /// Remainder of division of two numbers
    NumericRemainder(NumericRemainder),
    /// Less-than comparison of two numbers
    NumericLessthan(NumericLessthan),
    /// Less-than-or-equal comparison of two numbers
    NumericLessthaneq(NumericLessthaneq),
    /// Greater-than comparison of two numbers
    NumericGreaterthan(NumericGreaterthan),
    /// Greater-than-or-equal comparison of two numbers
    NumericGreaterthaneq(NumericGreaterthaneq),
    /// Part of string after first occurrence of second string
    StringAfter(StringAfter),
    /// Part of string before first occurrence of second string
    StringBefore(StringBefore),
    /// Lexicographic comparison of two strings
    StringCompare(StringCompare),
    /// Containment of strings
    StringContains(StringContains),
    /// Levenshtein distance of two strings
    StringLevenshtein(StringLevenshtein),
    /// Is the second string a suffix of the first string?
    StringEnds(StringEnds),
    /// Is the second string a prefix of the first string?
    StringStarts(StringStarts),
    /// Is the second string an infix of the first string?
    StringSubstring(StringSubstring),
    /// Shift value left by given number of bits
    BitShiftLeft(BitShiftLeft),
    /// Shift value right by given number of bits
    BitShiftRight(BitShiftRight),
    /// Shift value right by given number of bits, filling with zeros
    BitShiftRightUnsigned(BitShiftRightUnsigned),
    /// Check if a language tag matches a language range
    StringLangMatches(StringLangMatches),
    /// Construct a typed literal from a lexical value and a datatype IRI
    TypedLiteral(TypedLiteral),
}

impl BinaryFunction for BinaryFunctionEnum {
    delegate! {
        to match self {
            Self::Equals(function) => function,
            Self::Unequals(function) => function,
            Self::LanguageString(function) => function,
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
            Self::StringLevenshtein(function) => function,
            Self::StringEnds(function) => function,
            Self::StringStarts(function) => function,
            Self::StringSubstring(function) => function,
            Self::BitShiftLeft(function) => function,
            Self::BitShiftRightUnsigned(function) => function,
            Self::BitShiftRight(function) => function,
            Self::StringLangMatches(function) => function,
            Self::TypedLiteral(function) => function,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TernaryFunctionEnum {
    /// Substring of given string starting at offset of given length
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

/// Defines a nullary function on [AnyDataValue] (takes no arguments)
pub trait NullaryFunction {
    /// Evaluate this function.
    ///
    /// Returns `None` if the result of the operation is undefined.
    fn evaluate(&self) -> Option<AnyDataValue>;

    /// Return a [FunctionTypePropagation] indicating how storage types are propagated
    /// when applying this function.
    fn type_propagation(&self) -> FunctionTypePropagation;
}

/// Enum containing all implementations of [NullaryFunction]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullaryFunctionEnum {
    /// Pseudo-random double in [0, 1)
    FuncRand(FuncRand),
    /// Fresh UUID as an IRI
    FuncUuid(FuncUuid),
    /// Fresh UUID as a plain string
    FuncStruuid(FuncStruuid),
}

impl NullaryFunction for NullaryFunctionEnum {
    delegate! {
        to match self {
            Self::FuncRand(function) => function,
            Self::FuncUuid(function) => function,
            Self::FuncStruuid(function) => function,
        } {
            fn evaluate(&self) -> Option<AnyDataValue>;
            fn type_propagation(&self) -> FunctionTypePropagation;
        }
    }
}

impl NullaryFunctionEnum {
    /// Return `true` if this function is nondeterministic (must not be constant-folded).
    pub(crate) fn is_nondeterministic(&self) -> bool {
        true
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NaryFunctionEnum {
    /// Bitwise and
    BitAnd(BitAnd),
    /// Bitwise or
    BitOr(BitOr),
    /// Bitwise exclusive or
    BitXor(BitXor),
    /// Boolean and
    BooleanConjunction(BooleanConjunction),
    /// Boolean or
    BooleanDisjunction(BooleanDisjunction),
    /// Maximum of given numbers
    NumericMaximum(NumericMaximum),
    /// Minimum of given numbers
    NumericMinimum(NumericMinimum),
    /// Lukasiewicz T-norm of given numbers
    NumericLukasiewicz(NumericLukasiewicz),
    /// Sum of given numbers
    NumericSum(NumericSum),
    /// Product of given numbers
    NumericProduct(NumericProduct),
    /// Concatenation of given strings
    StringConcatenation(StringConcatenation),
    /// Regex matching of strings (with optional flags)
    StringRegex(StringRegex),
    /// Regex-based replacement within a string
    StringReplace(StringReplace),
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
            Self::StringRegex(function) => function,
            Self::StringReplace(function) => function,
        } {
            fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue>;
            fn type_propagation(&self) -> FunctionTypePropagation;
        }
    }
}
