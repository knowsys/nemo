//! This module defines a tree representation [FunctionTree].

use std::{collections::HashMap, fmt::Debug, hash::Hash};

use crate::datavalues::AnyDataValue;

use super::{
    definitions::{
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
            NumericDivision, NumericFloor, NumericGreaterthan, NumericGreaterthaneq,
            NumericLessthan, NumericLessthaneq, NumericLogarithm, NumericLukasiewicz,
            NumericMaximum, NumericMinimum, NumericMultiplication, NumericNegation, NumericPower,
            NumericProduct, NumericRemainder, NumericRound, NumericSine, NumericSquareroot,
            NumericSubtraction, NumericSum, NumericTangent,
        },
        string::{
            StringAfter, StringBefore, StringCompare, StringConcatenation, StringContains,
            StringEnds, StringLength, StringLowercase, StringRegex, StringReverse, StringStarts,
            StringSubstring, StringSubstringLength, StringUppercase,
        },
        BinaryFunctionEnum, NaryFunctionEnum, TernaryFunctionEnum, UnaryFunctionEnum,
    },
    evaluation::StackProgram,
};

/// Leaf node of a [FunctionTree]
#[derive(Debug, Clone)]
pub enum FunctionLeaf<ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Constant value
    Constant(AnyDataValue),
    /// Referenced value supplied when evaluating the [FunctionTree]
    Reference(ReferenceType),
}

/// Tree structure representing a series of function applications
#[derive(Debug, Clone)]
pub enum FunctionTree<ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Leaf node
    Leaf(FunctionLeaf<ReferenceType>),
    /// Application of a unary function
    Unary(UnaryFunctionEnum, Box<FunctionTree<ReferenceType>>),
    /// Application of a binary function
    Binary {
        /// Binary operation
        function: BinaryFunctionEnum,
        /// First parameter to the function
        left: Box<FunctionTree<ReferenceType>>,
        /// Second parameter to the function
        right: Box<FunctionTree<ReferenceType>>,
    },
    /// Application of a ternary function
    Ternary {
        /// Ternary operation
        function: TernaryFunctionEnum,
        /// First parameter to the function
        first: Box<FunctionTree<ReferenceType>>,
        /// Second parameter to the function
        second: Box<FunctionTree<ReferenceType>>,
        /// Third parameter to the function
        third: Box<FunctionTree<ReferenceType>>,
    },
    /// Application of an n-ary function
    Nary {
        /// N-ary function
        function: NaryFunctionEnum,
        /// Parameters of the function
        parameters: Vec<FunctionTree<ReferenceType>>,
    },
}

/// Enumeration of special cases for [FunctionTree]s
/// where it might be beneficial to have special handling
/// for performance reasons within the evaluation of
/// [TrieScanFunction][crate::tabular::operations::function::TrieScanFunction]
///
/// TODO: This is not yet used anywhere
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum SpecialCaseFunction<'a, ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Regular function with no special handling
    Normal,
    /// Function evaluates to a constant
    Constant(Option<AnyDataValue>),
    /// Function evaluates to a referenced value
    Reference(&'a ReferenceType),
}

/// Enumeration of special cases for [FunctionTree]s
/// where it might be beneficial to have special handling
/// for performance reasons within the evaluation of
/// [TrieScanFilter][crate::tabular::operations::filter::TrieScanFilter]
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum SpecialCaseFilter<'a, ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Regular function with no special handling
    Normal,
    /// Function checks whether a column is equal to a constant
    Constant(&'a ReferenceType, &'a AnyDataValue),
}

impl<ReferenceType> FunctionTree<ReferenceType>
where
    ReferenceType: Debug + Clone + Hash + Eq,
{
    /// Check if this function correspond to some special case defined in [SpecialCaseFunction].
    /// Returns `None` if this is not the case.
    pub(crate) fn special_function(&self) -> SpecialCaseFunction<'_, ReferenceType> {
        if self.references().is_empty() {
            let constant_program =
                StackProgram::from_function_tree(self, &HashMap::default(), None);

            return SpecialCaseFunction::Constant(constant_program.evaluate_data(&[]));
        }

        match self {
            FunctionTree::Leaf(FunctionLeaf::Reference(reference)) => {
                SpecialCaseFunction::Reference(reference)
            }
            _ => SpecialCaseFunction::Normal,
        }
    }

    /// Check if this function correspond to some special case defined in [SpecialCaseFilter].
    /// Returns `None` if this is not the case.
    pub(crate) fn special_filter(&self) -> SpecialCaseFilter<'_, ReferenceType> {
        match self {
            FunctionTree::Binary {
                function: BinaryFunctionEnum::Equals(_),
                left,
                right,
            } => {
                if let (Some(reference), Some(constant)) =
                    (left.get_reference(), right.get_constant_value())
                {
                    return SpecialCaseFilter::Constant(reference, constant);
                }

                if let (Some(reference), Some(constant)) =
                    (right.get_reference(), left.get_constant_value())
                {
                    return SpecialCaseFilter::Constant(reference, constant);
                }

                SpecialCaseFilter::Normal
            }
            _ => SpecialCaseFilter::Normal,
        }
    }

    /// Check if the function represented by this object evaluates to a constant.
    /// Returns `Some(constant)` if this is the case and `None` otherwise.
    fn get_constant_value(&self) -> Option<&AnyDataValue> {
        if let FunctionTree::Leaf(FunctionLeaf::Constant(constant)) = self {
            Some(constant)
        } else {
            None
        }
    }

    /// Check if the function represented by this object evaluates to a referenced value.
    /// Returns `Some(reference)` if this is the case and `None` otherwise.
    fn get_reference(&self) -> Option<&ReferenceType> {
        if let FunctionTree::Leaf(FunctionLeaf::Reference(reference)) = self {
            Some(reference)
        } else {
            None
        }
    }
}

// Contains constructors for each type of operation
impl<ReferenceType> FunctionTree<ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Create a leaf node with a constant.
    pub fn constant(constant: AnyDataValue) -> Self {
        Self::Leaf(FunctionLeaf::Constant(constant))
    }

    /// Create a leaf node with a reference.
    pub fn reference(reference: ReferenceType) -> Self {
        Self::Leaf(FunctionLeaf::Reference(reference))
    }

    /// Create a tree node for checking whether two values are equal to each other.
    ///
    /// This evaluates to `true` if `left` and `right`
    /// evaluate to the same value,
    /// and to `false` otherwise.
    pub fn equals(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::Equals(Equals),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node for checking whether two values are not equal to each other.
    ///
    /// This evaluates to `true` if `left` and `right`
    /// evaluate to different values,
    /// and to `false` otherwise.
    pub fn unequals(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::Unequals(Unequals),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node that evaluates to the canonical string representation of the sub node.
    pub fn canonical_string(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CanonicalString(CanonicalString),
            Box::new(sub),
        )
    }

    /// Create a tree node that evaluates to the lexical value of the sub node.
    pub fn lexical_value(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::LexicalValue(LexicalValue), Box::new(sub))
    }

    /// Create a tree node that evaluates to the data type string of the sub node.
    pub fn datatype(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::Datatype(Datatype), Box::new(sub))
    }

    /// Create a tree node that evaluates to the language tag of the sub node.
    pub fn languagetag(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::LanguageTag(LanguageTag), Box::new(sub))
    }

    /// Create a tree node the checks whether the sub node is an integer.
    pub fn check_is_integer(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CheckIsInteger(CheckIsInteger),
            Box::new(sub),
        )
    }

    /// Create a tree node the checks whether the sub node is a float.
    pub fn check_is_float(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::CheckIsFloat(CheckIsFloat), Box::new(sub))
    }

    /// Create a tree node the checks whether the sub node is a double.
    pub fn check_is_double(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CheckIsDouble(CheckIsDouble),
            Box::new(sub),
        )
    }

    /// Create a tree node the checks whether the sub node is numeric.
    pub fn check_is_numeric(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CheckIsNumeric(CheckIsNumeric),
            Box::new(sub),
        )
    }

    /// Create a tree node the checks whether the sub node is a null.
    pub fn check_is_null(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::CheckIsNull(CheckIsNull), Box::new(sub))
    }

    /// Create a tree node the checks whether the sub node is an iri.
    pub fn check_is_iri(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::CheckIsIri(CheckIsIri), Box::new(sub))
    }

    /// Create a tree node the checks whether the sub node is a string.
    pub fn check_is_string(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CheckIsString(CheckIsString),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the conjunction of boolean values.
    ///
    /// This evaluates to `true` if all its subnodes evaluate to `true`
    /// and returns `false` otherwise.
    /// Returns `true` if there are no subnodes.
    pub fn boolean_conjunction(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::BooleanConjunction(BooleanConjunction),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the disjunction of two boolean values.
    ///
    /// This evaluates to `true` if one of its subnodes evaluates to `true`
    /// and returns `false` otherwise.
    /// Returns `false` if there are no subnodes.
    pub fn boolean_disjunction(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::BooleanDisjunction(BooleanDisjunction),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the negation of a boolean value.
    ///
    /// This evaluates to `true` if `sub` evaluates to `false`,
    /// and returns `false` if `sub` evaluates to `true`.
    pub fn boolean_negation(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::BooleanNegation(BooleanNegation),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 64-bit integer.
    ///
    /// This evaluates to an integer representation of `sub`.
    pub fn casting_to_integer64(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoInteger64(CastingIntoInteger64),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 32-bit float.
    ///
    /// This evaluates to a 32-bit floating point representation of `sub`.
    pub fn casting_to_float(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoFloat(CastingIntoFloat),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 64-bit float.
    ///
    /// This evaluates to a 64-bit floating point representation of `sub`.
    pub fn casting_to_double(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoDouble(CastingIntoDouble),
            Box::new(sub),
        )
    }

    /// Create a tree node representing addition between numbers.
    ///
    /// This evaluates to the sum of the values of the
    /// `left` and `right` subtree.
    pub fn numeric_addition(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericAddition(NumericAddition),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing subtraction between numbers.
    ///
    /// This evaluates to the difference between the values of the
    /// `left` and `right` subtree.
    pub fn numeric_subtraction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericSubtraction(NumericSubtraction),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing multiplication between numbers.
    ///
    /// This evaluates to the product of the values of the
    /// `left` and `right` subtree.
    pub fn numeric_multiplication(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericMultiplication(NumericMultiplication),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing division between numbers.
    ///
    /// This evaluates to the quotient of the values of the
    /// `left` and `right` subtree.
    pub fn numeric_division(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericDivision(NumericDivision),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing taking a logarithm of a number w.r.t. some base.
    ///
    /// This evaluates to the logarithm of the value resulting from
    /// evaluating the `value` w.r.t. the base resulting from evaluating the `base`.
    pub fn numeric_logarithm(value: Self, base: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLogarithm(NumericLogarithm),
            left: Box::new(value),
            right: Box::new(base),
        }
    }

    /// Create a tree node representing raising of a number to some power.
    ///
    /// This evaluates to the result from evaluating
    /// the `base` raised to the power obtained by evaluating `exponent`.
    pub fn numeric_power(base: Self, exponent: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericPower(NumericPower),
            left: Box::new(base),
            right: Box::new(exponent),
        }
    }

    /// Create a tree node representing the numeric remainder opreation.
    ///
    /// This evaluates to the remainder of the (truncated) division
    /// between the value of the left and the value of the right subnode.
    pub fn numeric_remainder(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericRemainder(NumericRemainder),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a greater than comparison between two numbers.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the `left` evaluates to a value greater than the value of `right`,
    /// and to `false` otherwise.
    pub fn numeric_greaterthan(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericGreaterthan(NumericGreaterthan),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a greater than or equals comparison between two numbers.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the `left` evaluates to a value greater than or equal to the value of `right`,
    /// and to `false` otherwise.
    pub fn numeric_greaterthaneq(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericGreaterthaneq(NumericGreaterthaneq),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a less than comparison between two numbers.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the `left` evaluates to a value less than the value of `right`,
    /// and to `false` otherwise.
    pub fn numeric_lessthan(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLessthan(NumericLessthan),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a less than or equals comparison between two numbers.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the `left` evaluates to a value less than or equal to the value of `right`,
    /// and to `false` otherwise.
    pub fn numeric_lessthaneq(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLessthaneq(NumericLessthaneq),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the absolute value of a number.
    ///
    /// This evaluates to the absolute value of `sub`.
    pub fn numeric_absolute(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericAbsolute(NumericAbsolute),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the cosine of a number.
    ///
    /// This evaluates to the cosine of `sub`.
    pub fn numeric_cosine(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericCosine(NumericCosine),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the negation of a number.
    ///
    /// This evaluates to the multiplicative inverse of `sub`.
    pub fn numeric_negation(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericNegation(NumericNegation),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the sine of a number.
    ///
    /// This evaluates to the sine of `sub`.
    pub fn numeric_sine(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::NumericSine(NumericSine), Box::new(sub))
    }

    /// Create a tree node representing the square root of a number.
    ///
    /// This evaluates to the square root of `sub`.
    pub fn numeric_squareroot(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericSquareroot(NumericSquareroot),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the rounding of a number.
    ///
    /// This evaluates to the rounded value of `sub`.
    pub fn numeric_round(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::NumericRound(NumericRound), Box::new(sub))
    }

    /// Create a tree node representing rounding up of a number.
    ///
    /// This evaluates to the rounded value of `sub`.
    pub fn numeric_ceil(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::NumericCeil(NumericCeil), Box::new(sub))
    }

    /// Create a tree node representing rounding down of a number.
    ///
    /// This evaluates to the rounded value of `sub`.
    pub fn numeric_floor(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::NumericFloor(NumericFloor), Box::new(sub))
    }

    /// Create a tree node representing the tangent of a number.
    ///
    /// This evaluates to the tangent of `sub`.
    pub fn numeric_tangent(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericTangent(NumericTangent),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the comparison of strings.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the `left` and `right` subtree evaluate to the same string
    /// and to `false` otherwise.
    pub fn string_compare(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringCompare(StringCompare),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the concatenation of strings.
    ///
    /// This evaluates to a new string by concatenating the strings
    /// resulting from each of the subnodes.
    pub fn string_concatenation(parameters: Vec<Self>) -> Self {
        Self::Nary {
            function: NaryFunctionEnum::StringConcatenation(StringConcatenation),
            parameters,
        }
    }

    /// Create a tree node representing the comparison of the beginning of a string.
    ///
    /// This evaluates to a boolean indicating whether
    /// the string resulting from evaluating `left` starts with
    /// the string resulting from evaluating `right`.
    pub fn string_starts(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringStarts(StringStarts),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the comparison of the beginning of a string.
    ///
    /// This evaluates to a boolean indicating whether
    /// the string resulting from evaluating `left` ends with
    /// the string resulting from evaluating `right`.
    pub fn string_ends(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringEnds(StringEnds),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the first part of a string
    ///
    /// This evaluates to a string resulting from taking the first half
    /// of evaluating `left` when split at the string resulting from evaluating `right`.
    pub fn string_before(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringBefore(StringBefore),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the first part of a string
    ///
    /// This evaluates to a string resulting from taking the second half
    /// of evaluating `left` when split at the string resulting from evaluating `right`.
    pub fn string_after(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringAfter(StringAfter),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a check whether a string is contained in another.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the subtree `substring` evaluates to a string that is contained
    /// in the string resulting from evaluating the subtree `text`
    /// and to `false` otherwise.
    pub fn string_contains(text: Self, substring: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringContains(StringContains),
            left: Box::new(text),
            right: Box::new(substring),
        }
    }

    /// Create a tree node representing a check whether a string, or substring, matches
    /// a pattern.
    ///
    /// This evaluates to `true` from the boolean value space
    /// if the subtree `text` evaluates to a string that matches
    /// the pattern resulting from evaluating the subtree `pattern`
    /// and to `false` otherwise.
    pub fn string_regex(text: Self, pattern: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringRegex(StringRegex),
            left: Box::new(text),
            right: Box::new(pattern),
        }
    }

    /// Create a tree node representing the length of a string.
    ///
    /// This evaluates to a number from the integer value space
    /// that is the length of the string that results from evaluating `sub`.
    pub fn string_length(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::StringLength(StringLength), Box::new(sub))
    }

    /// Create a tree node representing the reverse of a string.
    ///
    /// This evaluates to a reversed version of the string
    /// that results from evaluating `sub`.
    pub fn string_reverse(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::StringReverse(StringReverse),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the lowercase of a string.
    ///
    /// This evaluates to a lower cased version of the string
    /// that results from evaluating `sub`.
    pub fn string_lowercase(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::StringLowercase(StringLowercase),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the uppercase of a string.
    ///
    /// This evaluates to an upper cased version of the string
    /// that results from evaluating `sub`.
    pub fn string_uppercase(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::StringUppercase(StringUppercase),
            Box::new(sub),
        )
    }

    /// Create a tree node representing a substring operation.
    ///
    /// This evaluates to a string containing the
    /// characters from the string that results from evaluating `string`,
    /// starting from the position that results from evaluating `start`.
    pub fn string_subtstring(string: Self, start: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringSubstring(StringSubstring),
            left: Box::new(string),
            right: Box::new(start),
        }
    }

    /// Create a tree node representing a substring operation with a length parameter.
    ///
    /// This evaluates to a string containing the
    /// characters from the string that results from evaluating `string`,
    /// starting from the position that results from evaluating `start`
    /// with the maximum length given by evaluating `length`.
    pub fn string_subtstring_length(string: Self, start: Self, length: Self) -> Self {
        Self::Ternary {
            function: TernaryFunctionEnum::StringSubstringLength(StringSubstringLength),
            first: Box::new(string),
            second: Box::new(start),
            third: Box::new(length),
        }
    }

    /// Create a tree node representing the bitwise and operation.
    ///
    /// Evaluates to an integer resulting from performing the bitwise and operation
    /// on the binary representation of its integer subnodes.
    pub fn bit_and(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::BitAnd(BitAnd),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the bitwise or operation.
    ///
    /// Evaluates to an integer resulting from performing the bitwise or operation
    /// on the binary representation of its integer subnodes.
    pub fn bit_or(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::BitOr(BitOr),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the bitwise xor operation.
    ///
    /// Evaluates to an integer resulting from performing the bitwise xor operation
    /// on the binary representation of its integer subnodes.
    pub fn bit_xor(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::BitXor(BitXor),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the sum operation.
    ///
    /// Evaluates to a number resulting from adding its subnodes.
    pub fn numeric_sum(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::NumericSum(NumericSum),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the product operation.
    ///
    /// Evaluates to a number resulting from multiplying its subnodes.
    pub fn numeric_product(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::NumericProduct(NumericProduct),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the maximum operation.
    ///
    /// Evaluates to a number that is the maximum of its subnodes.
    pub fn numeric_maximum(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::NumericMaximum(NumericMaximum),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the minimum operation.
    ///
    /// Evaluates to a number that is the minimum of its subnodes.
    pub fn numeric_minimum(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::NumericMinimum(NumericMinimum),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }

    /// Create a tree node representing the Lukasiewicz t-norm.
    pub fn numeric_lukasiewicz(mut parameters: Vec<Self>) -> Self {
        if parameters.len() != 1 {
            Self::Nary {
                function: NaryFunctionEnum::NumericLukasiewicz(NumericLukasiewicz),
                parameters,
            }
        } else {
            parameters.remove(0)
        }
    }
}

impl<ReferenceType> FunctionTree<ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Return all the references in the tree.
    pub fn references(&self) -> Vec<ReferenceType> {
        match self {
            FunctionTree::Leaf(leaf) => match leaf {
                FunctionLeaf::Constant(_) => vec![],
                FunctionLeaf::Reference(reference) => vec![reference.clone()],
            },
            FunctionTree::Unary(_, sub) => sub.references(),
            FunctionTree::Binary {
                function: _,
                left,
                right,
            } => {
                let mut result = left.references();
                result.extend(right.references());

                result
            }
            FunctionTree::Ternary {
                function: _,
                first,
                second,
                third,
            } => {
                let mut result = first.references();
                result.extend(second.references());
                result.extend(third.references());

                result
            }
            FunctionTree::Nary {
                function: _,
                parameters,
            } => {
                let mut result = Vec::new();

                for parameter in parameters {
                    result.extend(parameter.references());
                }

                result
            }
        }
    }

    /// Return whether this tree evauluates to a constant value.
    pub fn is_constant(&self) -> bool {
        self.references().is_empty()
    }
}
