//! This module defines a tree representation

use std::fmt::Debug;

use crate::datavalues::AnyDataValue;

use super::definitions::{
    boolean::{BooleanConjunction, BooleanDisjunction, BooleanNegation},
    casting::{CastingIntoDouble, CastingIntoFloat, CastingIntoInteger64},
    generic::{Equals, Unequals},
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
    BinaryFunctionEnum, UnaryFunctionEnum,
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
}

impl<ReferenceType> FunctionTree<ReferenceType>
where
    ReferenceType: Debug + Clone,
{
    /// Create a leaf node with a constant.
    pub fn constant(constant: AnyDataValue) -> Self {
        Self::Leaf(FunctionLeaf::Constant(constant))
    }

    /// Create a leaf node with a referene.
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

    /// Create a tree node representing the conjunction of two boolean values.
    ///
    /// This evaluates to `true` if `left` and `right` evaluate to `true`,
    /// and returns `false` otherwise.
    pub fn boolean_conjunction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::BooleanConjunction(BooleanConjunction),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the disjunction of two boolean values.
    ///
    /// This evaluates to `true` if `left` or `right` (or both) evaluate to `true`,
    /// and returns `false` otherwise.
    pub fn boolean_disjunction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::BooleanDisjunction(BooleanDisjunction),
            left: Box::new(left),
            right: Box::new(right),
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
    /// This evaluates to a new string by appending the string
    /// resulting from evaluating the `right` subtree to the end
    /// of the string resulting when evaluating the `left` subtree.
    pub fn string_concatenation(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringConcatenation(StringConcatenation),
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

    /// Create a tree node representing the length of a string.
    ///
    /// This evaluates to a number from the integer value space
    /// that is the length of the string that results from evaluating `sub`.
    pub fn string_length(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::StringLength(StringLength), Box::new(sub))
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
    /// This evaluates to a string containing the first $n$
    /// characters from the string that results from evaluating `string`,
    /// where $n$ is the integer that results from evaluating `length`.
    pub fn string_subtstring(string: Self, length: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringSubstring(StringSubstring),
            left: Box::new(string),
            right: Box::new(length),
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
        }
    }

    /// Return whether this tree evauluates to a constant value.
    pub fn is_constant(&self) -> bool {
        self.references().is_empty()
    }
}
