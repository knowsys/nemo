//! This module defines a tree representation

use std::fmt::Debug;

use crate::datavalues::AnyDataValue;

use super::definitions::{
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
        StringUppercase,
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
#[derive(Debug)]
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

    /// Create a tree node representing the conjunction of two boolean values.
    pub fn boolean_conjunction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::BooleanConjunction(BooleanConjunction),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the disjunction of two boolean values.
    pub fn boolean_disjunction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::BooleanDisjunction(BooleanDisjunction),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the negation of a boolean value.
    pub fn boolean_negation(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::BooleanNegation(BooleanNegation),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 64-bit integer.
    pub fn casting_to_integer64(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoInteger64(CastingIntoInteger64),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 32-bit float.
    pub fn casting_to_float(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoFloat(CastingIntoFloat),
            Box::new(sub),
        )
    }

    /// Create a tree node representing casting a value into an 64-bit float.
    pub fn casting_to_double(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::CastingIntoDouble(CastingIntoDouble),
            Box::new(sub),
        )
    }

    /// Create a tree node representing addition between numbers.
    pub fn numeric_addition(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericAddition(NumericAddition),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing subtraction between numbers.
    pub fn numeric_subtraction(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericSubtraction(NumericSubtraction),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing multiplication between numbers.
    pub fn numeric_multiplication(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericMultiplication(NumericMultiplication),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing division between numbers.
    pub fn numeric_division(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericDivision(NumericDivision),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing taking a logarithm of a number w.r.t. some base.
    pub fn numeric_logarithm(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLogarithm(NumericLogarithm),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing raising of a number to some power.
    pub fn numeric_power(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericPower(NumericPower),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a greater than comparison between two numbers.
    pub fn numeric_greaterthan(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericGreaterthan(NumericGreaterthan),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a greater than or equals comparison between two numbers.
    pub fn numeric_greaterthaneq(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericGreaterthaneq(NumericGreaterthaneq),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a less than comparison between two numbers.
    pub fn numeric_lessthan(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLessthan(NumericLessthan),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing a less than or equals comparison between two numbers.
    pub fn numeric_lessthaneq(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::NumericLessthaneq(NumericLessthaneq),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the absolute value of a number.
    pub fn numeric_absolute(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericAbsolute(NumericAbsolute),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the cosine of a number.
    pub fn numeric_cosine(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericCosine(NumericCosine),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the negation of a number.
    pub fn numeric_negation(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericNegation(NumericNegation),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the sine of a number.
    pub fn numeric_sine(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::NumericSine(NumericSine), Box::new(sub))
    }

    /// Create a tree node representing the square root of a number.
    pub fn numeric_squareroot(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericSquareroot(NumericSquareroot),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the tangent of a number.
    pub fn numeric_tangent(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::NumericTangent(NumericTangent),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the comparison of strings.
    pub fn string_compare(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringCompare(StringCompare),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the concatenation of strings.
    pub fn string_concatenation(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringConcatenation(StringConcatenation),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing checking whether a string is contained in another.
    pub fn string_contains(left: Self, right: Self) -> Self {
        Self::Binary {
            function: BinaryFunctionEnum::StringContains(StringContains),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a tree node representing the length of a string.
    pub fn string_length(sub: Self) -> Self {
        Self::Unary(UnaryFunctionEnum::StringLength(StringLength), Box::new(sub))
    }

    /// Create a tree node representing the lowercase of a string.
    pub fn string_lowercase(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::StringLowercase(StringLowercase),
            Box::new(sub),
        )
    }

    /// Create a tree node representing the uppercase of a string.
    pub fn string_uppercase(sub: Self) -> Self {
        Self::Unary(
            UnaryFunctionEnum::StringUppercase(StringUppercase),
            Box::new(sub),
        )
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
}
