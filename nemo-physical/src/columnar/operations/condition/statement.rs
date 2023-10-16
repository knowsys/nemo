//! Provides data structures and functionality to represent conditional statements

use std::fmt::Display;

use crate::columnar::operations::arithmetic::{
    expression::{ArithmeticTree, ArithmeticTreeLeaf},
    traits::ArithmeticOperations,
};

/// Represents a condition
#[derive(Debug, Clone)]
pub enum ConditionStatement<T> {
    /// First entry is equal to the second entry.
    Equal(ArithmeticTree<T>, ArithmeticTree<T>),
    /// First entry is not equal to the second entry.
    Unequal(ArithmeticTree<T>, ArithmeticTree<T>),
    /// First entry is smaller than the second entry.
    LessThan(ArithmeticTree<T>, ArithmeticTree<T>),
    /// First entry is smaller than or equal the second entry.
    LessThanEqual(ArithmeticTree<T>, ArithmeticTree<T>),
    /// First entry is greater than the second entry.
    GreaterThan(ArithmeticTree<T>, ArithmeticTree<T>),
    /// First entry is greater than or equal to the second entry.
    GreaterThanEqual(ArithmeticTree<T>, ArithmeticTree<T>),
}

impl<T> Display for ConditionStatement<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (left, right) = self.expressions();

        left.fmt(f)?;
        f.write_fmt(format_args!(" {} ", self.symbol_condition()))?;
        right.fmt(f)
    }
}

impl<T> ConditionStatement<T> {
    /// Return the sub-expressions of this condition.
    pub fn expressions(&self) -> (&ArithmeticTree<T>, &ArithmeticTree<T>) {
        match self {
            ConditionStatement::Equal(left, right) => (left, right),
            ConditionStatement::Unequal(left, right) => (left, right),
            ConditionStatement::LessThan(left, right) => (left, right),
            ConditionStatement::LessThanEqual(left, right) => (left, right),
            ConditionStatement::GreaterThan(left, right) => (left, right),
            ConditionStatement::GreaterThanEqual(left, right) => (left, right),
        }
    }

    /// Return the left sub expression of this condition.
    pub fn left(&self) -> &ArithmeticTree<T> {
        self.expressions().0
    }

    /// Return the right sub expression of this condition.
    pub fn right(&self) -> &ArithmeticTree<T> {
        self.expressions().1
    }

    /// Associates a string symbol to each
    pub fn symbol_condition(&self) -> String {
        match self {
            ConditionStatement::Equal(_, _) => "=".to_string(),
            ConditionStatement::Unequal(_, _) => "!=".to_string(),
            ConditionStatement::LessThan(_, _) => "<".to_string(),
            ConditionStatement::LessThanEqual(_, _) => "<=".to_string(),
            ConditionStatement::GreaterThan(_, _) => ">".to_string(),
            ConditionStatement::GreaterThanEqual(_, _) => ">=".to_string(),
        }
    }
}

impl<T> ConditionStatement<T>
where
    T: Clone,
{
    /// Return the maximum index that is referenced by a leaf node.
    /// Returns `None` if all the leaf nodes are constants.
    pub fn maximum_reference(&self) -> Option<usize> {
        let (left, right) = self.expressions();

        std::cmp::max(left.maximum_reference(), right.maximum_reference())
    }

    /// Apply `function` to every leaf node of the tree
    /// and return a new [`ArithmeticTree`] possibly of a different type.
    pub fn map<O>(
        &self,
        function: &dyn Fn(ArithmeticTreeLeaf<T>) -> ArithmeticTreeLeaf<O>,
    ) -> ConditionStatement<O>
    where
        O: Clone,
    {
        match self {
            ConditionStatement::Equal(left, right) => {
                ConditionStatement::Equal(left.map(function), right.map(function))
            }
            ConditionStatement::Unequal(left, right) => {
                ConditionStatement::Unequal(left.map(function), right.map(function))
            }
            ConditionStatement::LessThan(left, right) => {
                ConditionStatement::LessThan(left.map(function), right.map(function))
            }
            ConditionStatement::LessThanEqual(left, right) => {
                ConditionStatement::LessThanEqual(left.map(function), right.map(function))
            }
            ConditionStatement::GreaterThan(left, right) => {
                ConditionStatement::GreaterThan(left.map(function), right.map(function))
            }
            ConditionStatement::GreaterThanEqual(left, right) => {
                ConditionStatement::GreaterThanEqual(left.map(function), right.map(function))
            }
        }
    }
}

impl<T> ConditionStatement<T>
where
    T: Ord + Eq + ArithmeticOperations,
{
    /// Evaluate the condition.
    /// Returns `None` if some expression is undefined.
    pub fn evaluate(&self, referenced_values: &[T]) -> Option<bool> {
        match self {
            ConditionStatement::Equal(left, right) => {
                Some(left.evaluate(referenced_values)? == right.evaluate(referenced_values)?)
            }
            ConditionStatement::Unequal(left, right) => {
                Some(left.evaluate(referenced_values)? != right.evaluate(referenced_values)?)
            }
            ConditionStatement::LessThan(left, right) => {
                Some(left.evaluate(referenced_values)? < right.evaluate(referenced_values)?)
            }
            ConditionStatement::LessThanEqual(left, right) => {
                Some(left.evaluate(referenced_values)? <= right.evaluate(referenced_values)?)
            }
            ConditionStatement::GreaterThan(left, right) => {
                Some(left.evaluate(referenced_values)? > right.evaluate(referenced_values)?)
            }
            ConditionStatement::GreaterThanEqual(left, right) => {
                Some(left.evaluate(referenced_values)? >= right.evaluate(referenced_values)?)
            }
        }
    }
}
