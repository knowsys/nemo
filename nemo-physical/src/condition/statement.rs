//! Provides data structures and functionality to represent conditional statements

use crate::arithmetic::{expression::StackProgram, traits::ArithmeticOperations};

/// A boolean-valued operation on two values.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConditionOperator {
    /// First operand is equal to the second operand.
    Equal,
    /// First operand is not equal to the second operand.
    Unequal,
    /// First operand is smaller than the second operand.
    LessThan,
    /// First operand is smaller than or equal the second operand.
    LessThanEqual,
    /// First operand is greater than the second operand.
    GreaterThan,
    /// First operand is greater than or equal to the second operand.
    GreaterThanEqual,
}

impl ConditionOperator {
    /// Returns a symbolic representation of this operator.
    pub fn symbol(&self) -> String {
        match self {
            ConditionOperator::Equal => "=".to_string(),
            ConditionOperator::Unequal => "!=".to_string(),
            ConditionOperator::LessThan => "<".to_string(),
            ConditionOperator::LessThanEqual => "<=".to_string(),
            ConditionOperator::GreaterThan => ">".to_string(),
            ConditionOperator::GreaterThanEqual => ">=".to_string(),
        }
    }

    /// Performs the operation represented by this object.
    pub fn evaluate<T: Ord + Eq>(&self, lhs: T, rhs: T) -> bool {
        match self {
            ConditionOperator::Equal => lhs == rhs,
            ConditionOperator::Unequal => lhs != rhs,
            ConditionOperator::LessThan => lhs < rhs,
            ConditionOperator::LessThanEqual => lhs <= rhs,
            ConditionOperator::GreaterThan => lhs > rhs,
            ConditionOperator::GreaterThanEqual => lhs >= rhs,
        }
    }
}

/// Represents a condition
#[derive(Debug, Clone)]
pub struct ConditionStatement<T> {
    /// Program to compute the left hand side expression.
    pub lhs: StackProgram<T>,
    /// Program to compute the right hand side expression.
    pub rhs: StackProgram<T>,
    /// The operation to be performed.
    pub operation: ConditionOperator,
}

impl<T> ConditionStatement<T> {
    /// Return the sub-expressions of this condition.
    pub fn expressions(&self) -> (&StackProgram<T>, &StackProgram<T>) {
        (&self.lhs, &self.rhs)
    }

    /// Iterates over the references used by this computation.
    /// May contain duplicates.
    pub fn references(&self) -> impl Iterator<Item = usize> + '_ {
        self.lhs.references().chain(self.rhs.references())
    }

    /// Return the maximum index that is referenced by a leaf node.
    /// Returns `None` if all the leaf nodes are constants.
    pub fn maximum_reference(&self) -> Option<usize> {
        let (left, right) = self.expressions();

        std::cmp::max(left.references().max(), right.references().max())
    }

    /// Returns the maximum stack size needed to compute this condition
    pub fn space_requirement(&self) -> usize {
        std::cmp::max(self.lhs.space_requirement(), self.rhs.space_requirement())
    }

    /// Creates an [`ConditionOperator::Equal`] condition.
    pub fn equal<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::Equal,
        }
    }

    /// Creates an [`ConditionOperator::Unequal`] condition.
    pub fn unequal<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::Unequal,
        }
    }

    /// Creates a [`ConditionOperator::GreaterThan`] condition.
    pub fn greater_than<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::GreaterThan,
        }
    }

    /// Creates a [`ConditionOperator::GreaterThanEqual`] condition.
    pub fn greater_than_equal<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::GreaterThanEqual,
        }
    }

    /// Creates a [`ConditionOperator::LessThan`] condition.
    pub fn less_than<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::LessThan,
        }
    }

    /// Creates a [`ConditionOperator::LessThanEqual`] condition.
    pub fn less_than_equal<SP: Into<StackProgram<T>>>(lhs: SP, rhs: SP) -> Self {
        ConditionStatement {
            lhs: lhs.into(),
            rhs: rhs.into(),
            operation: ConditionOperator::LessThanEqual,
        }
    }
}

impl<T: ArithmeticOperations + Copy + Eq + Ord> ConditionStatement<T> {
    /// Evaluates the condition represented by this object.
    pub fn evaluate(&self, stack: &mut Vec<T>, referenced_values: &[T]) -> Option<bool> {
        let lhs = self.lhs.evaluate(stack, referenced_values)?;
        let rhs = self.rhs.evaluate(stack, referenced_values)?;
        Some(self.operation.evaluate(lhs, rhs))
    }
}
