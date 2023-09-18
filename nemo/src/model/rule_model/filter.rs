use super::{PrimitiveValue, Variable};

/// Operation for a filter
#[derive(Debug, Eq, PartialEq, Clone, Copy, PartialOrd, Ord)]
pub enum FilterOperation {
    /// Variable is equal to the term
    Equals,
    /// Variable is not equal to the term
    Unequals,
    /// Value of variable is less than the value of the term
    LessThan,
    /// Value of variable is greater than the value of the term
    GreaterThan,
    /// Value of variable is less than or equal to the value of the term
    LessThanEq,
    /// Value of variable is gretaer than or equal to the value of the term
    GreaterThanEq,
}

impl FilterOperation {
    /// Flips the operation: for `op`, returns a suitable operation
    /// `op'` such that `x op y` iff `y op' x`.
    pub fn flip(&self) -> Self {
        match self {
            Self::Equals => Self::Equals,
            Self::Unequals => Self::Unequals,
            Self::LessThan => Self::GreaterThan,
            Self::GreaterThan => Self::LessThan,
            Self::LessThanEq => Self::GreaterThanEq,
            Self::GreaterThanEq => Self::LessThanEq,
        }
    }
}

/// Filter of the form `<variable> <operation> <term>`
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Filter {
    /// Operation to be performed
    pub operation: FilterOperation,
    /// Left-hand side
    pub lhs: Variable,
    /// Right-hand side
    pub rhs: PrimitiveValue,
}

impl Filter {
    /// Creates a new [`Filter`]
    pub fn new(operation: FilterOperation, lhs: Variable, rhs: PrimitiveValue) -> Self {
        Self {
            operation,
            lhs,
            rhs,
        }
    }

    /// Creates a new [`Filter]` with the arguments flipped
    pub fn flipped(operation: FilterOperation, lhs: PrimitiveValue, rhs: Variable) -> Self {
        Self::new(operation.flip(), rhs, lhs)
    }
}
