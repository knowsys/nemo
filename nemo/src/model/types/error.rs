use super::primitive_types::PrimitiveType;
use crate::model::Term;

use nemo_physical::error::ReadingError;
use thiserror::Error;

/// An [`InvalidRuleTermConversion`]
#[derive(Debug, Error)]
#[error("The term \"{}\" cannot be converted to a {}.", .term, .target_type)]
pub struct InvalidRuleTermConversion {
    term: Term,
    target_type: PrimitiveType,
}

impl InvalidRuleTermConversion {
    /// Create new `InvalidRuleTermConversion` error
    pub fn new(term: Term, target_type: PrimitiveType) -> Self {
        Self { term, target_type }
    }
}

impl From<InvalidRuleTermConversion> for ReadingError {
    fn from(value: InvalidRuleTermConversion) -> Self {
        Self::TypeConversionError(value.term.to_string(), value.target_type.to_string())
    }
}

/// Errors that can occur during type checking
#[derive(Error, Debug)]
pub enum TypeError {
    /// Conflicting type declarations
    #[error("Conflicting type declarations. Predicate \"{0}\" at position {1} has been inferred to have the conflicting types {2} and {3}.")]
    InvalidRuleConflictingTypes(String, usize, PrimitiveType, PrimitiveType),
    /// Conflicting type conversions
    #[error(transparent)]
    InvalidRuleTermConversion(#[from] InvalidRuleTermConversion),
    /// Comparison of a non-numeric type
    #[error("Invalid type declarations. Comparison operator can only be used with numeric types.")]
    InvalidRuleNonNumericComparison,
    /// Arithmetic operations with of a non-numeric type
    #[error(
        "Invalid type declarations. Arithmetic operations can only be used with numeric types."
    )]
    InvalidRuleNonNumericArithmetic,
}
