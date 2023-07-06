//! This module defines the logical datatype model

use thiserror::Error;

pub mod primitive_types;
use primitive_types::PrimitiveType;

pub mod complex_types;

use super::Term;

/// Errors that can occur during type checking
#[derive(Error, Debug)]
pub enum TypeError {
    /// Conflicting type declarations
    #[error("Conflicting type declarations. Predicate \"{0}\" at position {1} has been inferred to have the conflicting types {2} and {3}.")]
    InvalidRuleConflictingTypes(String, usize, PrimitiveType, PrimitiveType),
    /// Conflicting type conversions
    #[error("Conflicting type declarations. The term \"{0}\" cannot be converted to a {1}.")]
    InvalidRuleTermConversion(Term, PrimitiveType),
    /// Comparison of a non-numeric type
    #[error("Invalid type declarations. Comparison operator can only be used with numeric types.")]
    InvalidRuleNonNumericComparison,
    /// Arithmetic operations with of a non-numeric type
    #[error(
        "Invalid type declarations. Arithmetic operations can only be used with numeric types."
    )]
    InvalidRuleNonNumericArithmetic,
}
