//! This module defines [ProgramValidationError]

pub mod translation_error;
pub mod validation_error;

use std::fmt::Display;

use validation_error::ValidationErrorKind;

use super::components::ProgramComponent;

/// Error that occurs during validation of a program.
#[derive(Debug)]
pub struct ProgramValidationError {
    /// The kind of error
    kind: ValidationErrorKind,
    /// stack of components in which the error occurred
    context: Vec<Box<dyn ProgramComponent>>,
}

impl Display for ProgramValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

// #[derive(Debug)]
// pub struct ProgramValidationErrors {}
