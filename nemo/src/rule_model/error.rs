//! This module defines different kinds of errors that can occur
//! while working with nemo programs.

use translation_error::TranslationError;
use validation_error::ValidationError;

use crate::error::rich::RichError;

use super::{components::ComponentIdentity, origin::Origin};

pub mod hint;
pub mod info;
pub mod translation_error;
pub mod validation_error;

/// Error associated with an [Origin]
#[derive(Debug)]
pub(crate) struct OriginatedError<Error: RichError> {
    /// The error
    error: Error,
    /// Origin of the error
    origin: Origin,
}

impl<Error: RichError> OriginatedError<Error> {
    /// Create a new [OriginatedError].
    pub fn new(error: Error, origin: Origin) -> Self {
        Self { error, origin }
    }
}

/// Collection of errors occurring within a program component
#[derive(Debug)]
pub struct ComponentErrorReport<Error: RichError> {
    errors: Vec<OriginatedError<Error>>,
}

impl<Error: RichError> Default for ComponentErrorReport<Error> {
    fn default() -> Self {
        Self {
            errors: Default::default(),
        }
    }
}

impl<Error: RichError> ComponentErrorReport<Error> {
    /// Add a new error to the report.
    pub fn add(&mut self, component: &dyn ComponentIdentity, error: Error) {
        self.errors
            .push(OriginatedError::new(error, component.origin().clone()));
    }

    /// Convert this report into a [Result],
    /// depending on whether it contains any errors.
    pub fn result(self) -> Result<(), Self> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }

    /// Merge another report.
    pub fn merge(&mut self, other: Result<(), Self>) {
        if let Err(report) = other {
            self.errors.extend(report.errors);
        }
    }
}

/// Error that can occur due to syntactically ill formed statements
pub type TranslationReport = ComponentErrorReport<TranslationError>;
/// Error that can occur because of incorrectly constructed program components
pub type ValidationReport = ComponentErrorReport<ValidationError>;
