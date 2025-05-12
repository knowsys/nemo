//! This module defines [ProgramErrors].

use std::ops::Range;

use ariadne::Source;

use super::{context::ContextError, rich::RichError};

/// Collects errors (and warning) that may occur in a nemo program
#[derive(Debug)]
pub struct ProgramErrors {
    /// List of all warnings
    warnings: Vec<ContextError>,
    /// List of all errors
    errors: Vec<ContextError>,
}

impl ProgramErrors {
    /// Check if there are errors.
    pub fn contains_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Print the error messages, provided the source string and label
    pub fn eprint(&self, source_string: &str, source_label: &str) -> Result<(), std::io::Error> {
        for error in self.warnings.iter().chain(self.errors.iter()) {
            error
                .report(source_label)
                .eprint((source_label.to_owned(), Source::from(source_string)))?;
        }

        Ok(())
    }

    /// Report a new error.
    pub fn report_error<Error: RichError>(
        &mut self,
        error: Error,
        range: Range<usize>,
    ) -> &mut ContextError {
        if error.is_warning() {
            self.warnings.push(ContextError::new(error, range));
            self.warnings.last_mut().expect("push in previous line")
        } else {
            self.errors.push(ContextError::new(error, range));
            self.errors.last_mut().expect("push in previous line")
        }
    }

    /// Merge another [ProgramErrors] into this one.
    pub fn merge(&mut self, other: Self) {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
    }
}
