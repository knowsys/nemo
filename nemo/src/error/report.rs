//! This module defines [ProgramReport].

use std::ops::Range;

use ariadne::Source;

use super::{context::ContextError, rich::RichError};

/// Collects errors (and warning) that may occur in a nemo program
#[derive(Debug)]
pub struct ProgramReport {
    /// List of all warnings
    warnings: Vec<ContextError>,
    /// List of all errors
    errors: Vec<ContextError>,
}

impl Default for ProgramReport {
    fn default() -> Self {
        Self {
            warnings: Default::default(),
            errors: Default::default(),
        }
    }
}

impl ProgramReport {
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

    /// Write this report to a given writer.
    pub fn write(
        &self,
        writer: &mut impl std::io::Write,
        source_string: &str,
        source_label: &str,
    ) -> Result<(), std::io::Error> {
        for error in self.warnings.iter().chain(self.errors.iter()) {
            error.report(source_label).write(
                (source_label.to_owned(), Source::from(source_string)),
                &mut *writer,
            )?
        }

        Ok(())
    }

    /// Add a new [ContextError].
    pub fn add_error(&mut self, error: ContextError) -> &mut ContextError {
        self.errors.push(error);
        self.errors.last_mut().expect("push in previous line")
    }

    /// Add a new [ContextError] treated as a warning.
    pub fn add_warning(&mut self, warning: ContextError) -> &mut ContextError {
        self.warnings.push(warning);
        self.warnings.last_mut().expect("push in previous line")
    }

    /// Report a new error.
    pub fn report_error<Error: RichError>(
        &mut self,
        error: Error,
        range: Range<usize>,
    ) -> &mut ContextError {
        if error.is_warning() {
            self.add_warning(ContextError::new(error, range))
        } else {
            self.add_error(ContextError::new(error, range))
        }
    }

    /// Merge another [ProgramErrors] into this one.
    pub fn merge(&mut self, other: Self) {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
    }
}
