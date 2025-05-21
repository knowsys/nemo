//! This module defines [ProgramReport].

use std::{fmt::Display, ops::Range};

use ariadne::Source;

use crate::rule_file::RuleFile;

use super::{context::ContextError, rich::RichError};

/// Collects errors (and warning) that may occur in a nemo program
#[derive(Debug)]
pub struct ProgramReport {
    /// Program
    program: RuleFile,

    /// List of all warnings
    warnings: Vec<ContextError>,
    /// List of all errors
    errors: Vec<ContextError>,
}

impl Display for ProgramReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for warning in &self.warnings {
            writeln!(f, "{}", warning)?;
        }
        for error in &self.errors {
            writeln!(f, "{}", error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ProgramReport {}

impl ProgramReport {
    /// Create a new empty [ProgramReport].
    pub fn new(program: RuleFile) -> Self {
        Self {
            program,
            warnings: Vec::default(),
            errors: Vec::default(),
        }
    }

    /// Return an iterator over all warnings.
    pub fn warnings(&self) -> impl Iterator<Item = &ContextError> {
        self.warnings.iter()
    }

    /// Return an iterator over all errors.
    pub fn errors(&self) -> impl Iterator<Item = &ContextError> {
        self.errors.iter()
    }

    /// Check if there are errors.
    pub fn contains_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Print the error messages, provided the source string and label
    pub fn eprint(&self) -> Result<(), std::io::Error> {
        for error in self.warnings.iter().chain(self.errors.iter()) {
            error.report(self.program.name()).eprint((
                self.program.name().to_owned(),
                Source::from(self.program.content()),
            ))?;
        }

        Ok(())
    }

    /// Write this report to a given writer.
    pub fn write(&self, writer: &mut impl std::io::Write) -> Result<(), std::io::Error> {
        for error in self.warnings.iter().chain(self.errors.iter()) {
            error.report(self.program.name()).write(
                (
                    self.program.name().to_owned(),
                    Source::from(self.program.content()),
                ),
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
