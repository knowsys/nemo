//! This module defines [ProgramReport].

use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use ariadne::Source;

use crate::{
    parser::ParserErrorReport,
    rule_file::RuleFile,
    rule_model::{
        error::{TranslationReport, ValidationReport},
        translation::ProgramParseReport,
    },
};

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

    /// Check whether report is empty.
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.errors.is_empty()
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
    pub fn eprint(&self, disbale_warnings: bool) -> Result<(), std::io::Error> {
        if !disbale_warnings {
            for warning in self.warnings() {
                warning.report(self.program.name()).eprint((
                    self.program.name().to_owned(),
                    Source::from(self.program.content()),
                ))?;
            }
        }

        for error in self.errors() {
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

    /// Merge another [ProgramReport] into this one.
    pub fn merge(&mut self, other: Self) {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
    }

    /// Merge a list of [ContextError]s into [ProgramReport].
    pub fn merge_errors<Iter>(&mut self, errors: Iter)
    where
        Iter: Iterator<Item = (ContextError, bool)>,
    {
        for (error, is_warning) in errors {
            if is_warning {
                self.warnings.push(error)
            } else {
                self.errors.push(error)
            }
        }
    }

    /// Merge a [ValidationReport] into this [ProgramReport].
    pub fn merge_validation(&mut self, report: ValidationReport) {
        self.merge_errors(report.context_errors());
    }

    /// Merge a [ValidationReport] into this [TranslationReport].
    pub fn merge_translation(&mut self, report: TranslationReport) {
        self.merge_errors(report.context_errors());
    }

    /// Merge a [ValidationReport] into this [ProgramReport].
    pub fn merge_parser(&mut self, report: ParserErrorReport) {
        self.merge_errors(report.context_errors());
    }

    /// Merge a [ValidationReport] into this [ProgramParseReport].
    pub fn merge_program_parser(&mut self, report: ProgramParseReport) {
        match report {
            ProgramParseReport::Parsing(report) => self.merge_parser(report),
            ProgramParseReport::Translation(report) => self.merge_translation(report),
        }
    }
}
