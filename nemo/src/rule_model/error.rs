//! This module defines different kinds of errors that can occur
//! while working with nemo programs.

use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use translation_error::TranslationError;
use validation_error::ValidationError;

use crate::error::{context::ContextError, report::ProgramReport, rich::RichError};

use super::{components::ComponentSource, origin::Origin};

pub mod hint;
pub mod info;
pub mod translation_error;
pub mod validation_error;

/// Error message associated with a source
#[derive(Debug)]
pub(crate) struct SourceMessage<Source> {
    /// Source
    source: Source,
    /// Message
    message: String,
}

/// Error associated wiht a source
#[derive(Debug)]
pub struct SourceError<Source, Error>
where
    Error: Debug + RichError,
    Source: Debug,
{
    /// Error
    error: Error,
    /// Source
    source: Source,
    /// Additional information
    context: Vec<SourceMessage<Source>>,
    /// Hint
    hints: Vec<String>,
}

impl<Source, Error> SourceError<Source, Error>
where
    Error: Debug + RichError,
    Source: Debug + Clone,
{
    /// Create a new [SourceError].
    pub fn new<Object: ComponentSource<Source = Source>>(error: Error, object: &Object) -> Self {
        Self {
            error,
            source: object.origin(),
            context: Vec::default(),
            hints: Vec::default(),
        }
    }

    /// Create a new [SourceError] from a source.
    pub fn new_source(error: Error, source: Source) -> Self {
        Self {
            error,
            source,
            context: Vec::default(),
            hints: Vec::default(),
        }
    }

    /// Check if this error should be treated as a warning.
    pub fn is_warning(&self) -> bool {
        self.error.is_warning()
    }

    /// Add more context to the error.
    pub fn add_context<Message: Display, Object: ComponentSource<Source = Source>>(
        &mut self,
        object: &Object,
        message: Message,
    ) -> &mut Self {
        self.add_context_source(object.origin(), message)
    }

    /// Add more context to the error attached to a source.
    pub fn add_context_source<Message: Display>(
        &mut self,
        source: Source,
        message: Message,
    ) -> &mut Self {
        self.context.push(SourceMessage {
            source,
            message: message.to_string(),
        });

        self
    }

    /// Add a new hint to the error.
    pub fn add_hint<Message: Display>(&mut self, hint: Message) -> &mut Self {
        self.hints.push(hint.to_string());
        self
    }

    /// Add a new hint to the error if `hint` is Some.
    /// Does nothing otherwise.
    pub fn add_hint_option<Message: Display>(&mut self, hint: Option<Message>) -> &mut Self {
        if let Some(hint) = hint {
            self.add_hint(hint);
        }

        self
    }
}

/// Report containing multiple errors
#[derive(Debug)]
pub struct SourceErrorReport<Source, Error>
where
    Error: Debug + RichError,
    Source: Debug,
{
    /// List of errors
    errors: Vec<SourceError<Source, Error>>,
}

impl<Source, Error> Default for SourceErrorReport<Source, Error>
where
    Error: Debug + RichError,
    Source: Debug,
{
    fn default() -> Self {
        Self {
            errors: Default::default(),
        }
    }
}

impl<Source, Error> SourceErrorReport<Source, Error>
where
    Error: Debug + RichError,
    Source: Debug + Clone,
{
    /// Create a new report containing the given error.
    pub fn single<Object: ComponentSource<Source = Source>>(object: &Object, error: Error) -> Self {
        let mut report = Self::default();
        report.errors.push(SourceError::new(error, object));
        report
    }

    /// Add a new error to the report.
    pub fn add<Object: ComponentSource<Source = Source>>(
        &mut self,
        object: &Object,
        error: Error,
    ) -> &mut SourceError<Source, Error> {
        self.errors.push(SourceError::new(error, object));
        self.errors
            .last_mut()
            .expect("error was pushed in last line")
    }

    /// Add a new error to the report by providing the source.
    pub fn add_source(&mut self, source: Source, error: Error) -> &mut SourceError<Source, Error> {
        self.errors.push(SourceError::new_source(error, source));
        self.errors
            .last_mut()
            .expect("error was pushed in last line")
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

    /// Convert this report into a [Result]
    /// containing the given value or `self`
    /// depending on whether it contains any errors.
    pub fn result_value<Value>(self, value: Value) -> Result<Value, Self> {
        if self.errors.is_empty() {
            Ok(value)
        } else {
            Err(self)
        }
    }

    /// Merge another report given as a `Result`.
    pub fn merge(&mut self, other: Result<(), Self>) {
        if let Err(report) = other {
            self.errors.extend(report.errors);
        }
    }

    /// Merge another report.
    pub fn merge_report(&mut self, other: Self) {
        self.errors.extend(other.errors);
    }

    /// Return an iterator over all [ValidationError]s.
    pub fn errors(&self) -> impl Iterator<Item = &Error> {
        self.errors.iter().map(|error| &error.error)
    }

    /// Translate this [ValidationReport] into a [ProgramReport].
    pub fn program_report<Translation>(self, translation: Translation) -> ProgramReport
    where
        Translation: Fn(Source) -> Range<usize>,
    {
        let mut result = ProgramReport::default();

        for error in self.errors {
            let is_warning = error.is_warning();
            let context_error =
                ContextError::new(error.error, translation(error.source)).add_hints(error.hints);

            if is_warning {
                result.add_warning(context_error);
            } else {
                result.add_error(context_error);
            }
        }

        result
    }
}

/// Error that can occur because of incorrectly constructed program components
pub type ValidationReport = SourceErrorReport<Origin, ValidationError>;
/// Error that can occur due to syntactically ill formed statements
pub type TranslationReport = SourceErrorReport<Range<usize>, TranslationError>;
