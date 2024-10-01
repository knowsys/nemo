//! This module defines different kinds of errors that can occur
//! while working with nemo programs.

pub mod hint;
pub mod info;
pub mod translation_error;
pub mod validation_error;

use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use ariadne::{Color, Config, Label, ReportBuilder};
use hint::Hint;
use translation_error::TranslationErrorKind;
use validation_error::ValidationErrorKind;

use crate::parser::span::{CharacterRange, Span};

use super::origin::Origin;

/// Types of [ComplexError]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComplexErrorKind {
    /// Error
    Error,
    /// Warning,
    Warning,
}

/// Types of [ComplexErrorLabel]s
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComplexErrorLabelKind {
    /// Error
    Error,
    /// Warning,
    Warning,
    /// Information
    Information,
}

/// Label of a [ComplexError]
#[derive(Clone, Debug)]
pub struct ComplexErrorLabel<Reference>
where
    Reference: Debug,
{
    /// Kind of label
    kind: ComplexErrorLabelKind,
    /// Reference to the source code
    reference: Reference,
    /// Message
    message: String,
}

/// Complex error that additional information to an error
#[derive(Clone, Debug)]
pub struct ComplexError<Reference>
where
    Reference: Debug,
{
    /// Type of error
    pub kind: ComplexErrorKind,
    /// Where this error occurred
    pub reference: Reference,
    /// Labels
    pub labels: Vec<ComplexErrorLabel<Reference>>,
    /// Hints
    pub hints: Vec<Hint>,
}

impl<Reference> ComplexError<Reference>
where
    Reference: Debug,
{
    /// Check whether this is a error (and not a warning).
    pub fn is_error(&self) -> bool {
        matches!(self.kind, ComplexErrorKind::Error)
    }

    /// Create a new error.
    pub fn new_error(reference: Reference) -> Self {
        Self {
            kind: ComplexErrorKind::Error,
            reference,
            labels: Vec::default(),
            hints: Vec::default(),
        }
    }

    /// Create a new warning
    pub fn new_warning(reference: Reference) -> Self {
        Self {
            kind: ComplexErrorKind::Warning,
            reference,
            labels: Vec::default(),
            hints: Vec::default(),
        }
    }

    /// Add a new label to the error.
    pub fn add_label<Message: Display>(
        &mut self,
        kind: ComplexErrorLabelKind,
        reference: Reference,
        message: Message,
    ) -> &mut Self {
        self.labels.push(ComplexErrorLabel {
            kind,
            reference,
            message: message.to_string(),
        });

        self
    }

    /// Add a new hint to the error.
    pub fn add_hint(&mut self, hint: Hint) -> &mut Self {
        self.hints.push(hint);

        self
    }

    /// Add a new hint to the error if `hint` is Some.
    /// Does nothing otherwise.
    pub fn add_hint_option(&mut self, hint: Option<Hint>) -> &mut Self {
        if let Some(hint) = hint {
            self.hints.push(hint);
        }

        self
    }

    /// Add this information to a [ReportBuilder].
    pub fn report<'a, Translation>(
        &self,
        mut report: ReportBuilder<'a, (String, Range<usize>)>,
        source_label: String,
        translation: Translation,
    ) -> ReportBuilder<'a, (String, Range<usize>)>
    where
        Translation: Fn(&Reference) -> Range<usize>,
    {
        for label in &self.labels {
            let color = match label.kind {
                ComplexErrorLabelKind::Error => Color::Red,
                ComplexErrorLabelKind::Warning => Color::Yellow,
                ComplexErrorLabelKind::Information => Color::BrightBlue,
            };

            report = report
                .with_label(
                    Label::new((source_label.clone(), translation(&label.reference)))
                        .with_message(label.message.clone())
                        .with_color(color),
                )
                .with_config(Config::default().with_index_type(ariadne::IndexType::Byte));
        }

        for hint in &self.hints {
            report = report.with_help(hint.message());
        }

        report
    }
}

/// Error that occurs during validation of a program.
#[derive(Clone, Debug)]
pub struct ValidationError {
    /// The kind of error
    kind: ValidationErrorKind,
    /// Additional information
    info: ComplexError<Origin>,
}

/// Builder for [ValidationError]
#[derive(Debug, Default)]
pub struct ValidationErrorBuilder {
    /// Current stack of [ValidationError]s
    errors: Vec<ValidationError>,
}

impl ValidationErrorBuilder {
    /// Add a new error.
    pub fn report_error(
        &mut self,
        origin: Origin,
        kind: ValidationErrorKind,
    ) -> &mut ComplexError<Origin> {
        let message = kind.to_string();

        self.errors.push(ValidationError {
            kind,
            info: ComplexError::new_error(origin),
        });

        let info = &mut self.errors.last_mut().expect("error was just added").info;
        info.add_label(ComplexErrorLabelKind::Error, origin, message);

        info
    }

    /// Finish building and return a list of [ValidationError]s.
    pub fn finalize(self) -> Vec<ValidationError> {
        self.errors
    }
}

/// Error that occurs while translating the ast into the logical representation
#[derive(Clone, Debug)]
pub struct TranslationError {
    /// The type of error that occurred
    kind: TranslationErrorKind,
    /// Additional information
    info: Box<ComplexError<CharacterRange>>,
}

impl TranslationError {
    /// Create a new [TranslationError] from a given [Span].
    pub fn new(span: Span<'_>, kind: TranslationErrorKind) -> Self {
        let message = kind.to_string();

        let mut result = Self {
            kind,
            info: Box::new(ComplexError::new_error(span.range())),
        };

        result
            .info
            .add_label(ComplexErrorLabelKind::Error, span.range(), message);

        result
    }

    /// Add a new label to the error.
    pub fn add_label<Message: Display>(
        mut self,
        kind: ComplexErrorLabelKind,
        range: CharacterRange,
        message: Message,
    ) -> Self {
        self.info.add_label(kind, range, message);

        self
    }

    /// Add a new hint to the error.
    pub fn add_hint(mut self, hint: Hint) -> Self {
        self.info.add_hint(hint);

        self
    }

    /// Add a new hint to the error if `hint` is Some.
    /// Does nothing otherwise.
    pub fn add_hint_option(&mut self, hint: Option<Hint>) -> &mut Self {
        if let Some(hint) = hint {
            self.info.add_hint(hint);
        }

        self
    }
}

/// Error that may occur while translating or validating a nemo program
#[derive(Clone, Debug)]
pub enum ProgramError {
    /// Error occurred while translating
    /// the AST representation into the logical representation
    TranslationError(TranslationError),
    /// Error occurred while validating a certain program component
    ValidationError(ValidationError),
}

impl ProgramError {
    /// Return the message of the error.
    pub fn message(&self) -> String {
        match self {
            ProgramError::TranslationError(error) => error.kind.to_string(),
            ProgramError::ValidationError(error) => error.kind.to_string(),
        }
    }

    /// Return the error code of the message.
    pub fn error_code(&self) -> usize {
        match self {
            ProgramError::TranslationError(error) => error.kind.code(),
            ProgramError::ValidationError(error) => error.kind.code(),
        }
    }

    /// Return the range indicating where the error occurred
    pub fn range<Translation>(&self, translation: Translation) -> Range<usize>
    where
        Translation: Fn(&Origin) -> Range<usize>,
    {
        match self {
            ProgramError::TranslationError(error) => error.info.reference.range(),
            ProgramError::ValidationError(error) => translation(&error.info.reference),
        }
    }

    /// Return the [`CharacterRange`] where the error occurred
    pub fn character_range<Translation>(&self, translation: Translation) -> Option<CharacterRange>
    where
        Translation: Fn(&Origin) -> Option<CharacterRange>,
    {
        match self {
            ProgramError::TranslationError(error) => Some(error.info.reference),
            ProgramError::ValidationError(error) => translation(&error.info.reference),
        }
    }

    /// Return the note attached to this error, if it exists.
    pub fn note(&self) -> Option<String> {
        match self {
            ProgramError::TranslationError(error) => error.kind.note(),
            ProgramError::ValidationError(error) => error.kind.note(),
        }
        .map(|string| string.to_string())
    }

    /// Return the [`Hint`]s attached to this error.
    pub fn hints(&self) -> &Vec<Hint> {
        match self {
            ProgramError::TranslationError(error) => &error.info.hints,
            ProgramError::ValidationError(error) => &error.info.hints,
        }
    }

    /// Append the information of this error to a [ReportBuilder].
    pub fn report<'a, Translation>(
        &'a self,
        mut report: ReportBuilder<'a, (String, Range<usize>)>,
        source_label: String,
        translation: Translation,
    ) -> ReportBuilder<'a, (String, Range<usize>)>
    where
        Translation: Fn(&Origin) -> Range<usize>,
    {
        let config = Config::default().with_index_type(ariadne::IndexType::Byte);
        report = report
            .with_code(self.error_code())
            .with_message(self.message())
            .with_config(config);

        if let Some(note) = self.note() {
            report = report.with_note(note);
        }

        match self {
            ProgramError::TranslationError(error) => {
                error
                    .info
                    .report(report, source_label, |range| range.range())
            }
            ProgramError::ValidationError(error) => {
                error.info.report(report, source_label, translation)
            }
        }
    }
}
