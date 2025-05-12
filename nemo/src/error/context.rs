//! This module defines [ContextError].

use std::{fmt::Display, ops::Range};

use ariadne::{Report, ReportBuilder, ReportKind};

use super::rich::RichError;

/// Severity of an error message
#[derive(Debug, Clone, Copy)]
pub(crate) enum DiagnosticSeverity {
    /// Information
    Information,
    /// Warning
    Warning,
    /// Error
    Error,
}

impl DiagnosticSeverity {
    /// Return the associated [ariadne::Color] of this severity.
    pub fn color(&self) -> ariadne::Color {
        match self {
            DiagnosticSeverity::Information => ariadne::Color::BrightBlue,
            DiagnosticSeverity::Warning => ariadne::Color::Yellow,
            DiagnosticSeverity::Error => ariadne::Color::Red,
        }
    }

    /// Return the associated [ReportKind] of this severity.
    pub fn ariande_kind<'a>(&self) -> ReportKind<'a> {
        match self {
            DiagnosticSeverity::Information => ReportKind::Advice,
            DiagnosticSeverity::Warning => ReportKind::Warning,
            DiagnosticSeverity::Error => ReportKind::Error,
        }
    }
}

/// Message relating to some part of the input program
#[derive(Debug)]
pub(crate) struct DiagnosticLabel {
    /// Type of label
    severity: DiagnosticSeverity,
    /// Range of characters this error applies to
    range: Range<usize>,
    /// Message
    message: String,
}

impl DiagnosticLabel {
    /// Create a new [DiagnosticLabel].
    pub fn new(message: String, range: Range<usize>, severity: DiagnosticSeverity) -> Self {
        Self {
            range,
            message,
            severity,
        }
    }

    /// Return the message of this label.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return the character range this label is associated with.
    pub fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// Return the [DiagnosticSeverity] associated with this label.
    pub fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    /// Add this information to a [ReportBuilder].
    pub fn report<'a>(
        &self,
        report: ReportBuilder<'a, (String, Range<usize>)>,
        source_label: &str,
    ) -> ReportBuilder<'a, (String, Range<usize>)> {
        report
            .with_label(
                ariadne::Label::new((source_label.to_owned(), self.range()))
                    .with_message(self.message())
                    .with_color(self.severity().color()),
            )
            .with_config(ariadne::Config::default().with_index_type(ariadne::IndexType::Byte))
    }
}

/// Error that may contain addition context
#[derive(Debug)]
pub struct ContextError {
    /// Code identifying the error
    code: usize,
    /// Main error that is being reported
    diagnostic: DiagnosticLabel,
    /// Additional context information
    context: Vec<DiagnosticLabel>,
    /// Hints to the user
    hints: Vec<String>,
    /// Note about the error
    note: Option<String>,
}

impl ContextError {
    /// Create a new [ContextError] associated with a given string range.
    pub fn new<Error: RichError>(error: Error, range: Range<usize>) -> Self {
        let severity = if error.is_warning() {
            DiagnosticSeverity::Warning
        } else {
            DiagnosticSeverity::Error
        };

        let diagnostic = DiagnosticLabel::new(error.message(), range, severity);

        Self {
            code: error.code(),
            diagnostic,
            context: Vec::default(),
            hints: Vec::default(),
            note: error.note(),
        }
    }

    /// Add a new context label to the error.
    pub fn add_context<Message: Display>(
        &mut self,
        message: Message,
        range: Range<usize>,
    ) -> &mut Self {
        self.context.push(DiagnosticLabel::new(
            message.to_string(),
            range,
            DiagnosticSeverity::Information,
        ));
        self
    }

    /// Add a new hint to the error.
    pub fn add_hint(&mut self, hint: String) -> &mut Self {
        self.hints.push(hint);
        self
    }

    /// Add a new hint to the error if `hint` is Some.
    /// Does nothing otherwise.
    pub fn add_hint_option(&mut self, hint: Option<String>) -> &mut Self {
        if let Some(hint) = hint {
            self.add_hint(hint);
        }

        self
    }

    /// Add this information to a [ReportBuilder].
    pub fn report<'a>(&self, source_label: &str) -> Report<'a, (String, Range<usize>)> {
        let mut report = Report::build(
            self.diagnostic.severity.ariande_kind(),
            source_label.to_owned(),
            12,
        );

        report = self
            .diagnostic
            .report(report, source_label)
            .with_code(self.code)
            .with_message(self.diagnostic.message());

        if let Some(note) = &self.note {
            report = report.with_note(note);
        }

        for context in &self.context {
            report = context.report(report, source_label);
        }

        for hint in &self.hints {
            report = report.with_help(hint.message());
        }

        report.finish()
    }
}
