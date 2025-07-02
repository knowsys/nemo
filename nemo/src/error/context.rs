//! This module defines [ContextError].

use std::{fmt::Display, ops::Range};

use ariadne::{Config, Report, ReportBuilder, ReportKind};

use super::rich::RichError;

/// Severity of an error message
#[derive(Debug, Clone, Copy)]
pub enum DiagnosticSeverity {
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
pub struct DiagnosticLabel {
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
        report.with_label(
            ariadne::Label::new((source_label.to_owned(), self.range()))
                .with_message(self.message())
                .with_color(self.severity().color()),
        )
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

impl Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.diagnostic.message.fmt(f)
    }
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

    /// Return the code of this error.
    pub fn code(&self) -> usize {
        self.code
    }

    /// Return the main diagnostic of this error.
    pub fn diagnostic(&self) -> &DiagnosticLabel {
        &self.diagnostic
    }

    /// Return an iterator over additional context labels.
    pub fn context(&self) -> impl Iterator<Item = &DiagnosticLabel> {
        self.context.iter()
    }

    /// Return an iterator over user hints.
    pub fn hints(&self) -> impl Iterator<Item = &String> {
        self.hints.iter()
    }

    /// Return a note for the error,
    /// if there is one.
    pub fn note(&self) -> Option<&String> {
        self.note.as_ref()
    }

    /// Set the range of the error.
    pub fn set_range(&mut self, range: Range<usize>) -> &mut Self {
        self.diagnostic.range = range;
        self
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
    pub fn add_hint<Message: Display>(&mut self, hint: Message) -> &mut Self {
        self.hints.push(hint.to_string());
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

    /// Add a list of hints.
    pub fn add_hints(mut self, hints: Vec<String>) -> Self {
        self.hints.extend(hints);
        self
    }

    /// Add this information to a [ReportBuilder].
    pub fn report<'a>(&self, source_label: &str) -> Report<'a, (String, Range<usize>)> {
        let mut report = Report::build(
            self.diagnostic.severity.ariande_kind(),
            (source_label.to_string(), self.diagnostic.range.clone()),
        )
        .with_config(Config::default().with_index_type(ariadne::IndexType::Byte));

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
            report = report.with_help(hint);
        }

        report.finish()
    }
}
