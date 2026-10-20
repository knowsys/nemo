//! The diagnostic, representing a reported error or warning.

use std::fmt;

use crate::diagnostics::{Code, DiagnosticKind, Span, registry::INTERNAL_NOTE};

/// How severe a diagnostic is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// A problem the operation recovered from.
    Warning,
    /// A problem that fails the operation.
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Severity::Warning => "warning",
            Severity::Error => "error",
        })
    }
}

/// A message pointing at a range of a source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    /// The range the message points at.
    pub span: Span,
    /// The message shown at the range.
    pub message: String,
}

/// One named value of a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// The name of the value.
    pub name: &'static str,
    /// The value, as it is shown.
    pub value: String,
}

/// One error or warning, with everything needed to show it.
///
/// # Example
///
/// ```
/// use nemo_physical::diagnostics::{Code, Diagnostic, Severity};
///
/// let diagnostic = Diagnostic::error(Code::new(42), "row has 3 fields, expected 4")
///     .with_field("resource", "data/people.csv")
///     .with_hint("check the delimiter");
///
/// assert_eq!(diagnostic.severity(), Severity::Error);
/// assert_eq!(diagnostic.field("resource"), Some("data/people.csv"));
/// assert_eq!(
///     diagnostic.to_string(),
///     "error[N0042]: row has 3 fields, expected 4\n  \
///      resource: data/people.csv\n  \
///      hint: check the delimiter"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic(Box<Details>);

/// The content of a [Diagnostic].
#[derive(Debug, Clone, PartialEq, Eq)]
struct Details {
    /// The code naming the kind of problem.
    code: Code,
    /// How severe the problem is.
    severity: Severity,
    /// The message saying what happened, complete on its own.
    message: String,
    /// The messages pointing at ranges of source texts.
    labels: Vec<Label>,
    /// The named values of the problem, each name once.
    fields: Vec<Field>,
    /// The notes explaining the problem.
    notes: Vec<String>,
    /// The hints on what to do about the problem.
    hints: Vec<String>,
}

impl Diagnostic {
    /// Creates an error with the given code and message.
    pub fn error(code: Code, message: impl Into<String>) -> Self {
        Self::new(code, Severity::Error, message)
    }

    /// Creates a warning with the given code and message.
    pub fn warning(code: Code, message: impl Into<String>) -> Self {
        Self::new(code, Severity::Warning, message)
    }

    /// Creates the error a host reports a caught panic as:
    /// a bug in Nemo itself, not in the program or its data.
    ///
    /// It carries a note asking the user to report the bug.
    /// Library code panics instead of returning it.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::error(Code::INTERNAL, message).with_note(INTERNAL_NOTE)
    }

    /// Creates the diagnostic a kind reports:
    /// its code, severity and message, its fields,
    /// and the note and hint of the kind.
    pub fn from_kind(kind: &impl DiagnosticKind) -> Self {
        let mut diagnostic = Self::new(kind.code(), kind.severity(), kind.message());
        diagnostic.0.fields = kind.fields();

        if let Some(note) = kind.note() {
            diagnostic = diagnostic.with_note(note);
        }
        if let Some(hint) = kind.hint() {
            diagnostic = diagnostic.with_hint(hint);
        }

        diagnostic
    }

    /// Creates a diagnostic of the given severity.
    fn new(code: Code, severity: Severity, message: impl Into<String>) -> Self {
        Self(Box::new(Details {
            code,
            severity,
            message: message.into(),
            labels: Vec::new(),
            fields: Vec::new(),
            notes: Vec::new(),
            hints: Vec::new(),
        }))
    }

    /// Adds a message pointing at a range of a source text.
    pub fn with_label(mut self, span: Span, message: impl Into<String>) -> Self {
        self.0.labels.push(Label {
            span,
            message: message.into(),
        });
        self
    }

    /// Sets the value of the field with the given name,
    /// replacing the value it had.
    pub fn with_field(mut self, name: &'static str, value: impl Into<String>) -> Self {
        let value = value.into();

        match self.0.fields.iter_mut().find(|field| field.name == name) {
            Some(field) => field.value = value,
            None => self.0.fields.push(Field { name, value }),
        }

        self
    }

    /// Adds a note explaining the problem.
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.0.notes.push(note.into());
        self
    }

    /// Adds a hint on what to do about the problem.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.0.hints.push(hint.into());
        self
    }

    /// Rewrites every text the diagnostic shows: its message, its labels'
    /// messages, its field values, its notes, and its hints.
    ///
    /// A layer above uses it to show its own names for what the
    /// diagnostic's text names.
    pub fn map_text(mut self, mut rewrite: impl FnMut(&str) -> String) -> Self {
        let details = &mut *self.0;
        details.message = rewrite(&details.message);
        for label in &mut details.labels {
            label.message = rewrite(&label.message);
        }
        for field in &mut details.fields {
            field.value = rewrite(&field.value);
        }
        for text in details.notes.iter_mut().chain(&mut details.hints) {
            *text = rewrite(text);
        }
        self
    }

    /// Returns the code naming the kind of problem.
    pub fn code(&self) -> Code {
        self.0.code
    }

    /// Returns how severe the problem is.
    pub fn severity(&self) -> Severity {
        self.0.severity
    }

    /// Returns the message saying what happened.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// Returns the messages pointing at ranges of source texts.
    pub fn labels(&self) -> &[Label] {
        &self.0.labels
    }

    /// Returns the value of the field with the given name, if it has one.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.0
            .fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| field.value.as_str())
    }

    /// Returns the named values of the problem.
    pub fn fields(&self) -> &[Field] {
        &self.0.fields
    }

    /// Returns the notes explaining the problem.
    pub fn notes(&self) -> &[String] {
        &self.0.notes
    }

    /// Returns the hints on what to do about the problem.
    pub fn hints(&self) -> &[String] {
        &self.0.hints
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}]: {}",
            self.0.severity, self.0.code, self.0.message
        )?;

        for field in &self.0.fields {
            write!(f, "\n  {}: {}", field.name, field.value)?;
        }
        for note in &self.0.notes {
            write!(f, "\n  note: {note}")?;
        }
        for hint in &self.0.hints {
            write!(f, "\n  hint: {hint}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::diagnostics::{FileId, Span};

    #[test]
    fn mapping_text_reaches_every_text() {
        let span = Span {
            file: FileId::new(0),
            start: 0,
            end: 1,
        };
        let diagnostic = Diagnostic::warning(Code::new(1), "reads `a`")
            .with_label(span, "`a` here")
            .with_field("predicate", "a")
            .with_note("about `a`")
            .with_hint("rename `a`")
            .map_text(|text| text.replace('a', "b"));
        assert_eq!(diagnostic.message(), "rebds `b`");
        assert_eq!(diagnostic.labels()[0].message, "`b` here");
        assert_eq!(diagnostic.field("predicate"), Some("b"));
        assert_eq!(diagnostic.notes(), ["bbout `b`"]);
        assert_eq!(diagnostic.hints(), ["renbme `b`"]);
    }
}
