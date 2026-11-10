//! Diagnostics, the reported errors and warnings.

use std::fmt;

use crate::diagnostics::{Code, DiagnosticKind, Location, Span, registry::INTERNAL_NOTE};

/// How severe a diagnostic is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// A problem that the operation recovered from.
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

/// A message that points at a range of a source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    /// The range that the message points at.
    pub span: Span,
    /// The message shown at the range.
    pub message: String,
    /// The name of the source text, and the line and column at which the range starts.
    ///
    /// A label is created without a location,
    /// and [Diagnostic::with_locations] sets it from the source text.
    pub location: Option<Location>,
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
    /// The message that says what happened, complete on its own.
    message: String,
    /// The messages that point at ranges of source texts.
    labels: Vec<Label>,
    /// The named values of the problem, with each name at most once.
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

    /// Creates an internal error, which reports a bug in Nemo itself
    /// rather than a problem of its input.
    ///
    /// It is the error for a caught panic,
    /// and it has a note that asks the user to report the bug.
    /// Library code panics instead of returning it.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::error(Code::INTERNAL, message).with_note(INTERNAL_NOTE)
    }

    /// Creates the diagnostic of a kind,
    /// with the kind's code, severity, message, fields, note and hint.
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

    /// Adds a message that points at a range of a source text.
    pub fn with_label(mut self, span: Span, message: impl Into<String>) -> Self {
        self.0.labels.push(Label {
            span,
            message: message.into(),
            location: None,
        });
        self
    }

    /// Sets the location of every label for which `locate` returns one,
    /// and keeps the locations of the other labels.
    ///
    /// `locate` is given the range of a label,
    /// and returns the location of its start if the source text of the range is known.
    pub fn with_locations(mut self, mut locate: impl FnMut(Span) -> Option<Location>) -> Self {
        for label in &mut self.0.labels {
            if let Some(location) = locate(label.span) {
                label.location = Some(location);
            }
        }
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

    /// Removes the hints, so that other hints can replace them.
    pub fn without_hints(mut self) -> Self {
        self.0.hints.clear();
        self
    }

    /// Rewrites every text that the diagnostic shows:
    /// its message, the messages of its labels, the values of its fields, its notes and its hints.
    ///
    /// `rewrite` returns the new form of a text,
    /// for example with other names for the things that it names.
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

    /// Returns the code that names the kind of problem.
    pub fn code(&self) -> Code {
        self.0.code
    }

    /// Returns how severe the problem is.
    pub fn severity(&self) -> Severity {
        self.0.severity
    }

    /// Returns the message that says what happened.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// Returns the messages that point at ranges of source texts.
    ///
    /// The first label marks the problem, and the others show its context.
    pub fn labels(&self) -> &[Label] {
        &self.0.labels
    }

    /// Returns the location of the problem, which is the location of the first label,
    /// or `None` if the diagnostic has no label or its first label has no location.
    pub fn location(&self) -> Option<&Location> {
        self.0.labels.first()?.location.as_ref()
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

    /// Mapping the text rewrites the message, the labels, the fields, the notes and the hints.
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

    /// Setting the locations keeps the location of a label whose source text is not known,
    /// and the location of the first label is the location of the diagnostic.
    #[test]
    fn locations_are_set_where_the_source_text_is_known() {
        let span = |file| Span {
            file: FileId::new(file),
            start: 0,
            end: 1,
        };
        let at = |line| Location {
            source: None,
            line,
            column: 1,
        };
        let diagnostic = Diagnostic::error(Code::new(1), "refused")
            .with_label(span(0), "here")
            .with_label(span(1), "declared here");
        assert_eq!(diagnostic.location(), None);

        // The first text is known first, and then only the second.
        let diagnostic = diagnostic
            .with_locations(|span| (span.file == FileId::new(0)).then(|| at(3)))
            .with_locations(|span| (span.file == FileId::new(1)).then(|| at(7)));
        assert_eq!(diagnostic.location(), Some(&at(3)));
        assert_eq!(diagnostic.labels()[1].location, Some(at(7)));
    }

    /// Removing the hints lets new hints replace them.
    #[test]
    fn a_layer_above_replaces_the_hints() {
        let diagnostic = Diagnostic::error(Code::new(1), "refused")
            .with_hint("call `resume()`")
            .with_hint("call `discard_pending()`")
            .without_hints()
            .with_hint("call `discardPending`");
        assert_eq!(diagnostic.hints(), ["call `discardPending`"]);
    }
}
