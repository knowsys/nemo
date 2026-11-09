//! The kinds of problem that diagnostics report.

use crate::diagnostics::{Code, CodeEntry, Field, Severity};

/// One kind of problem, with its code, severity, message, fields, note and hint.
///
/// Kinds are declared with [diagnostic_kinds](crate::diagnostic_kinds),
/// as an enum with one variant per kind.
pub trait DiagnosticKind {
    /// The documentation of all kinds of the enum, in declaration order.
    const ENTRIES: &'static [CodeEntry];

    /// Returns the code of the kind.
    fn code(&self) -> Code;

    /// Returns whether the kind is an error or a warning.
    fn severity(&self) -> Severity;

    /// Returns the message, with the data of the kind filled in.
    fn message(&self) -> String;

    /// Returns the data of the kind, each field under its declared name.
    fn fields(&self) -> Vec<Field>;

    /// Returns the note that every diagnostic of the kind has, if there is one.
    fn note(&self) -> Option<&'static str>;

    /// Returns the hint that every diagnostic of the kind has, if there is one.
    fn hint(&self) -> Option<&'static str>;
}
