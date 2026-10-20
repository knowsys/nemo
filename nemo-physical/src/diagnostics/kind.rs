//! The kinds of problems that may be reported.

use crate::diagnostics::{Code, CodeEntry, Field, Severity};

/// One kind of problem: its code, severity, message, fields, note and hint.
///
/// Kinds are declared as enums with [diagnostic_kinds](crate::diagnostic_kinds),
/// one variant per kind.
pub trait DiagnosticKind {
    /// All available kinds, in declaration order.
    const ENTRIES: &'static [CodeEntry];

    /// Returns the code naming the kind.
    fn code(&self) -> Code;

    /// Returns whether the kind is an error or a warning.
    fn severity(&self) -> Severity;

    /// Returns the message, with the data of the kind filled in.
    fn message(&self) -> String;

    /// Returns the data of the kind, each field under its declared name.
    fn fields(&self) -> Vec<Field>;

    /// Returns the note every diagnostic of the kind carries, if any.
    fn note(&self) -> Option<&'static str>;

    /// Returns the hint every diagnostic of the kind carries, if any.
    fn hint(&self) -> Option<&'static str>;
}
