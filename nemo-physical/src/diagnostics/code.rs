//! Codes that identify the kinds of problem that diagnostics report.

use std::fmt;

/// A code that identifies one kind of problem.
///
/// It is written as `N` followed by four digits, such as `N0042`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Code(u16);

impl Code {
    /// The code of an [internal error](crate::diagnostics::Diagnostic::internal),
    /// a bug in Nemo itself.
    pub const INTERNAL: Self = Self(1);

    /// Creates the code with the given number.
    pub const fn new(number: u16) -> Self {
        Self(number)
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{:04}", self.0)
    }
}
