//! Codes identifying the kinds of problem a diagnostic reports.

use std::fmt;

/// A code identifying one kind of problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Code(u16);

impl Code {
    /// The code of an [internal error](crate::diagnostics::Diagnostic::internal),
    /// which hosts report caught panics as.
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
