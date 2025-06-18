//! This module defines [RichError].

/// Trait implemented by error types
/// that associate each error with an error code an optional note
pub trait RichError {
    /// Return whether this error should be treated as a warning.
    fn is_warning(&self) -> bool;
    /// Return the error message.
    fn message(&self) -> String;
    /// Return the error code.
    fn code(&self) -> usize;
    /// Return an optional note relating to the error.
    fn note(&self) -> Option<String>;
}
