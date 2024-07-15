//! This module defines [TranslationErrorKind]

use thiserror::Error;

/// Types of errors that occur
/// while translating the ASP representation of a nemo program
/// into its logical representation.
#[derive(Error, Debug, Copy, Clone)]
pub enum TranslationErrorKind {}
