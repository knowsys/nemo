//! This module defines [TranslationErrorKind]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use thiserror::Error;

/// Types of errors that occur
/// while translating the ASP representation of a nemo program
/// into its logical representation.
#[derive(Assoc, Error, Debug, Clone)]
#[func(pub fn note(&self) -> Option<&'static str>)]
#[func(pub fn code(&self) -> usize)]
pub enum TranslationErrorKind {
    /// A negated atom was used in the head of a rule
    #[error(r#"{0} used in rule head"#)]
    #[assoc(note = "rule head must only use atoms")]
    #[assoc(code = 101)]
    HeadNonAtom(String),
    /// An undefined prefix was used
    #[error(r#"unknown prefix: `{0}`"#)]
    #[assoc(note = "prefix must be defined using @prefix")]
    #[assoc(code = 102)]
    UnknownPrefix(String),
    /// Unnamed non-anonymous variable
    #[error(r#"unnamed variable"#)]
    #[assoc(note = "variables starting with ? or ! must have a name")]
    #[assoc(code = 103)]
    UnnamedVariable,
    /// Named non-anonymous variable
    #[error(r#"anonymous variable with name: ``"#)]
    #[assoc(note = "anonymous variables cannot have a name")]
    #[assoc(code = 104)]
    NamedAnonymous(String),
}
