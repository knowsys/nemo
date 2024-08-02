//! This module defines [Hint]
#![allow(missing_docs)]

pub(crate) mod similar;

use enum_assoc::Assoc;

/// Hints for error messages
#[derive(Assoc, Debug)]
#[func(pub fn message(&self) -> String)]
pub enum Hint {
    #[assoc(message = "unnamed universal variables may be expressed with an underscore `_`".to_string())]
    AnonymousVariables,
    #[assoc(message = format!("a {} with a similar name exists: `{}`", _kind, _name))]
    SimilarExists { kind: String, name: String },
}

impl std::fmt::Display for Hint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message().fmt(f)
    }
}
