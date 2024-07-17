//! This module defines [Hint]
#![allow(missing_docs)]

use enum_assoc::Assoc;

/// Hints for error messages
#[derive(Assoc, Debug)]
#[func(pub fn message(&self) -> String)]
pub enum Hint {
    #[assoc(message = "unnamed universal variables may be expressed with an underscore `_`".to_string())]
    AnonymousVariables,
    #[assoc(message = format!("similar {} exists: `{}`", _kind, _name))]
    SimilarExists { kind: String, name: String },
}
