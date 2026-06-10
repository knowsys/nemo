//! This module defines [Info].
#![allow(missing_docs)]

use enum_assoc::Assoc;

/// Infos for error messages
#[derive(Assoc, Debug, Copy, Clone)]
#[func(pub fn message(&self) -> String)]
pub enum Info {
    /// Value was defined externally
    #[assoc(message = "value was defined externally".to_string())]
    DefinedExternally,
    /// First definition occurred somewhere
    #[assoc(message = "first definition occurred here".to_string())]
    FirstDefinition,
    /// First use occurred somewhere
    #[assoc(message = "first use occurred here".to_string())]
    FirstUse,
    /// Predicate different arity
    #[assoc(message = format!("predicate was used here with arity {}", _arity))]
    PredicateArity { arity: usize },
    /// Value was defined here
    #[assoc(message = "value was defined here".to_string())]
    ValueDefined,
}

impl std::fmt::Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message().fmt(f)
    }
}
