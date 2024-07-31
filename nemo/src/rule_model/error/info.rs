//! This module defines [Info].
#![allow(missing_docs)]

use enum_assoc::Assoc;

/// Infos for error messages
#[derive(Assoc, Debug, Copy, Clone)]
#[func(pub fn message(&self) -> String)]
pub enum Info {
    /// First definition occurred somewhere
    #[assoc(message = format!("first definition occurred here"))]
    FirstDefinition,
}

impl std::fmt::Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message().fmt(f)
    }
}
