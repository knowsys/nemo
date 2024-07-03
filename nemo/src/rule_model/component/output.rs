//! This module defines [Output]

use crate::rule_model::origin::Origin;

use super::term::Identifier;

/// TODO
#[derive(Debug, Clone)]
pub struct Output {
    /// Origin of this component
    origin: Origin,

    ///
    predicate: Identifier,
}

impl Output {
    /// Create a mew [Output]
    pub fn new(predicate: Identifier) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
        }
    }
}
