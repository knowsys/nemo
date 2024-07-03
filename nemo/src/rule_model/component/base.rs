//! This module defines [Base]

use std::fmt::Display;

use crate::rule_model::origin::Origin;

/// TODO
#[derive(Debug, Clone)]
pub struct Base {
    /// Origin of this component
    origin: Origin,

    base: String,
}

impl Base {
    /// Create a new [Base]
    pub fn new(base: String) -> Self {
        Self {
            origin: Origin::default(),
            base,
        }
    }
}

impl Display for Base {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
