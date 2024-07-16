//! This module defines [Base]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{error::ValidationErrorBuilder, origin::Origin};

use super::ProgramComponent;

/// Global prefix
#[derive(Debug, Clone, Eq)]
pub struct Base {
    /// Origin of this component
    origin: Origin,

    /// Prefix
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@base {} .", self.base)
    }
}

impl PartialEq for Base {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base
    }
}

impl Hash for Base {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.base.hash(state);
    }
}

impl ProgramComponent for Base {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}
