//! This module defines [Output]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{error::ValidationErrorBuilder, origin::Origin};

use super::{tag::Tag, ProgramComponent, ProgramComponentKind};

/// Output directive
///
/// Marks a predicate as an output predicate.
#[derive(Debug, Clone, Eq)]
pub struct Output {
    /// Origin of this component
    origin: Origin,

    /// Output predicate
    predicate: Tag,
}

impl Output {
    /// Create a mew [Output].
    pub fn new(predicate: Tag) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
        }
    }

    /// Return the output predicate.
    pub fn predicate(&self) -> &Tag {
        &self.predicate
    }
}

impl Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@output {} .", self.predicate)
    }
}

impl PartialEq for Output {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
    }
}

impl Hash for Output {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
    }
}

impl ProgramComponent for Output {
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

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Output
    }
}
