//! This module defines [Output]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    error::ValidationErrorBuilder, origin::Origin, pipeline::id::ProgramComponentId,
};

use super::{
    tag::Tag, ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
    ProgramComponentKind,
};

/// Output directive
///
/// Marks a predicate as an output predicate.
#[derive(Debug, Clone)]
pub struct Output {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Output predicate
    predicate: Tag,
}

impl Output {
    /// Create a mew [Output].
    pub fn new(predicate: Tag) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
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

impl Eq for Output {}

impl Hash for Output {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
    }
}

impl ComponentBehavior for Output {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Output
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for Output {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl IterableComponent for Output {}
