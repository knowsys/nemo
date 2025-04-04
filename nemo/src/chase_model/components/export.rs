//! This module defines [ChaseExport].

use crate::{
    io::formats::Export,
    rule_model::{components::tag::Tag, origin::Origin},
};

use super::ChaseComponent;

/// Component for handling exports
#[derive(Debug, Clone)]
pub(crate) struct ChaseExport {
    /// Origin of this component
    origin: Origin,

    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for exporting data
    handler: Export,
}

impl ChaseExport {
    /// Create a new [ChaseExport].
    pub(crate) fn new(predicate: Tag, handler: Export) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            handler,
        }
    }

    /// Return the predicate.
    pub(crate) fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return the arity of this import.
    pub(crate) fn arity(&self) -> usize {
        self.handler.predicate_arity()
    }

    /// Convert self into pair of predicate `[Tag]` and `[Export]` handler.
    pub(crate) fn into_predicate_and_handler(self) -> (Tag, Export) {
        (self.predicate, self.handler)
    }
}

impl ChaseComponent for ChaseExport {
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
}
