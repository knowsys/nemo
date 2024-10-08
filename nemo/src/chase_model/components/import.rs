//! This module defines [ChaseImport].

use crate::{
    io::formats::ImportExportHandler,
    rule_model::{components::tag::Tag, origin::Origin},
};

use super::ChaseComponent;

/// Component for handling imports
#[derive(Debug, Clone)]
pub(crate) struct ChaseImport {
    /// Origin of this component
    origin: Origin,

    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for importing data
    handler: Box<dyn ImportExportHandler>,
}

impl ChaseImport {
    /// Create a new [ChaseImport].
    pub(crate) fn new(predicate: Tag, handler: Box<dyn ImportExportHandler>) -> Self {
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

    /// Return the handler.
    pub(crate) fn handler(&self) -> Box<dyn ImportExportHandler> {
        self.handler.clone()
    }

    /// Return the arity of this import.
    pub(crate) fn arity(&self) -> usize {
        self.handler.predicate_arity()
    }
}

impl ChaseComponent for ChaseImport {
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
