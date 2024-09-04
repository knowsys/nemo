//! This module defines [ChaseExport].

use crate::{
    io::formats::ImportExportHandler,
    rule_model::{components::tag::Tag, origin::Origin},
};

use super::ChaseComponent;

/// Component for handling exports
#[derive(Debug)]
pub(crate) struct ChaseExport {
    /// Origin of this component
    origin: Origin,

    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for exporting data
    handler: Box<dyn ImportExportHandler>,
}

impl ChaseExport {
    /// Create a new [ChaseExport].
    pub(crate) fn new(
        origin: Origin,
        predicate: Tag,
        handler: Box<dyn ImportExportHandler>,
    ) -> Self {
        Self {
            origin,
            predicate,
            handler,
        }
    }

    /// Return the predicate.
    pub(crate) fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return the handler.
    pub(crate) fn handler(&self) -> &Box<dyn ImportExportHandler> {
        &self.handler
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
