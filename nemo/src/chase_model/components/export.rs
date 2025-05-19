//! This module defines [ChaseExport].

use crate::{io::formats::Export, rule_model::components::tag::Tag};

/// Component for handling exports
#[derive(Debug, Clone)]
pub(crate) struct ChaseExport {
    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for exporting data
    handler: Export,
}

impl ChaseExport {
    /// Create a new [ChaseExport].
    pub(crate) fn new(predicate: Tag, handler: Export) -> Self {
        Self { predicate, handler }
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
