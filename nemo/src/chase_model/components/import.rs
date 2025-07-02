//! This module defines [ChaseImport].

use crate::{io::formats::Import, rule_model::components::tag::Tag};

/// Component for handling imports
#[derive(Debug, Clone)]
pub(crate) struct ChaseImport {
    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for importing data
    handler: Import,
}

impl ChaseImport {
    /// Create a new [ChaseImport].
    pub(crate) fn new(predicate: Tag, handler: Import) -> Self {
        Self { predicate, handler }
    }

    /// Return the predicate.
    pub(crate) fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return the handler.
    pub(crate) fn handler(&self) -> &Import {
        &self.handler
    }

    /// Return the arity of this import.
    pub(crate) fn arity(&self) -> usize {
        self.handler.predicate_arity()
    }
}
