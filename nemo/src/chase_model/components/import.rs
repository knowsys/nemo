//! This module defines [ChaseImport] and [ChaseImportClause].

use crate::{
    io::formats::Import,
    rule_model::components::{IterableVariables, tag::Tag, term::primitive::variable::Variable},
};

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

/// Represents an import that is executed as part of a rule evaluation
#[derive(Debug, Clone)]
pub(crate) struct ChaseImportClause {
    /// Import
    import: ChaseImport,

    /// Bindings
    bindings: Vec<Variable>,
}

impl ChaseImportClause {
    /// Create a new [ChaseImportClause].
    pub(crate) fn new(predicate: Tag, handler: Import, bindings: Vec<Variable>) -> Self {
        Self {
            import: ChaseImport::new(predicate, handler),
            bindings,
        }
    }

    /// Return the predicate.
    pub(crate) fn predicate(&self) -> &Tag {
        &self.import.predicate
    }

    /// Return the handler.
    pub(crate) fn handler(&self) -> &Import {
        &self.import.handler
    }

    /// Return a reference to the bindings.
    pub(crate) fn bindings(&self) -> &Vec<Variable> {
        &self.bindings
    }
}

impl IterableVariables for ChaseImportClause {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.bindings.iter())
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.bindings.iter_mut())
    }
}
