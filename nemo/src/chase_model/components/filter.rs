//! This module defines [ChaseFilter].

use std::fmt::Display;

use crate::rule_model::components::{
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives, IterableVariables,
};

use super::term::operation_term::OperationTerm;

/// Indicates that a new value must be created according to [OperationTerm].
///
/// The result will be "stored" in the given variable.
#[derive(Debug, Clone)]
pub(crate) struct ChaseFilter {
    /// Operation the will be evaluated
    filter: OperationTerm,
}

impl Display for ChaseFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.filter {
            OperationTerm::Primitive(primitive) => f.write_str(&format!("{primitive}")),
            OperationTerm::Operation(operation) => f.write_str(&format!("{operation}")),
        }
    }
}

impl ChaseFilter {
    /// Create a new [ChaseFilter].
    pub(crate) fn new(filter: OperationTerm) -> Self {
        Self { filter }
    }

    /// Return the filter that is being applied.
    pub(crate) fn filter(&self) -> &OperationTerm {
        &self.filter
    }
}

impl IterableVariables for ChaseFilter {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        self.filter.variables()
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        self.filter.variables_mut()
    }
}

impl IterablePrimitives for ChaseFilter {
    type TermType = Primitive;

    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        self.filter.primitive_terms()
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        self.filter.primitive_terms_mut()
    }
}
