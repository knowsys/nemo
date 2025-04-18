//! This module defines [ChaseFilter].

use crate::rule_model::{
    components::{
        term::primitive::{variable::Variable, Primitive},
        IterablePrimitives, IterableVariables,
    },
    origin::Origin,
};

use super::{term::operation_term::OperationTerm, ChaseComponent};

/// Indicates that a new value must be created according to [OperationTerm].
///
/// The result will be "stored" in the given variable.
#[derive(Debug, Clone)]
pub(crate) struct ChaseFilter {
    /// Origin of this component
    origin: Origin,

    /// Operation the will be evaluated
    filter: OperationTerm,
}

impl ChaseFilter {
    /// Create a new [ChaseFilter].
    pub(crate) fn new(filter: OperationTerm) -> Self {
        Self {
            origin: Origin::default(),
            filter,
        }
    }

    /// Return the filter that is being applied.
    pub(crate) fn filter(&self) -> &OperationTerm {
        &self.filter
    }
}

impl ChaseComponent for ChaseFilter {
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
