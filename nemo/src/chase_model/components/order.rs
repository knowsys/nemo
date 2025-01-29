//! This module defines [ChaseOrder].

use crate::rule_model::{
    components::{
        term::primitive::{variable::Variable, Primitive},
        IterablePrimitives, IterableVariables,
    },
    origin::Origin,
};

use super::{filter::ChaseFilter, term::operation_term::OperationTerm, ChaseComponent};

///
#[derive(Debug, Clone)]
pub(crate) struct ChaseOrder {
    /// Origin of this component
    origin: Origin,

    /// Variables of the domating atom
    _variables_dominating: Vec<Variable>,
    /// Variables of the dominated atom
    _variables_dominated: Vec<Variable>,

    /// Operation that defines the order
    filter: ChaseFilter,
}

impl ChaseOrder {
    /// Create a new [ChaseOrder].
    pub(crate) fn new(
        _variables_dominating: Vec<Variable>,
        _variables_dominated: Vec<Variable>,
        filter: ChaseFilter,
    ) -> Self {
        Self {
            origin: Origin::default(),
            _variables_dominating,
            _variables_dominated,
            filter,
        }
    }

    /// Return the operation that defines the order
    pub(crate) fn filter(&self) -> &OperationTerm {
        &self.filter.filter()
    }

    /// Return the list of variables of the dominating atom
    pub(crate) fn _variables_dominating(&self) -> &[Variable] {
        &self._variables_dominating
    }

    /// Return the list of variables of the dominating atom
    pub(crate) fn _variables_dominated(&self) -> &[Variable] {
        &self._variables_dominated
    }
}

impl ChaseComponent for ChaseOrder {
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

impl IterableVariables for ChaseOrder {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        self.filter.variables()
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        self.filter.variables_mut()
    }
}

impl IterablePrimitives for ChaseOrder {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        self.filter.primitive_terms()
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        self.filter.primitive_terms_mut()
    }
}
