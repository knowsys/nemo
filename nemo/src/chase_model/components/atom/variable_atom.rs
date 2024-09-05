//! This module defines [VariableAtom].

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{tag::Tag, term::primitive::variable::Variable, IterableVariables},
        origin::Origin,
    },
};

use super::ChaseAtom;

/// An atom which may only use [Variable]s.
#[derive(Debug, Clone)]
pub(crate) struct VariableAtom {
    /// Origin of this component
    origin: Origin,

    /// Predicate name of this atom
    predicate: Tag,
    /// Variables contained in this atom
    variables: Vec<Variable>,
}

impl VariableAtom {
    /// Construct a new [VariableAtom].
    pub(crate) fn new(predicate: Tag, variables: Vec<Variable>) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            variables,
        }
    }
}

impl ChaseAtom for VariableAtom {
    type TypeTerm = Variable;

    fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    fn terms(&self) -> impl Iterator<Item = &Self::TypeTerm> {
        self.variables.iter()
    }

    fn terms_mut(&mut self) -> impl Iterator<Item = &mut Self::TypeTerm> {
        self.variables.iter_mut()
    }
}

impl IterableVariables for VariableAtom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms())
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms_mut())
    }
}

impl ChaseComponent for VariableAtom {
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
