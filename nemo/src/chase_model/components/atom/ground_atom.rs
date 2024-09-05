//! This module defines [GroundAtom].

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{
            tag::Tag,
            term::primitive::{ground::GroundTerm, variable::Variable},
            IterableVariables,
        },
        origin::Origin,
    },
};

use super::ChaseAtom;

/// An atom which may only use [GroundTerm]s
#[derive(Debug, Clone)]
pub(crate) struct GroundAtom {
    /// Origin of this component
    origin: Origin,

    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<GroundTerm>,
}

impl GroundAtom {
    /// Construct a new [GroundAtom].
    pub(crate) fn new(predicate: Tag, terms: Vec<GroundTerm>) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            terms,
        }
    }

    /// Returns all [AnyDataValue]s used as constants in this atom
    pub(crate) fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> + '_ {
        self.terms().map(|term| term.value())
    }
}

impl ChaseAtom for GroundAtom {
    type TypeTerm = GroundTerm;

    fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    fn terms(&self) -> impl Iterator<Item = &Self::TypeTerm> {
        self.terms.iter()
    }

    fn terms_mut(&mut self) -> impl Iterator<Item = &mut Self::TypeTerm> {
        self.terms.iter_mut()
    }
}

impl IterableVariables for GroundAtom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(std::iter::empty())
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(std::iter::empty())
    }
}

impl ChaseComponent for GroundAtom {
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
