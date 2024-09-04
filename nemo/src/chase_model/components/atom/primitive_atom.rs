//! This module defines [PrimitiveAtom].

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{
            tag::Tag,
            term::primitive::{variable::Variable, Primitive},
            IterableVariables,
        },
        origin::Origin,
    },
};

use super::ChaseAtom;

/// An atom which may only use [PrimitiveTerm]s
#[derive(Debug, Clone)]
pub(crate) struct PrimitiveAtom {
    /// Origin of this component
    origin: Origin,

    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<Primitive>,
}

impl PrimitiveAtom {
    /// Construct a new [PrimitiveAtom].
    pub(crate) fn new(origin: Origin, predicate: Tag, terms: Vec<Primitive>) -> Self {
        Self {
            origin,
            predicate,
            terms,
        }
    }

    /// Returns all [AnyDataValue]s used as constants in this atom.
    pub(crate) fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> + '_ {
        self.terms.iter().filter_map(|term| match term {
            Primitive::Ground(ground) => Some(ground.value().clone()),
            Primitive::Variable(_) => None,
        })
    }
}

impl ChaseAtom for PrimitiveAtom {
    type TypeTerm = Primitive;

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

impl IterableVariables for PrimitiveAtom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms().filter_map(|term| match term {
            Primitive::Variable(variable) => Some(variable),
            Primitive::Ground(_) => None,
        }))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms_mut().filter_map(|term| match term {
            Primitive::Variable(variable) => Some(variable),
            Primitive::Ground(_) => None,
        }))
    }
}

impl ChaseComponent for PrimitiveAtom {
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
