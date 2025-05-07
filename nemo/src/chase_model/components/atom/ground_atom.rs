//! This module defines [GroundAtom].

use std::fmt::Display;

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{
            atom::Atom,
            tag::Tag,
            term::{
                primitive::{ground::GroundTerm, variable::Variable, Primitive},
                Term,
            },
            ComponentIdentity, IterableVariables,
        },
        origin::Origin,
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

use super::ChaseAtom;

/// An atom which may only use [GroundTerm]s
#[derive(Debug, Clone)]
pub struct GroundAtom {
    /// Origin of this component
    origin: Origin,

    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<GroundTerm>,
}

impl GroundAtom {
    /// Construct a new [GroundAtom].
    pub fn new(predicate: Tag, terms: Vec<GroundTerm>) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            terms,
        }
    }

    /// Returns all [AnyDataValue]s used as constants in this atom
    pub fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> + '_ {
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

impl Display for GroundAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let terms = DisplaySeperatedList::display(
            self.terms(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );
        let predicate = self.predicate();

        f.write_str(&format!(
            "{predicate}{}{terms}{}",
            syntax::expression::atom::OPEN,
            syntax::expression::atom::CLOSE
        ))
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

/// Error struct for converting logical atoms to [GroundAtom]s
#[derive(Debug, Clone, Copy)]
pub struct GroundAtomConversionError;

impl Display for GroundAtomConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("atom contains non-ground terms")
    }
}

impl TryFrom<Atom> for GroundAtom {
    type Error = GroundAtomConversionError;

    fn try_from(value: Atom) -> Result<Self, Self::Error> {
        let origin = value.origin().clone();
        let predicate = value.predicate();
        let mut terms = Vec::new();

        for term in value.terms().cloned() {
            if let Term::Primitive(Primitive::Ground(ground_term)) = term {
                terms.push(ground_term)
            } else {
                return Err(GroundAtomConversionError);
            }
        }

        Ok(Self::new(predicate, terms).set_origin(origin))
    }
}
