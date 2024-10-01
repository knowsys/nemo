//! This module defines [PrimitiveAtom].

use std::fmt::Display;

use crate::{
    chase_model::components::ChaseComponent,
    rule_model::{
        components::{
            atom::Atom,
            tag::Tag,
            term::{
                primitive::{variable::Variable, Primitive},
                Term,
            },
            IterablePrimitives, IterableVariables, ProgramComponent,
        },
        origin::Origin,
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
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
    pub(crate) fn new(predicate: Tag, terms: Vec<Primitive>) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            terms,
        }
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

impl Display for PrimitiveAtom {
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

impl IterablePrimitives for PrimitiveAtom {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.terms.iter())
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        Box::new(self.terms.iter_mut())
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

/// Error struct for converting logical atoms to [PrimitiveAtom]s
#[derive(Debug)]
pub(crate) struct PrimitiveAtomConversionError;

impl Display for PrimitiveAtomConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("atom contains non-primitive terms")
    }
}

impl TryFrom<Atom> for PrimitiveAtom {
    type Error = PrimitiveAtomConversionError;

    fn try_from(value: Atom) -> Result<Self, Self::Error> {
        let origin = *value.origin();
        let predicate = value.predicate();
        let mut terms = Vec::new();

        for term in value.arguments().cloned() {
            if let Term::Primitive(primitive_term) = term {
                terms.push(primitive_term)
            } else {
                return Err(PrimitiveAtomConversionError);
            }
        }

        Ok(Self::new(predicate, terms).set_origin(origin))
    }
}
