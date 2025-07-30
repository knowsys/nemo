//! This module defines [PrimitiveAtom].

use std::fmt::Display;

use crate::{
    rule_model::components::{
        IterablePrimitives, IterableVariables,
        atom::Atom,
        tag::Tag,
        term::{
            Term,
            primitive::{Primitive, variable::Variable},
        },
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

use super::ChaseAtom;

/// An atom which may only use [PrimitiveTerm]s
#[derive(Debug, Clone)]
pub(crate) struct PrimitiveAtom {
    /// Predicate name of this atom
    predicate: Tag,
    /// Terms contained in this atom
    terms: Vec<Primitive>,
}

impl PrimitiveAtom {
    /// Construct a new [PrimitiveAtom].
    pub(crate) fn new(predicate: Tag, terms: Vec<Primitive>) -> Self {
        Self { predicate, terms }
    }

    pub fn _set_predicate(&mut self, predicate: Tag) {
        self.predicate = predicate;
    }

    pub fn _get(&self, index: usize) -> &Primitive {
        &self.terms[index]
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
    type TermType = Primitive;

    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.terms.iter())
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        Box::new(self.terms.iter_mut())
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
        let predicate = value.predicate();
        let mut terms = Vec::new();

        for term in value.terms().cloned() {
            if let Term::Primitive(primitive_term) = term {
                terms.push(primitive_term)
            } else {
                return Err(PrimitiveAtomConversionError);
            }
        }

        Ok(Self::new(predicate, terms))
    }
}
