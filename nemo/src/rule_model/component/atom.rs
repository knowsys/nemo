//! This module defines an [Atom].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{error::ProgramConstructionError, origin::Origin};

use super::{
    term::{Identifier, Term},
    ProgramComponent,
};

/// An atom
#[derive(Debug, Clone, Eq)]
pub struct Atom {
    /// Origin of this component.
    origin: Origin,

    /// Predicate name associated with this atom
    name: Identifier,
    /// Subterms of the function
    terms: Vec<Term>,
}

impl Atom {
    /// Create a new [Atom].
    pub fn new(name: &str, subterms: Vec<Term>) -> Self {
        Self {
            origin: Origin::Created,
            name: Identifier::new(name.to_string()),
            terms: subterms,
        }
    }

    /// Return an iterator over the subterms of this atom.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this atom.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }
}

impl Display for Atom {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && self.name == other.name && self.terms == other.terms
    }
}

impl Hash for Atom {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for Atom {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

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

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        if !self.name.is_valid() {
            todo!()
        }

        for term in self.subterms() {
            term.validate()?;
        }

        Ok(())
    }
}
