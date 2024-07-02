//! This module defines [FunctionTerm]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::ProgramComponent, error::ProgramConstructionError, origin::Origin,
};

use super::{Identifier, Term};

/// Function term
#[derive(Debug, Clone, Eq)]
pub struct FunctionTerm {
    /// Origin of this component
    origin: Origin,

    /// Name of the function
    name: Identifier,
    /// Subterms of the function
    terms: Vec<Term>,
}

impl FunctionTerm {
    /// Create a new [FunctionTerm].
    pub fn new(name: &str, subterms: Vec<Term>) -> Self {
        Self {
            origin: Origin::Created,
            name: Identifier::new(name.to_string()),
            terms: subterms,
        }
    }

    /// Return an iterator over the subterms of this function term.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this function terms.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }
}

impl Display for FunctionTerm {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for FunctionTerm {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && self.name == other.name && self.terms == other.terms
    }
}

impl PartialOrd for FunctionTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.terms.partial_cmp(&other.terms)
    }
}

impl Hash for FunctionTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for FunctionTerm {
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
            term.validate()?
        }

        Ok(())
    }
}
