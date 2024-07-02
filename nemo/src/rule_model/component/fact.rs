//! This module defines [Fact].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::origin::Origin;

use super::{term::Term, ProgramComponent};

/// A (ground) fact
#[derive(Debug, Clone, Eq)]
pub struct Fact {
    /// Origin of this component
    origin: Origin,

    terms: Vec<Term>,
}

impl Fact {
    /// Return an iterator over the subterms of this fact.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this fact.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }
}

impl Display for Fact {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Fact {
    fn eq(&self, other: &Self) -> bool {
        self.terms == other.terms
    }
}

impl Hash for Fact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.terms.hash(state);
    }
}

impl ProgramComponent for Fact {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        todo!()
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}
