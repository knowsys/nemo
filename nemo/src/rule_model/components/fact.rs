//! This module defines [Fact].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::origin::Origin;

use super::{term::Term, ProgramComponent, Tag};

/// A (ground) fact
#[derive(Debug, Clone, Eq)]
pub struct Fact {
    /// Origin of this component
    origin: Origin,

    /// Predicate of the fact
    predicate: Tag,

    /// List of [Term]s
    terms: Vec<Term>,
}

impl Fact {
    /// Create a new [Fact].
    pub fn new<Terms: IntoIterator<Item = Term>>(predicate: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            predicate: Tag::new(predicate.to_string()),
            terms: subterms.into_iter().collect(),
        }
    }

    /// Create a new [Fact] from an AST
    pub fn from_ast(_ast: crate::io::parser::ast::statement::Fact) {
        todo!("create a fact from an ast")
    }

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.predicate))?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for Fact {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate && self.terms == other.terms
    }
}

impl Hash for Fact {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for Fact {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramValidationError>
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

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramValidationError>
    where
        Self: Sized,
    {
        todo!()
    }
}
