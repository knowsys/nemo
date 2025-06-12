//! Defines the trait [ChaseAtom]

pub(crate) mod ground_atom;
pub(crate) mod primitive_atom;
pub(crate) mod variable_atom;

use std::fmt::Display;

use crate::rule_model::components::{tag::Tag, IterableVariables};

/// Tagged list of terms.
pub trait ChaseAtom: IterableVariables + Display {
    /// Type of the terms within the atom.
    type TypeTerm;

    /// Return the predicate [Tag].
    fn predicate(&self) -> Tag;

    /// Return an immutable iterator over the list of terms.
    fn terms(&self) -> impl Iterator<Item = &Self::TypeTerm>;

    /// Return a mutable iterator over the list of terms.
    fn terms_mut(&mut self) -> impl Iterator<Item = &mut Self::TypeTerm>;

    /// Return the arity of the atom
    fn arity(&self) -> usize {
        self.terms().count()
    }
}
