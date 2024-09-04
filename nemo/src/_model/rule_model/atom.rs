use crate::model::VariableAssignment;

use super::{Aggregate, Identifier, PrimitiveTerm, Term, Variable};

/// An atom.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Atom {
    /// The predicate.
    predicate: Identifier,
    /// The terms.
    terms: Vec<Term>,
}

impl Atom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<Term>) -> Self {
        Self { predicate, terms }
    }

    /// Return the predicate [Identifier].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    #[must_use]
    pub fn terms(&self) -> &Vec<Term> {
        &self.terms
    }

    /// Return the terms trees in the atom - mutable.
    #[must_use]
    pub fn terms_mut(&mut self) -> &mut Vec<Term> {
        &mut self.terms
    }

    /// Returns all terms at the leave of the term trees of the atom.
    pub fn primitive_terms(&self) -> impl Iterator<Item = &PrimitiveTerm> {
        self.terms.iter().flat_map(|t| t.primitive_terms())
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms.iter().flat_map(|t| t.variables())
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms.iter().flat_map(|t| t.universal_variables())
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms.iter().flat_map(|t| t.existential_variables())
    }

    /// Return all aggregates in the atom.
    pub fn aggregates(&self) -> Vec<Aggregate> {
        let mut result = Vec::new();
        for term in self.terms() {
            result.extend(term.aggregates());
        }

        result
    }

    /// Replaces [super::Variable]s with [Term]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        for tree in &mut self.terms {
            tree.apply_assignment(assignment);
        }
    }
}

impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate.fmt(f)?;
        f.write_str("(")?;
        for (index, term) in self.terms().iter().enumerate() {
            term.fmt(f)?;
            if index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}
