use std::ops::Neg;

use crate::model::VariableAssignment;

use super::{Aggregate, Atom, Identifier, PrimitiveTerm, Term, Variable};

/// A literal.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum Literal {
    /// A non-negated literal.
    Positive(Atom),
    /// A negated literal.
    Negative(Atom),
}

impl Literal {
    /// Check if the literal is positive.
    pub fn is_positive(&self) -> bool {
        matches!(self, Self::Positive(_))
    }

    /// Check if the literal is negative.
    pub fn is_negative(&self) -> bool {
        matches!(self, Self::Negative(_))
    }

    /// Returns a reference to the underlying atom.
    pub fn atom(&self) -> &Atom {
        match self {
            Self::Positive(atom) => atom,
            Self::Negative(atom) => atom,
        }
    }

    /// Returns a mutable reference to the underlying atom.
    pub fn atom_mut(&mut self) -> &mut Atom {
        match self {
            Self::Positive(atom) => atom,
            Self::Negative(atom) => atom,
        }
    }
}

impl Neg for Literal {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Literal::Positive(atom) => Self::Negative(atom),
            Literal::Negative(atom) => Self::Positive(atom),
        }
    }
}

generate_forwarder!(forward_to_atom; Positive, Negative);

impl Literal {
    /// Return the predicate [Identifier].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        forward_to_atom!(self, predicate)
    }

    /// Return the terms in the literal.
    #[must_use]
    pub fn terms(&self) -> &Vec<Term> {
        forward_to_atom!(self, terms)
    }

    /// Return the variables in the literal.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        forward_to_atom!(self, variables)
    }

    /// Return the universally quantified variables in the literal.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> {
        forward_to_atom!(self, universal_variables)
    }

    /// Return the existentially quantified variables in the literal.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> {
        forward_to_atom!(self, existential_variables)
    }

    /// Returns all terms at the leave of the term trees of the atom.
    pub fn primitive_terms(&self) -> impl Iterator<Item = &PrimitiveTerm> {
        forward_to_atom!(self, primitive_terms)
    }

    /// Return all aggregates in the literal.
    pub fn aggregates(&self) -> Vec<Aggregate> {
        forward_to_atom!(self, aggregates)
    }

    /// Replaces [Variable]s with [Term]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        match self {
            Literal::Positive(atom) => atom.apply_assignment(assignment),
            Literal::Negative(atom) => atom.apply_assignment(assignment),
        }
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Positive(_) => {}
            Literal::Negative(_) => f.write_str("~")?,
        }

        self.atom().fmt(f)
    }
}
