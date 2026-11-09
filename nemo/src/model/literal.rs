//! Literals, the elements of a rule's body.

use std::fmt::{self, Display, Formatter};

use crate::model::{Atom, Operation};

/// A literal, which is one element of the body of a [Rule](crate::model::Rule).
///
/// A literal has no span of its own, since the component it wraps has one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    /// An atom that must hold.
    Positive(Atom),
    /// An atom that must not hold, written with `~`.
    Negative(Atom),
    /// An operation that must hold, like `?age >= 18`.
    Operation(Operation),
}

impl Literal {
    /// Returns the atom of a positive or a negated literal, or `None` for an operation.
    pub fn atom(&self) -> Option<&Atom> {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
            Literal::Operation(_) => None,
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Positive(atom) => write!(f, "{atom}"),
            Literal::Negative(atom) => write!(f, "~{atom}"),
            Literal::Operation(operation) => write!(f, "{operation}"),
        }
    }
}
