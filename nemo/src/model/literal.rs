//! Body elements of rules.

use std::fmt::{self, Display, Formatter};

use crate::model::{Atom, Operation};

/// A body element of a [Rule](crate::model::Rule).
///
/// Carries no span of its own: the wrapped component does.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    /// An atom that must hold.
    Positive(Atom),
    /// An atom that must not hold.
    Negative(Atom),
    /// A constraint expressed by an operation, like `?age >= 18`.
    Operation(Operation),
}

impl Literal {
    /// Returns the atom of a positive or a negated literal, or `None` for a constraint.
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
