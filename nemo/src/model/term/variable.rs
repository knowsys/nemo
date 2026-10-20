//! Variables of rules and defined builtins.

use std::fmt::{self, Display, Formatter};

/// The kinds of variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VariableKind {
    /// A universally quantified variable, written `?name`.
    Universal,
    /// An existentially quantified variable, written `!name`.
    Existential,
    /// A global variable bound to a parameter, written `$name`.
    Global,
    /// A nameless placeholder, written `_`, each occurrence its own variable.
    Anonymous,
}

impl VariableKind {
    /// Returns the marker character prefixing variables of this kind.
    ///
    /// An anonymous variable is nothing but its marker.
    pub fn prefix(&self) -> char {
        match self {
            VariableKind::Universal => '?',
            VariableKind::Existential => '!',
            VariableKind::Global => '$',
            VariableKind::Anonymous => '_',
        }
    }
}

/// A variable: universal, existential, global, or anonymous.
///
/// Carries no span of its own: variables occur wrapped in a
/// [Term](crate::model::Term) (or in the `per` scope of an
/// [Aggregate](crate::model::Aggregate)), which carries it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Variable {
    /// The kind of the variable.
    pub kind: VariableKind,
    /// The name, without the kind marker.
    pub name: String,
}

impl Variable {
    /// Creates a universal variable of the given name.
    pub fn universal(name: impl Into<String>) -> Self {
        Self {
            kind: VariableKind::Universal,
            name: name.into(),
        }
    }

    /// Creates an existential variable of the given name.
    pub fn existential(name: impl Into<String>) -> Self {
        Self {
            kind: VariableKind::Existential,
            name: name.into(),
        }
    }

    /// Creates a global variable of the given name.
    pub fn global(name: impl Into<String>) -> Self {
        Self {
            kind: VariableKind::Global,
            name: name.into(),
        }
    }

    /// Creates an anonymous variable.
    pub fn anonymous() -> Self {
        Self {
            kind: VariableKind::Anonymous,
            name: String::new(),
        }
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.kind.prefix(), self.name)
    }
}
