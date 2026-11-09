//! Variables, which appear in rules, in parameter declarations and in function definitions.

use std::fmt::{self, Display, Formatter};

/// The kinds of variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VariableKind {
    /// A universally quantified variable, written `?name`.
    Universal,
    /// An existentially quantified variable, written `!name`.
    Existential,
    /// A global variable, which a `@parameter` declares, written `$name`.
    Global,
    /// A placeholder without a name, written `_`, whose every occurrence is a distinct variable.
    Anonymous,
}

impl VariableKind {
    /// Returns the marker that variables of this kind start with.
    ///
    /// An anonymous variable consists of its marker alone.
    pub fn prefix(&self) -> char {
        match self {
            VariableKind::Universal => '?',
            VariableKind::Existential => '!',
            VariableKind::Global => '$',
            VariableKind::Anonymous => '_',
        }
    }
}

/// A variable, which is universal, existential, global or anonymous.
///
/// A variable has no span of its own, since it always appears inside a component
/// that has one, such as a [Term](crate::model::Term)
/// or the scope of an [Aggregate](crate::model::Aggregate).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Variable {
    /// The kind of the variable.
    pub kind: VariableKind,
    /// The name, without the marker of the kind.
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
