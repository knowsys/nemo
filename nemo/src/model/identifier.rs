//! Identifiers: names as written in a program.

mod renaming;

pub(crate) use self::renaming::{IdentifierRenaming, IdentifierRole};

use std::fmt::{self, Display, Formatter};

use crate::{model::Operator, parser::is_bare_name};

/// A name as written in a program, in any of its forms: of a predicate, a constant, a function,
/// a datatype, a structure label, an attribute, or a format.
///
/// Prefixed and module-qualified names are stored as written.
/// The engine resolves module-qualified names against `@use` when it links the program
/// ([Engine::link](crate::engine::Engine::link)),
/// and prefixed names against `@prefix` when it loads it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Identifier {
    /// A bare name like `parent`, a relative IRI.
    Bare(String),
    /// A name abbreviated by a declared prefix, like `foaf:Person`.
    Prefixed {
        /// The prefix part before the `:`.
        prefix: String,
        /// The name part after the `:`.
        name: String,
    },
    /// A full IRI, written in angle brackets.
    Iri(String),
    /// A name reached through a module alias, like `geo::area`.
    Qualified {
        /// The module alias before the `::`.
        module: String,
        /// The public name within the module.
        name: String,
    },
}

impl Identifier {
    /// Creates a bare name.
    pub fn bare(name: impl Into<String>) -> Self {
        Identifier::Bare(name.into())
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Identifier::Bare(name) => {
                // `_`, the booleans and builtin names read as something else before a `(`.
                let reads_back = is_bare_name(name)
                    && !matches!(name.as_str(), "_" | "true" | "false")
                    && Operator::from_name(name).is_none();
                if reads_back {
                    f.write_str(name)
                } else {
                    write!(f, "<{name}>")
                }
            }
            Identifier::Prefixed { prefix, name } => write!(f, "{prefix}:{name}"),
            Identifier::Iri(iri) => write!(f, "<{iri}>"),
            Identifier::Qualified { module, name } => write!(f, "{module}::{name}"),
        }
    }
}
