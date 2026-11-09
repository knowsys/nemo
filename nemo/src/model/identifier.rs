//! Identifiers: names as written in a program.

mod renaming;

pub(crate) use self::renaming::{IdentifierRenaming, IdentifierRole};

use std::fmt::{self, Display, Formatter};

use nemo_physical::value::IriRef;

use crate::model::Operator;

/// A name as written in a program, in any of its forms.
///
/// An identifier names a predicate, a constant, a function, a datatype, a structure label,
/// an attribute or a format.
/// Names are stored as written.
/// [Engine::link](crate::engine::Engine::link) resolves module-qualified names by the `@use`
/// directives, and loading a program resolves the other names by `@base` and `@prefix`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Identifier {
    /// A bare name like `parent`, which is an IRI relative to the `@base`, if there is one.
    Bare(String),
    /// A name abbreviated by a declared prefix, like `foaf:Person`.
    Prefixed {
        /// The prefix, before the `:`.
        prefix: String,
        /// The local name, after the `:`.
        name: String,
    },
    /// A full IRI, written in angle brackets.
    Iri(String),
    /// A name in a module, reached through the module's alias, like `geo::area`.
    Qualified {
        /// The module's alias, before the `::`.
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
                // A name that is not a valid bare name, or that is a builtin's name, is written
                // in brackets, since a builtin's name before a `(` is parsed as a call.
                let reads_back =
                    IriRef::new(name).writes_bare() && Operator::from_name(name).is_none();
                if reads_back {
                    f.write_str(name)
                } else {
                    write!(f, "{}", IriRef::new(name).bracketed())
                }
            }
            Identifier::Prefixed { prefix, name } => write!(f, "{prefix}:{name}"),
            Identifier::Iri(iri) => write!(f, "{}", IriRef::new(iri).bracketed()),
            Identifier::Qualified { module, name } => write!(f, "{module}::{name}"),
        }
    }
}
