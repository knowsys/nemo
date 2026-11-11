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
    ///
    /// The name of a column format may end in the marker `?` of a column that admits a missing
    /// value, as in `int?`.
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

impl Identifier {
    /// Writes the identifier.
    ///
    /// `applied` says whether a `(` or a `{` may follow the identifier,
    /// as it does after the name of a predicate, a function term or a labeled map.
    /// A builtin's name is parsed as a call there, so it is written in angle brackets.
    fn fmt_name(&self, f: &mut Formatter<'_>, applied: bool) -> fmt::Result {
        match self {
            Identifier::Bare(name) => {
                // The marker of a column format follows its name.
                let (name, marker) = match name.strip_suffix('?') {
                    Some(name) => (name, "?"),
                    None => (name.as_str(), ""),
                };

                // A name that is not a valid bare name is written in angle brackets.
                let reads_back = IriRef::new(name).writes_bare()
                    && !(applied && Operator::from_name(name).is_some());
                if reads_back {
                    write!(f, "{name}{marker}")
                } else {
                    write!(f, "{}{marker}", IriRef::new(name).bracketed())
                }
            }
            Identifier::Prefixed { prefix, name } => write!(f, "{prefix}:{name}"),
            Identifier::Iri(iri) => write!(f, "{}", IriRef::new(iri).bracketed()),
            Identifier::Qualified { module, name } => write!(f, "{module}::{name}"),
        }
    }

    /// Returns the identifier as it is written where it is a constant,
    /// which no `(` and no `{` follows.
    ///
    /// The name of a builtin is read back as a name there, so it is written without angle
    /// brackets.
    pub(crate) fn as_constant(&self) -> impl Display + '_ {
        struct Constant<'a>(&'a Identifier);

        impl Display for Constant<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                self.0.fmt_name(f, false)
            }
        }

        Constant(self)
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.fmt_name(f, true)
    }
}
