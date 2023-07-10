use std::path::PathBuf;

use nemo_physical::datatypes::Double;
use sanitise_file_name::{sanitise_with_options, Options};

use crate::model::TypeConstraint;

/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) String);

impl Identifier {
    /// Returns the associated name
    pub fn name(&self) -> String {
        self.0.clone()
    }

    /// Returns a sanitised path with respect to the associated name
    pub fn sanitised_file_name(&self, mut path: PathBuf) -> PathBuf {
        let sanitise_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };
        let file_name = sanitise_with_options(&self.name(), &sanitise_options);
        path.push(file_name);
        path
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Identifier(value)
    }
}

/// A qualified predicate name, i.e., a predicate name together with a type constraint.
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct QualifiedPredicateName {
    /// The predicate name
    pub(crate) identifier: Identifier,
    /// The type qualification
    pub(crate) associated_type: Option<TypeConstraint>,
}

impl QualifiedPredicateName {
    /// Construct a new qualified predicate name from an identifier,
    /// leaving the arity unspecified.
    pub fn new(identifier: Identifier) -> Self {
        Self {
            identifier,
            associated_type: None,
        }
    }

    /// Construct a new qualified predicate name with the given arity or a list of types.
    pub fn with_constraint(identifier: Identifier, constraint: TypeConstraint) -> Self {
        Self {
            identifier,
            associated_type: Some(constraint),
        }
    }
}

impl From<Identifier> for QualifiedPredicateName {
    fn from(identifier: Identifier) -> Self {
        Self::new(identifier)
    }
}

/// Variable occuring in a rule
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum Variable {
    /// A universally quantified variable.
    Universal(Identifier),
    /// An existentially quantified variable.
    Existential(Identifier),
}

impl Variable {
    /// Return the name of the variable.
    pub fn name(&self) -> String {
        match self {
            Self::Universal(identifier) | Self::Existential(identifier) => identifier.name(),
        }
    }
}

impl std::fmt::Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

/// Terms occurring in programs.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Term {
    /// An (abstract) constant.
    Constant(Identifier),
    /// A variable.
    Variable(Variable),
    /// A numeric literal.
    NumericLiteral(NumericLiteral),
    /// A string literal.
    StringLiteral(String),
    /// An RDF literal.
    RdfLiteral(RdfLiteral),
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Term::Constant(Identifier(s)) => {
                // Nulls start with __ and shall be wrapped in angle brackets
                if s.starts_with("__") {
                    write!(f, "<{s}>")
                }
                // blank nodes and anything that starts with an ascii latter (like bare names)
                // should not be wrapped in angle brackets
                else if s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                    write!(f, "{s}")
                }
                // everything else shall be wrapped in angle_brackets
                else {
                    write!(f, "<{s}>")
                }
            }
            Term::Variable(term) => write!(f, "{term}"),
            Term::NumericLiteral(term) => write!(f, "{term}"),
            Term::StringLiteral(term) => write!(f, "\"{term}\""),
            Term::RdfLiteral(term) => write!(f, "{term}"),
        }
    }
}

impl Term {
    /// Check if the term is ground.
    pub fn is_ground(&self) -> bool {
        matches!(
            self,
            Self::Constant(_) | Self::NumericLiteral(_) | Self::RdfLiteral(_)
        )
    }
}

/// A numerical literal.
#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum NumericLiteral {
    /// An integer literal.
    Integer(i64),
    /// A decimal literal.
    Decimal(i64, u64),
    /// A double literal.
    Double(Double),
}

impl std::fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericLiteral::Integer(value) => write!(f, "{value}"),
            NumericLiteral::Decimal(left, right) => write!(f, "{left}.{right}"),
            NumericLiteral::Double(value) => write!(f, "{:E}", f64::from(*value)),
        }
    }
}

/// An RDF literal.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    /// A language string.
    LanguageString {
        /// The literal value.
        value: String,
        /// The language tag.
        tag: String,
    },
    /// A literal with a datatype.
    DatatypeValue {
        /// The literal value.
        value: String,
        /// The datatype IRI.
        datatype: String,
    },
}

impl std::fmt::Display for RdfLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdfLiteral::LanguageString { value, tag } => write!(f, "\"{value}\"@{tag}"),
            RdfLiteral::DatatypeValue { value, datatype } => write!(f, "\"{value}\"^^<{datatype}>"),
        }
    }
}
