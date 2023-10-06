use std::path::PathBuf;

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
