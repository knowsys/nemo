//! This module defines [ExistentialVariable].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::ProgramComponent,
    error::{validation_error::ValidationErrorKind, ValidationError, ValidationErrorBuilder},
    origin::Origin,
};

use super::VariableName;

/// Existentially quantified variable
///
/// Variable that implies the existence of a value satisfying a certain pattern.
#[derive(Debug, Clone, Eq)]
pub struct ExistentialVariable {
    /// Origin of this component
    origin: Origin,

    /// Name of the variable
    name: VariableName,
}

impl ExistentialVariable {
    /// Create a new [ExistentialVariable].
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            name: VariableName::new(name.to_string()),
        }
    }

    /// Return the name of this variable.
    pub fn name(&self) -> String {
        self.name.to_string()
    }
}

impl Display for ExistentialVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.name)
    }
}

impl PartialEq for ExistentialVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for ExistentialVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for ExistentialVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ProgramComponent for ExistentialVariable {
    fn parse(_string: &str) -> Result<Self, ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        if !self.name.is_valid() {
            builder.report_error(
                self.origin,
                ValidationErrorKind::InvalidVariableName(self.name()),
            );
        }

        Ok(())
    }
}