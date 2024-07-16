//! This module defines [Variable]

use std::fmt::Display;

use existential::ExistentialVariable;
use universal::UniversalVariable;

use crate::rule_model::{
    error::{ValidationError, ValidationErrorBuilder},
    origin::Origin,
};

use super::ProgramComponent;

pub mod existential;
pub mod universal;

/// Name of a variable
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VariableName(String);

impl VariableName {
    /// Create a new [VariableName].
    fn new(name: String) -> Self {
        Self(name)
    }

    /// Validate variable name.
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty()
    }
}

impl Display for VariableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Variable
///
/// A general placeholder that can be bound to any value.
/// We distinguish [UniversalVariable] and [ExistentialVariable].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum Variable {
    /// Universal variable
    Universal(UniversalVariable),
    /// Existential variable
    Existential(ExistentialVariable),
}

impl Variable {
    /// Create a new universal variable.
    pub fn universal(name: &str) -> Self {
        Self::Universal(UniversalVariable::new(name))
    }

    /// Create a new existential variable.
    pub fn existential(name: &str) -> Self {
        Self::Existential(ExistentialVariable::new(name))
    }

    /// Create a new anonymous variable.
    pub fn anonymous() -> Self {
        Self::Universal(UniversalVariable::new_anonymous())
    }

    /// Return the name of the variable or `None` if it is anonymous
    pub fn name(&self) -> Option<String> {
        match self {
            Variable::Universal(variable) => variable.name(),
            Variable::Existential(variable) => Some(variable.name()),
        }
    }
}

impl From<UniversalVariable> for Variable {
    fn from(value: UniversalVariable) -> Self {
        Self::Universal(value)
    }
}

impl From<ExistentialVariable> for Variable {
    fn from(value: ExistentialVariable) -> Self {
        Self::Existential(value)
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Variable::Universal(variable) => variable.fmt(f),
            Variable::Existential(variable) => variable.fmt(f),
        }
    }
}

impl ProgramComponent for Variable {
    fn parse(_string: &str) -> Result<Self, ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        match self {
            Variable::Universal(variable) => variable.origin(),
            Variable::Existential(variable) => variable.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Variable::Universal(variable) => Self::Universal(variable.set_origin(origin)),
            Variable::Existential(variable) => Self::Existential(variable.set_origin(origin)),
        }
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        match &self {
            Variable::Universal(universal) => {
                universal.validate(builder)?;
            }
            Variable::Existential(existential) => {
                existential.validate(builder)?;
            }
        }

        Ok(())
    }
}
