//! This module defines [UniversalVariable].

use std::{fmt::Display, hash::Hash};

use crate::{
    parse_component,
    parser::ast::ProgramAST,
    rule_model::{
        components::{parse::ComponentParseError, ProgramComponent, ProgramComponentKind},
        error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
        origin::Origin,
        translation::ASTProgramTranslation,
    },
};

use super::{Variable, VariableName};

/// Universally quantified variable
///
/// Represents a variable that can take on any value in the domain.
///
/// Universal variables may not have a name,
/// in which case we call them anonymous.
#[derive(Debug, Clone, Eq)]
pub struct UniversalVariable {
    /// Origin of this component
    origin: Origin,

    /// Name of the variable
    ///
    /// This can be `None` in case this is an anonymous variable.
    name: Option<VariableName>,
}

impl UniversalVariable {
    /// Create a new named [UniversalVariable]
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            name: Some(VariableName::new(name.to_string())),
        }
    }

    /// Create a new anonymous [UniversalVariable]
    pub fn new_anonymous() -> Self {
        Self {
            origin: Origin::Created,
            name: None,
        }
    }

    /// Return the name of this variable,
    /// or `None` if the variable is unnamed.
    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|name| name.to_string())
    }

    /// Return `true` if this is an anonymous variable,
    /// and `false` otherwise
    pub fn is_anonymous(&self) -> bool {
        self.name.is_none()
    }

    /// Change the name of this variable.
    pub fn rename(&mut self, name: VariableName) {
        self.name = Some(name);
    }
}

impl Display for UniversalVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "?{}", name),
            None => write!(f, "_"),
        }
    }
}

impl PartialEq for UniversalVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for UniversalVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UniversalVariable {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl Hash for UniversalVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ProgramComponent for UniversalVariable {
    fn parse(string: &str) -> Result<Self, ComponentParseError> {
        let variable = parse_component!(
            string,
            crate::parser::ast::expression::basic::variable::Variable::parse,
            ASTProgramTranslation::build_variable
        )?;

        if let Variable::Universal(universal) = variable {
            return Ok(universal);
        }

        Err(ComponentParseError::ParseError)
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        if let Some(name) = &self.name {
            if !name.is_valid() {
                builder.report_error(
                    self.origin,
                    ValidationErrorKind::InvalidVariableName(name.0.clone()),
                );
            }
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }
}
