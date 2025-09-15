//! This module defines [UniversalVariable].

use std::{fmt::Display, hash::Hash};

use crate::{
    parser::ParserErrorReport,
    rule_model::{
        components::{
            ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent,
            ProgramComponent, ProgramComponentKind, symbols::Symbols,
        },
        error::{ValidationReport, validation_error::ValidationError},
        origin::Origin,
        pipeline::id::ProgramComponentId,
        translation::{ProgramParseReport, TranslationComponent},
    },
};

use super::Variable;

/// Universally quantified variable
///
/// Represents a variable that can take on any value in the domain.
///
/// Universal variables may not have a name,
/// in which case we call them anonymous.
#[derive(Debug, Clone)]
pub struct UniversalVariable {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the variable
    ///
    /// This can be `None` in case this is an anonymous variable.
    name: Option<String>,
}

impl UniversalVariable {
    /// Create a new named [UniversalVariable].
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            name: Some(name.to_owned()),
        }
    }

    /// Create a new anonymous [UniversalVariable].
    pub fn new_anonymous() -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            name: None,
        }
    }

    /// Construct this object from a string.
    pub fn parse(input: &str) -> Result<Self, ProgramParseReport> {
        if let Variable::Universal(result) = Variable::parse(input)? {
            Ok(result)
        } else {
            Err(ProgramParseReport::Parsing(ParserErrorReport::empty()))
        }
    }

    /// Return the name of this variable,
    /// or `None` if the variable is unnamed.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Return `true` if this is an anonymous variable,
    /// and `false` otherwise
    pub fn is_anonymous(&self) -> bool {
        self.name.is_none()
    }

    /// Change the name of this variable.
    pub fn rename(&mut self, name: String) {
        self.name = Some(name);
    }
}

impl Display for UniversalVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "?{name}"),
            None => write!(f, "_"),
        }
    }
}

impl PartialEq for UniversalVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for UniversalVariable {}

impl PartialOrd for UniversalVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for UniversalVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ComponentBehavior for UniversalVariable {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        if let Some(name) = self.name()
            && Symbols::is_reserved(name) {
                report.add(
                    self,
                    ValidationError::InvalidVariableName {
                        variable_name: name.to_owned(),
                    },
                );
            }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for UniversalVariable {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for UniversalVariable {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for UniversalVariable {}
