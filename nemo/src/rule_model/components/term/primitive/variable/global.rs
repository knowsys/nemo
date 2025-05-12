//! This module defines [GlobalVariable].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{
        symbols::Symbols, ComponentBehavior, ComponentIdentity, IterableComponent,
        ProgramComponent, ProgramComponentKind,
    },
    error::{validation_error::ValidationError, ValidationReport},
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Globally defined named constant
///
/// Variable that implies the existence of a value satisfying a certain pattern.
#[derive(Debug, Clone)]
pub struct GlobalVariable {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the variable
    name: String,
}

impl GlobalVariable {
    /// Create a new [GlobalVariable].
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            name: name.to_owned(),
        }
    }

    /// Return the name of this variable.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Change the name of this variable.
    pub fn rename(&mut self, name: String) {
        self.name = name;
    }
}

impl PartialEq for GlobalVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for GlobalVariable {}

impl PartialOrd for GlobalVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for GlobalVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Display for GlobalVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.name)
    }
}

impl ComponentBehavior for GlobalVariable {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        if Symbols::is_reserved(self.name()) {
            report.add(
                self,
                ValidationError::InvalidVariableName(self.name().to_owned()),
            );
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for GlobalVariable {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl IterableComponent for GlobalVariable {}
