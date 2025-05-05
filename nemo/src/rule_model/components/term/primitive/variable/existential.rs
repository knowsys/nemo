//! This module defines [ExistentialVariable].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{
        ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationErrorBuilder,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::VariableName;

/// Existentially quantified variable
///
/// Variable that implies the existence of a value satisfying a certain pattern.
#[derive(Debug, Clone)]
pub struct ExistentialVariable {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Name of the variable
    name: VariableName,
}

impl ExistentialVariable {
    /// Create a new [ExistentialVariable].
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            name: VariableName::new(name.to_owned()),
        }
    }

    /// Return the name of this variable.
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    /// Change the name of this variable.
    pub fn rename(&mut self, name: VariableName) {
        self.name = name;
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

impl Eq for ExistentialVariable {}

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

impl ComponentBehavior for ExistentialVariable {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()> {
        // if !self.name.is_valid() {
        //     builder.report_error(
        //         self.origin,
        //         ValidationErrorKind::InvalidVariableName(self.name()),
        //     );
        // }

        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for ExistentialVariable {
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
        self.origin = origin
    }
}

impl IterableComponent for ExistentialVariable {}
