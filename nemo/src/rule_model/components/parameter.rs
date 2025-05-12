//! This module defines [ParameterDeclaration].

use std::fmt;

use crate::rule_model::{origin::Origin, pipeline::id::ProgramComponentId};

use super::{
    term::{primitive::variable::global::GlobalVariable, Term},
    ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
    ProgramComponentKind,
};

/// Parameter declaration
///
/// Declares a global variable and potentially assigns it to a [Term].
#[derive(Debug, Clone)]
pub struct ParameterDeclaration {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Global variable that is potentially assigned a value
    variable: GlobalVariable,
    /// Value assigned to the global variable
    /// If this is `None` then it is expected
    /// the the value will be provided externally, e.g. as a command line parameter
    expression: Option<Term>,
}

impl ParameterDeclaration {
    /// Create a new [ParameterDeclaration].
    pub fn new(variable: GlobalVariable) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            variable,
            expression: None,
        }
    }

    /// Return a reference to the expression assigned to the global variable,
    /// if there is one.
    pub fn expression(&self) -> Option<&Term> {
        self.expression.as_ref()
    }

    /// Set the expression that is assigned to the global variable
    pub fn set_expression(&mut self, expression: Term) {
        self.expression = Some(expression)
    }

    /// Return a reference to the global variable that
    /// is assigned to a term.
    pub fn variable(&self) -> &GlobalVariable {
        &self.variable
    }

    /// Return a mutable reference to the global variable
    /// that is assigned to a term.
    pub fn set_variable(&mut self) -> &mut GlobalVariable {
        &mut self.variable
    }
}

impl fmt::Display for ParameterDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.expression {
            Some(expression) => f.write_fmt(format_args!(
                "@parameter {} = {}.",
                self.variable, expression,
            )),
            None => f.write_fmt(format_args!("@parameter {}.", self.variable)),
        }
    }
}

impl ComponentBehavior for ParameterDeclaration {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::ParameterDeclaration
    }

    fn validate(
        &self,
        _builder: &mut crate::rule_model::error::ValidationErrorBuilder,
    ) -> Option<()> {
        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for ParameterDeclaration {
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

impl IterableComponent for ParameterDeclaration {}
