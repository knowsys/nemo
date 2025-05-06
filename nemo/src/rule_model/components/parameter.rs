use std::fmt;

use crate::rule_model::{origin::Origin, pipeline::id::ProgramComponentId};

use super::{
    term::{primitive::variable::global::GlobalVariable, Term},
    ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
    ProgramComponentKind,
};

#[derive(Debug, Clone)]
pub struct ParameterDeclaration {
    origin: Origin,
    id: ProgramComponentId,
    variable: GlobalVariable,
    expression: Option<Term>,
}

impl ParameterDeclaration {
    pub fn new(variable: GlobalVariable) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            variable,
            expression: None,
        }
    }

    pub fn set_expression(&mut self, expression: Term) {
        self.expression = Some(expression)
    }

    pub fn variable(&self) -> &GlobalVariable {
        &self.variable
    }

    pub fn expression(&self) -> Option<&Term> {
        self.expression.as_ref()
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
        builder: &mut crate::rule_model::error::ValidationErrorBuilder,
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
