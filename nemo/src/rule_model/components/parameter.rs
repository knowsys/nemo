use std::fmt;

use crate::rule_model::origin::Origin;

use super::{
    term::{primitive::variable::global::GlobalVariable, Term},
    ProgramComponent,
};

#[derive(Debug, Clone)]
pub struct ParameterDeclaration {
    origin: Origin,
    variable: GlobalVariable,
    expression: Option<Term>,
}

impl ParameterDeclaration {
    pub fn new(variable: GlobalVariable) -> Self {
        Self {
            origin: Origin::Created,
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

impl ProgramComponent for ParameterDeclaration {
    type ValidationResult = ();

    fn kind(&self) -> super::ProgramComponentKind {
        super::ProgramComponentKind::ParameterDeclaration
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(self, origin: Origin) -> Self {
        Self { origin, ..self }
    }

    fn validate(
        &self,
        _builder: &mut crate::rule_model::error::ValidationErrorBuilder,
    ) -> Option<Self::ValidationResult> {
        unimplemented!("validation defined in Program::validate")
    }
}
