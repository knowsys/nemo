//! This module defines [ParameterDeclaration].

use std::fmt;

use crate::rule_model::{
    components::{
        IterablePrimitives, IterableVariables,
        term::primitive::{Primitive, variable::Variable},
    },
    error::{ValidationReport, validation_error::ValidationError},
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

use super::{
    ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, ProgramComponent,
    ProgramComponentKind, component_iterator, component_iterator_mut,
    term::{Term, primitive::variable::global::GlobalVariable},
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

    /// Return a mutable reference to the expression assigned to the global variable,
    /// if there is one.
    pub fn expression_mut(&mut self) -> Option<&mut Term> {
        self.expression.as_mut()
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
    pub fn variable_mut(&mut self) -> &mut GlobalVariable {
        &mut self.variable
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

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        if let Some(expression) = self.expression() {
            if !expression.is_resolvable() {
                report.add(
                    expression,
                    ValidationError::ParameterDeclarationNotGroundish,
                );
            }
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ParameterDeclaration {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for ParameterDeclaration {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for ParameterDeclaration {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let variable = component_iterator(std::iter::once(self.variable()));
        let term = component_iterator(self.expression().into_iter());

        Box::new(variable.chain(term))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let variable = component_iterator_mut(std::iter::once(&mut self.variable));
        let term = component_iterator_mut(self.expression.as_mut().into_iter());

        Box::new(variable.chain(term))
    }
}

impl IterableVariables for ParameterDeclaration {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.expression.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.expression
                .iter_mut()
                .flat_map(|term| term.variables_mut()),
        )
    }
}

impl IterablePrimitives for ParameterDeclaration {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(std::iter::empty())
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        Box::new(std::iter::empty())
    }
}
