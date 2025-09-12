//! This module defines [ImportClause].

use std::fmt::Display;

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut, import_export::ImportDirective,
        term::primitive::variable::Variable, ComponentBehavior, ComponentIdentity, ComponentSource,
        IterableComponent, IterableVariables, ProgramComponent, ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Represents an import executed during the evaluation of a rule.
#[derive(Debug, Clone)]
pub struct ImportClause {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Import
    pub(crate) import: ImportDirective,

    /// Output variables
    variables: Vec<Variable>,
}

impl Display for ImportClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: This component does not have a representation in syntax

        f.write_fmt(format_args!("{}(", self.import.predicate()))?;

        for (term_index, term) in self.variables.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.variables.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl ImportClause {
    /// Create a new [ImportClause]
    pub fn new(import: ImportDirective, variables: Vec<Variable>) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            import,
            variables,
        }
    }

    /// Return a reference to the output variables.
    pub fn output_variables(&self) -> &Vec<Variable> {
        &self.variables
    }

    /// Return a reference to the underlying [ImportDirective].
    pub fn import_directive(&self) -> &ImportDirective {
        &self.import
    }
}

impl ComponentBehavior for ImportClause {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Import
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        // TODO: Variables

        self.import.validate()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for ImportClause {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for ImportClause {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableVariables for ImportClause {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        let import_variables = self.import.variables();

        Box::new(import_variables.chain(self.variables.iter()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let import_variables = self.import.variables_mut();

        Box::new(import_variables.chain(self.variables.iter_mut()))
    }
}

impl IterableComponent for ImportClause {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let import_iter = self.import.children();
        let variable_iter = component_iterator(self.variables.iter());
        Box::new(import_iter.chain(variable_iter))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let import_iter = self.import.children_mut();
        let variable_iter = component_iterator_mut(self.variables.iter_mut());
        Box::new(import_iter.chain(variable_iter))
    }
}
