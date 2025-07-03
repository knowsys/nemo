//! This module defines [Program].

use std::fmt::Write;

use crate::rule_model::{
    components::{
        component_iterator, component_iterator_mut, rule::Rule, statement::Statement,
        ComponentBehavior, ComponentIdentity, ComponentSource, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationReport,
    origin::Origin,
    pipeline::id::ProgramComponentId,
    programs::{ProgramRead, ProgramWrite},
};

/// Representation of a nemo program
#[derive(Debug, Default, Clone)]
pub struct Program {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Statements
    statements: Vec<Statement>,
    /// Rules
    rules: Vec<usize>,
}

impl Program {
    /// Return the rule at a particular index.
    ///
    /// # Panics
    /// Panics if there is no rule at this position.
    pub fn rule(&self, index: usize) -> &Rule {
        let rule_index = self.rules[index];

        match &self.statements[rule_index] {
            Statement::Rule(rule) => rule,
            _ => unreachable!(),
        }
    }

    /// Returns all rules of the program
    // TODO: NEEDED FOR TESTS OF STATIC CHECKS, SHOULD BE REMOVED
    pub fn all_rules(&self) -> Vec<Rule> {
        (0..self.rules.len()).fold(Vec::<Rule>::new(), |mut ret_val, index| {
            ret_val.push(self.rule(index).clone());
            ret_val
        })
    }

    /// Remove all export statements
    pub fn clear_exports(&mut self) {
        self.statements.retain(|statement| !statement.is_export());
    }

    /// Remove all export statements
    pub fn clear_imports(&mut self) {
        self.statements.retain(|statement| !statement.is_import());
    }
}

impl ProgramRead for Program {
    fn statements(&self) -> impl Iterator<Item = &Statement> {
        self.statements.iter()
    }

    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules
            .iter()
            .map(|&index| match &self.statements[index] {
                Statement::Rule(rule) => rule,
                _ => unreachable!(),
            })
    }
}

impl ProgramWrite for Program {
    fn add_statement(&mut self, statement: Statement) {
        if statement.is_rule() {
            self.rules.push(self.statements.len());
        }

        self.statements.push(statement);
    }

    fn add_rule(&mut self, rule: Rule) {
        self.rules.push(self.statements.len());
        self.statements.push(Statement::Rule(rule))
    }
}

impl ComponentBehavior for Program {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Program
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        self.validate_arity(&mut report);
        self.validate_stdin_imports(&mut report);

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Program {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Program {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Program {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(component_iterator(self.statements.iter()))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        Box::new(component_iterator_mut(self.statements.iter_mut()))
    }
}

impl std::fmt::Display for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for statement in &self.statements {
            statement.fmt(f)?;
            f.write_char('\n')?;
        }

        Ok(())
    }
}
