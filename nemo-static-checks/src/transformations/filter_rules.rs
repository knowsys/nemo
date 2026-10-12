//! This module defines [TransformationFilterRules].
use nemo::rule_model::{
    components::statement::Statement,
    error::ValidationReport,
    pipeline::transformations::ProgramTransformation,
    programs::{ProgramRead, handle::ProgramHandle},
};

/// Program transformation that retains rules selected by a [RuleSelector].
#[derive(Debug, Clone, Copy)]
pub struct TransformationFilterRules(pub RuleSelector);

impl TransformationFilterRules {
    fn matches(&self, stmt: &Statement) -> bool {
        let Statement::Rule(rule) = stmt else {
            return false;
        };

        let is_ex = rule.existential_variables().next().is_some();
        match self.0 {
            RuleSelector::Existential => is_ex,
            RuleSelector::NonExistential => !is_ex,
        }
    }
}

/// Selects rules based on whether they contain existential variables.
#[derive(Debug, Clone, Copy)]
pub enum RuleSelector {
    Existential,
    NonExistential,
}

impl ProgramTransformation for TransformationFilterRules {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        program
            .statements()
            .filter(|stmt| self.matches(stmt))
            .for_each(|stmt| commit.keep(stmt));

        commit.submit()
    }
}
