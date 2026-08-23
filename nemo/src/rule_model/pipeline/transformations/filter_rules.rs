//! This module defines [TransformationFilterRules].

use super::ProgramTransformation;
use crate::rule_model::{
    components::statement::Statement,
    error::ValidationReport,
    programs::{ProgramRead, handle::ProgramHandle},
};

/// Program transformation that retains rules selected by a [RuleSelector].
#[derive(Debug, Clone, Copy)]
pub struct TransformationFilterRules(pub RuleSelector);

impl TransformationFilterRules {
    fn matches(&self, statement: &Statement) -> bool {
        let Statement::Rule(rule) = statement else {
            return false;
        };

        let is_existential = rule.existential_variables().next().is_some();
        match self.0 {
            RuleSelector::Existential => is_existential,
            RuleSelector::NonExistential => !is_existential,
        }
    }
}

/// Selects rules based on whether they contain existential variables.
#[derive(Debug, Clone, Copy)]
pub enum RuleSelector {
    /// Select rules containing at least one existential variable.
    Existential,
    /// Select rules containing no existential variables.
    NonExistential,
}

impl ProgramTransformation for TransformationFilterRules {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program
            .statements()
            .filter(|statement| self.matches(statement))
        {
            commit.keep(statement);
        }

        commit.submit()
    }
}
