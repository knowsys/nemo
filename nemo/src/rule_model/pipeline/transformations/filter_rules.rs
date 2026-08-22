use super::ProgramTransformation;
use crate::rule_model::components::statement::Statement;
use crate::rule_model::error::ValidationReport;
use crate::rule_model::programs::{ProgramRead, handle::ProgramHandle};

#[derive(Debug, Clone, Copy)]
pub struct TransformationFilterRules(pub RuleSelector);

impl TransformationFilterRules {
    fn sel(&self) -> &RuleSelector {
        &self.0
    }

    fn filter(&self, stmt: &Statement) -> bool {
        let rule = match stmt {
            Statement::Rule(rule) => rule,
            _ => return false,
        };
        let non_ex = 0 == rule.existential_variables().count();
        match self.sel() {
            RuleSelector::Existential => !non_ex,
            RuleSelector::NonExistential => non_ex,
        }
    }
}

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
            .filter(|stmt| self.filter(stmt))
            .for_each(|stmt| commit.keep(stmt));
        // for stmt in program.statements() {
        //     if let Statement::Rule(rule) = stmt {
        //         match self.sel() {
        //             RuleSelector::Existential
        //         }
        //         if 0 == rule.existential_variables().count() {
        //             continue;
        //         }
        //         commit.keep(stmt)
        //     }
        // }

        commit.submit()
    }
}
