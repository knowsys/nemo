//! This module defines [TransformationGlobal].

use crate::rule_model::{
    components::{ComponentIdentity, IterableVariables},
    pipeline::{
        commit::ProgramCommit,
        state::{ExtendStatementKind, ExtendStatementValidity},
        ProgramPipeline,
    },
    substitution::Substitution,
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each occurrence of a global variable
/// with the term it evaluates to.
#[derive(Debug)]
pub struct TransformationGlobal {
    /// Current commit
    commit: ProgramCommit,
}

impl ProgramTransformation for TransformationGlobal {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(&mut self, pipeline: &ProgramPipeline) {
        let mut subsitution = Substitution::default();

        for parameter in pipeline.parameters() {
            if let Some(expression) = parameter.expression() {
                let reduced = expression.reduce_with_substitution(&subsitution);
                subsitution.insert(parameter.variable().clone(), reduced);
            }
        }

        for rule in pipeline.rules() {
            if rule.variables().any(|variable| variable.is_global()) {
                self.commit.delete(rule.id());

                let mut new_rule = rule.clone();
                subsitution.apply(&mut new_rule);

                self.commit.add_rule(new_rule);
            }
        }
    }

    fn finalize(self) -> ProgramCommit {
        self.commit
    }
}
