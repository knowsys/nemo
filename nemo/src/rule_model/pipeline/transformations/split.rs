//! This module defines [TransformationSplitRule].

use crate::rule_model::{
    components::{rule::Rule, ComponentIdentity},
    pipeline::{
        commit::ProgramCommit,
        state::{ExtendStatementKind, ExtendStatementValidity},
        ProgramPipeline,
    },
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each occurrence of a global variable
/// with the term it evaluates to.
#[derive(Debug)]
pub struct TransformationSplitRule {
    /// Current commit
    commit: ProgramCommit,
}

impl ProgramTransformation for TransformationSplitRule {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(&mut self, pipeline: &ProgramPipeline) {
        for rule in pipeline.rules() {
            if rule.head().len() > 1 {
                self.commit.delete(rule.id());

                for head in rule.head() {
                    let new_rule = Rule::new(vec![head.clone()], rule.body().clone());

                    self.commit.add_rule(new_rule);
                }
            }
        }
    }

    fn finalize(self) -> ProgramCommit {
        self.commit
    }
}
