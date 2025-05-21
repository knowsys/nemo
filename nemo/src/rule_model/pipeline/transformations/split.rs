//! This module defines [TransformationSplitRule].

use crate::rule_model::{
    components::{rule::Rule, ComponentIdentity},
    error::ValidationReport,
    pipeline::{
        commit::ProgramCommit,
        state::{ExtendStatementKind, ExtendStatementValidity},
        ProgramPipeline,
    },
    program::ProgramRead,
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each occurrence of a global variable
/// with the term it evaluates to.
#[derive(Debug, Clone, Copy)]
pub struct TransformationSplitRule {}

impl ProgramTransformation for TransformationSplitRule {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(
        self,
        commit: &mut ProgramCommit,
        pipeline: &ProgramPipeline,
    ) -> Result<(), ValidationReport> {
        for rule in pipeline.rules() {
            if rule.head().len() > 1 {
                commit.delete(rule.id());

                for head in rule.head() {
                    let new_rule = Rule::new(vec![head.clone()], rule.body().clone());

                    commit.add_rule(new_rule);
                }
            }
        }

        Ok(())
    }
}
