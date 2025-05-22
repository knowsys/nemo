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
/// Rewrites multi-head rules into several single-head rules with the same body,
/// and only one of the head atoms each.
/// 
/// FIXME: The implementation is only for showcasing the code. It is not semantically correct
/// for rules that contain existential quantifiers, where one would have to split into "pieces"
/// (connected components of head atoms that share existential variables). Currently, this 
/// transformation is unused, and this semantic error does not affect reasoning.
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
