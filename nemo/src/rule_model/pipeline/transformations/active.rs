//! This module defines [TransformationActive].

use std::collections::HashSet;

use crate::rule_model::{
    components::{tag::Tag, ComponentIdentity},
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
/// Removes any rule that is not needed in the output.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationActive {}

impl ProgramTransformation for TransformationActive {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::Except(&ExtendStatementKind::Rule))
    }

    fn apply(
        self,
        commit: &mut ProgramCommit,
        pipeline: &ProgramPipeline,
    ) -> Result<(), ValidationReport> {
        let mut required = HashSet::<Tag>::new();
        let mut result = HashSet::new();

        for output in pipeline.outputs() {
            required.insert(output.predicate().clone());
        }

        for export in pipeline.exports() {
            required.insert(export.predicate().clone());
        }

        let mut required_count: usize = 0;
        while required_count != required.len() {
            required_count = required.len();

            for rule in pipeline.rules() {
                if result.contains(&rule.id()) {
                    continue;
                }

                if rule
                    .head()
                    .iter()
                    .any(|atom| required.contains(&atom.predicate()))
                {
                    for predicate in rule.body().iter().filter_map(|literal| literal.predicate()) {
                        required.insert(predicate);
                    }

                    result.insert(rule.id());
                }
            }
        }

        for id in result {
            commit.keep(id);
        }

        Ok(())
    }
}
