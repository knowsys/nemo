//! This module defines [TransformationExports].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExportParameters,
    rule_model::{
        components::{import_export::ExportDirective, tag::Tag, ComponentIdentity},
        error::ValidationReport,
        pipeline::{commit::ProgramCommit, ProgramPipeline},
        program::ProgramRead,
    },
};

use super::ProgramTransformation;

/// Program transformation
///
/// Adds or removes export statements depending on external parameters
#[derive(Debug, Clone, Copy)]
pub struct TransformationExports {
    /// External export parameters
    parameters: ExportParameters,
}

impl TransformationExports {
    /// Create a new [TransformationExports].
    pub fn new(parameters: ExportParameters) -> Self {
        Self { parameters }
    }
}

impl ProgramTransformation for TransformationExports {
    fn apply(self, pipeline: &mut ProgramPipeline, _report: &mut ValidationReport) {
        if let ExportParameters::Keep = self.parameters {
            return;
        }

        let mut commit = ProgramCommit::default();

        let derived_predicates = pipeline.derived_predicates();
        let mut exported_predicates = HashSet::<Tag>::new();

        for export in pipeline.exports() {
            let delete = match self.parameters {
                ExportParameters::Keep => false,
                ExportParameters::None => true,
                ExportParameters::Idb => derived_predicates.contains(export.predicate()),
                ExportParameters::Edb => !derived_predicates.contains(export.predicate()),
                ExportParameters::All => {
                    exported_predicates.insert(export.predicate().clone());
                    false
                }
            };

            if delete {
                commit.delete(export.id());
            }
        }

        if let ExportParameters::All = self.parameters {
            let all_predicates = pipeline.all_predicates();

            for predicate in all_predicates.difference(&exported_predicates) {
                let export = ExportDirective::new_csv(predicate.clone());
                commit.add_export(export);
            }
        }

        pipeline.commit(commit);
    }
}
