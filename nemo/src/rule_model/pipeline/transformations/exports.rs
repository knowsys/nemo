//! This module defines [TransformationExports].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExportParameters,
    rule_model::{
        components::{import_export::ExportDirective, statement::Statement, tag::Tag},
        error::ValidationReport,
        programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
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
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let derived_predicates = program.derived_predicates();
        let mut exported_predicates = HashSet::<Tag>::new();

        for statement in program.statements() {
            let keep = if let Statement::Export(export) = statement {
                exported_predicates.insert(export.predicate().clone());

                match self.parameters {
                    ExportParameters::Keep => true,
                    ExportParameters::None => false,
                    ExportParameters::Idb => derived_predicates.contains(export.predicate()),
                    ExportParameters::Edb => !derived_predicates.contains(export.predicate()),
                    ExportParameters::All => false, // we'll add one below, no need to keep it
                }
            } else {
                true
            };

            if keep {
                commit.keep(statement);
            }
        }

        match self.parameters {
            ExportParameters::All => {
                for predicate in program.all_predicates().difference(&exported_predicates) {
                    let export = ExportDirective::new_csv(predicate.clone());
                    commit.add_export(export);
                }
            }
            ExportParameters::Idb => {
                for predicate in derived_predicates.difference(&exported_predicates) {
                    let export = ExportDirective::new_csv(predicate.clone());
                    commit.add_export(export);
                }
            }
            ExportParameters::Edb => {
                for predicate in program.import_predicates().difference(&exported_predicates) {
                    let export = ExportDirective::new_csv(predicate.clone());
                    commit.add_export(export);
                }
            }
            ExportParameters::Keep | ExportParameters::None => {}
        };

        commit.submit()
    }
}
