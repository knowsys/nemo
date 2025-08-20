//! This module defines [TransformationDefault].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{
        components::term::Term, error::ValidationReport, pipeline::transformations::validate::TransformationValidate, programs::{handle::ProgramHandle, ProgramRead}
    },
};

use super::{
    active::TransformationActive, exports::TransformationExports, global::TransformationGlobal,
    ProgramTransformation, projection_pushing::TransformationProjectionPushing, filter_pushing::TransformationFilterPushing
};

/// Default transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug)]
pub struct TransformationDefault<'a> {
    /// Execution Parameters
    parameters: &'a ExecutionParameters,
}

impl<'a> TransformationDefault<'a> {
    /// Create a new [TransformationDefault].
    pub fn new(parameters: &'a ExecutionParameters) -> Self {
        Self { parameters }
    }

    // check whether filter pushing can and should be applied
    fn check_application_filter_pushing(program: &ProgramHandle) -> bool {
        for rule in program.rules() {
            if rule.body().len() > 5 || rule.positive_variables().len() > 5 {
                return false;
            }

            for literal in rule.body() {
                for term in literal.terms() {
                    if !Self::supported_for_filter_pushing(term) {
                        return false;
                    }
                }
            }
        }
        true
    }

    // check whether a term is supported for filter pushing
    fn supported_for_filter_pushing(term: &Term) -> bool {
        match term {
            Term::Primitive(_) => true,
            Term::Operation(op) => op.terms().all(Self::supported_for_filter_pushing),
            _ => false,
        }
    }
}

impl<'a> ProgramTransformation for TransformationDefault<'a> {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork_full();

        program.validate_parameters(
            commit.report_mut(),
            self.parameters
                .global_variables
                .keys()
                .collect::<HashSet<_>>(),
        );

        let mut commit = commit
            .submit()?
            .transform(TransformationGlobal::new(&self.parameters.global_variables))?
            .transform(TransformationExports::new(
                self.parameters.export_parameters,
            ))?
            .transform(TransformationValidate::default())?
            .transform(TransformationActive::default())?
            .transform(TransformationFilterPushing::default());

        if Self::check_application_filter_pushing(program) {
            commit = commit?
                .transform(TransformationProjectionPushing::default());
        }

        commit
    }
}
