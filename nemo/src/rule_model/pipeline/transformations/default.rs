//! This module defines [TransformationDefault].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{
        error::ValidationReport,
        pipeline::transformations::validate::TransformationValidate,
        programs::{handle::ProgramHandle, ProgramRead},
    },
};

use super::{exports::TransformationExports, global::TransformationGlobal, ProgramTransformation};

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

        commit
            .submit()?
            .transform(TransformationGlobal::new(&self.parameters.global_variables))?
            .transform(TransformationExports::new(
                self.parameters.export_parameters,
            ))?
            .transform(TransformationValidate::default())
        // .transform(TransformationActive::default())
    }
}
