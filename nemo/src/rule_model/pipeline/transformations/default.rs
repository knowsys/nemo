//! This module defines [TransformationDefault].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{
        error::ValidationReport,
        pipeline::{
            commit::ProgramCommit,
            state::{ExtendStatementKind, ExtendStatementValidity},
            ProgramPipeline,
        },
        program::ProgramRead,
    },
};

use super::{
    active::TransformationActive, exports::TransformationExports, global::TransformationGlobal,
    ProgramTransformation,
};

/// Default transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug)]
pub struct TransformationDefault {
    /// Execution Parameters
    parameters: ExecutionParameters,
}

impl TransformationDefault {
    /// Create a new [TransformationDefault].
    pub fn new(parameters: ExecutionParameters) -> Self {
        Self { parameters }
    }
}

impl ProgramTransformation for TransformationDefault {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(
        self,
        commit: &mut ProgramCommit,
        pipeline: &ProgramPipeline,
    ) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        pipeline.validate_parameters(
            &mut report,
            self.parameters
                .global_variables
                .keys()
                .collect::<HashSet<_>>(),
        );

        TransformationGlobal::new(self.parameters.global_variables).apply(commit, pipeline)?;
        TransformationExports::new(self.parameters.export_parameters).apply(commit, pipeline)?;

        pipeline.validate_arity(&mut report);
        pipeline.validate_stdin_imports(&mut report);
        report.result()?;

        TransformationActive::default().apply(commit, pipeline)?;

        Ok(())
    }
}
