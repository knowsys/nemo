//! This module defines [TransformationDefault].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{error::ValidationReport, pipeline::ProgramPipeline, program::ProgramRead},
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
    fn apply(self, pipeline: &mut ProgramPipeline, report: &mut ValidationReport) {
        pipeline.validate_parameters(
            report,
            self.parameters
                .global_variables
                .keys()
                .collect::<HashSet<_>>(),
        );

        TransformationGlobal::new(self.parameters.global_variables).apply(pipeline, report);
        TransformationExports::new(self.parameters.export_parameters).apply(pipeline, report);

        pipeline.validate_arity(report);
        pipeline.validate_stdin_imports(report);
        pipeline.validate_components(report);

        if report.contains_errors() {
            return;
        }

        TransformationActive::default().apply(pipeline, report);
    }
}
