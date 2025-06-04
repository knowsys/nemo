//! This module defines [TransformationValidate].

use crate::rule_model::{error::ValidationReport, pipeline::ProgramPipeline, program::ProgramRead};

use super::ProgramTransformation;

/// Validation transformation
///
/// This transformation will check the correctness of given program,
/// assuming the correct external parameters are provided.
///
/// This transformation is mostly useful for the language server.
#[derive(Debug, Copy, Clone, Default)]
pub struct TransformationValidate {}

impl ProgramTransformation for TransformationValidate {
    fn apply(self, pipeline: &mut ProgramPipeline, report: &mut ValidationReport) {
        pipeline.validate_arity(report);
        pipeline.validate_stdin_imports(report);
        pipeline.validate_components(report);
    }
}
