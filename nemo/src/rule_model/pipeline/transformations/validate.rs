//! This module defines [TransformationValidate].

use crate::rule_model::{
    error::ValidationReport,
    programs::{handle::ProgramHandle, ProgramRead},
};

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
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork_full();

        program.validate_arity(commit.report_mut());
        program.validate_stdin_imports(commit.report_mut());
        program.validate_components(commit.report_mut());

        commit.submit()
    }
}
