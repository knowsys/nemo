//! This module defines [TransformationSetDefaultOutputs].

use crate::rule_model::{
    components::output::Output,
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// Program transformation
///
/// If no output predicates have been specified, then all predicates are set as outputs.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationSetDefaultOutputs {}

impl ProgramTransformation for TransformationSetDefaultOutputs {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        program.statements().for_each(|s| commit.keep(s));

        if program.outputs().next().is_none() {
            for predicate in program.all_predicates() {
                commit.add_output(Output::new(predicate))
            }
        }

        commit.submit()
    }
}
