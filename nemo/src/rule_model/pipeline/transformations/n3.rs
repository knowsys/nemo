//! This module defines [TransformationN3Default].

pub mod split_triples;

use split_triples::TransformationN3SplitTriples;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{error::ValidationReport, programs::handle::ProgramHandle},
};

use super::{ProgramTransformation, default::TransformationDefault};

/// Default transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug, Copy, Clone)]
pub struct TransformationN3Default<'a> {
    /// Execution Parameters
    parameters: &'a ExecutionParameters,
}

impl<'a> TransformationN3Default<'a> {
    /// Create a new [TransformationN3Default].
    pub fn new(parameters: &'a ExecutionParameters) -> Self {
        Self { parameters }
    }
}

impl ProgramTransformation for TransformationN3Default<'_> {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let commit = program.fork_full();

        commit
            .submit()?
            .transform(TransformationN3SplitTriples::default())?
            .transform(TransformationDefault::new(self.parameters))
    }
}
