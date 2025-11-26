//! This module defines [TransformationMergeSparql].

use crate::rule_model::{error::ValidationReport, programs::handle::ProgramHandle};

use super::ProgramTransformation;

#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationMergeSparql {}

impl ProgramTransformation for TransformationMergeSparql {
    fn apply(mut self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        // find pure EDBs
        // merge queries

        todo!()
    }
}
