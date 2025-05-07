//! This module defines [ProgramTransformation]s.

pub mod active;
pub mod global;
pub mod skolem;
pub mod split;

use super::{commit::ProgramCommit, state::ExtendStatementValidity, ProgramPipeline};

/// Trait
pub trait ProgramTransformation {
    /// Define which program statements to keep from the last commit.
    fn keep(&self) -> ExtendStatementValidity;

    /// Apply the transformation.
    fn apply(&mut self, pipeline: &ProgramPipeline);

    /// Finalize the transformation, returning the [ProgramCommit].
    fn finalize(self) -> ProgramCommit;
}
