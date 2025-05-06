//! This module defines [Transformation]s.

use super::{commit::ProgramCommit, state::ExtendStatementValidity};

/// Trait
pub trait Transformation {
    fn keep(&self) -> ExtendStatementValidity;

    fn finalize(self) -> ProgramCommit;
}
