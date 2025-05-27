//! This module defines [ProgramTransformation]s.

pub mod active;
pub mod default;
pub mod exports;
pub mod global;
pub mod skolem;
pub mod split;

use crate::rule_model::error::ValidationReport;

use super::{commit::ProgramCommit, state::ExtendStatementValidity, ProgramPipeline};

/// Trait that defines a program transformation
pub trait ProgramTransformation {
    /// Define which program statements to keep from the last commit.
    fn keep(&self) -> ExtendStatementValidity;

    /// Apply the transformation.
    fn apply(
        self,
        commit: &mut ProgramCommit,
        report: &mut ValidationReport,
        pipeline: &ProgramPipeline,
    );
}
