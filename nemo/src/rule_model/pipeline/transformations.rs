//! This module defines [ProgramTransformation]s.

pub mod active;
pub mod default;
pub mod exports;
pub mod global;
pub mod skolem;
pub mod split;

use crate::rule_model::error::ValidationReport;

use super::ProgramPipeline;

/// Trait that defines a program transformation
pub trait ProgramTransformation {
    /// Apply the transformation.
    fn apply(self, pipeline: &mut ProgramPipeline, report: &mut ValidationReport);
}
