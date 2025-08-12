//! This module defines [ProgramTransformation]s.

pub mod active;
pub mod default;
pub mod exports;
pub mod global;
pub mod skolem;
pub mod split;
pub mod validate;
pub mod projection_pushing;
pub mod filter_pushing;

use crate::rule_model::{error::ValidationReport, programs::handle::ProgramHandle};

/// Trait that defines a program transformation
pub trait ProgramTransformation {
    /// Apply the transformation.
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport>;
}
