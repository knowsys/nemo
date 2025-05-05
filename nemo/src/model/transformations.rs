//! This module defines [Transformation].

use super::pipeline::ProgramPipeline;

pub trait Transformation {
    fn name() -> &'static str;

    fn extend() -> usize;

    fn apply(&self, pipeline: &mut ProgramPipeline);
}
