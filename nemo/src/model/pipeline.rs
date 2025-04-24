//! This module defines [ProgramPipeline].

use super::components::rule::Rule;

pub mod address;
pub mod id;

/// Big manager object
#[derive(Debug)]
pub struct ProgramPipeline {
    rules: Vec<Rule>,
}

impl ProgramPipeline {}
