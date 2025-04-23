//! Module for recording the history of program components

use super::manager::ProgramComponentId;

/// Records the history of a given program component
#[derive(Debug, Clone, Copy)]
pub enum Origin {
    /// Component originates from parsing
    Parsed(usize),
    /// Component was created through the API
    Created,
    /// Component is the result of merging two rules
    RuleMerge(ProgramComponentId, ProgramComponentId),
}
