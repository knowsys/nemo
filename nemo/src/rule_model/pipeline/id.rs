//! This module defines [ProgramComponentId].

use std::hash::Hash;

/// Identifies a [super::super::components::ProgramComponent] within a [super::ProgramPipeline]
///
/// The id `Self::UNASSIGNED` represents the [super::super::components::ProgramComponent]
/// not being associated with any [super::ProgramPipeline].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ProgramComponentId(usize);

impl ProgramComponentId {
    /// Indicates that the [super::super::components::ProgramComponent]
    /// does not belong to any [super::ProgramPipeline]
    const UNASSIGNED: Self = Self(usize::MAX);

    /// Create a new [ProgramComponentId].
    pub fn start() -> Self {
        Self(0)
    }

    /// Create a new [ProgramComponentId]
    /// that indicates that the [super::super::components::ProgramComponent]
    /// does not belong to any [super::ProgramPipeline]
    pub fn unassigned() -> Self {
        Self::UNASSIGNED
    }

    /// Increment the id and return its old value.
    pub fn increment(&mut self) -> Self {
        let current = self.clone();

        if *self != Self::UNASSIGNED {
            self.0 += 1;
        }

        current
    }

    /// Returns whether a id has been assigned yet.
    pub fn is_assigned(&self) -> bool {
        *self != Self::UNASSIGNED
    }
}

impl Default for ProgramComponentId {
    fn default() -> Self {
        Self::unassigned()
    }
}

impl PartialOrd for ProgramComponentId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
