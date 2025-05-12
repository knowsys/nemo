//! This module defines [Origin].

use super::{
    components::{rule::Rule, ComponentDuplicate, ProgramComponent},
    pipeline::id::ProgramComponentId,
};

/// Origin of a [super::components::ProgramComponent]
#[derive(Debug, Clone)]
pub enum Origin {
    /// Component has no special origin
    Created,
    /// Component was created by parsing a file
    File {
        /// Start position of the character
        start: usize,
        /// End position of the character
        end: usize,
    },

    /// Reference to another object by id
    Reference(ProgramComponentId),
    /// Component was created from another component
    Component(Box<dyn ProgramComponent>),

    /// Combination of two rules
    RuleCombination(Box<Origin>, Box<Origin>),
}

impl Default for Origin {
    fn default() -> Self {
        Self::Created
    }
}

impl Origin {
    /// Create an [Origin] that ... combination
    pub fn rule_combination(first: &Rule, second: &Rule) -> Self {
        let first_origin = first.duplicated_origin();
        let second_origin = second.duplicated_origin();

        Self::RuleCombination(Box::new(first_origin), Box::new(second_origin))
    }
}
