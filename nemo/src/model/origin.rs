//! This module defines

use super::{
    components::{rule::Rule, EffectiveOrigin, ProgramComponent},
    pipeline::id::ProgramComponentId,
};

// TODO: The thing which holds the parser should have this
pub(crate) type ExternalReference = usize;

/// Origin of a [super::components::ProgramComponent]
#[derive(Debug, Clone)]
pub enum Origin {
    /// Component has no special origin
    Created,
    /// Reference
    Reference(ProgramComponentId),

    /// Component stems from parsing
    Parsing(ExternalReference),

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
        let first_origin = first.effective_origin();
        let second_origin = second.effective_origin();

        Self::RuleCombination(Box::new(first_origin), Box::new(second_origin))
    }
}
