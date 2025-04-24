//! This module defines [Rule].

use std::fmt::Display;

use crate::model::{
    origin::Origin,
    pipeline::{id::ProgramComponentId, ProgramPipeline},
};

use super::{atom::Atom, ProgramComponent, ProgramComponentKind};

#[derive(Debug)]
pub struct Rule {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Atom>,
}

impl Rule {
    /// Create a new empty [Rule].
    pub fn empty() -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            head: Vec::default(),
            body: Vec::default(),
        }
    }

    /// Create a new [Rule].
    pub fn new(head: Vec<Atom>, body: Vec<Atom>) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            head,
            body,
        }
    }

    /// Create a new empty [Rule],
    /// which combines two existing rules
    pub fn new_combined(first: &Rule, second: &Rule) -> Self {
        let mut empty_rule = Self::empty();

        empty_rule.origin = Origin::rule_combination(first, second);
        empty_rule
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramComponent for Rule {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Rule
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn validate(&self) -> Result<(), super::NewValidationError> {
        todo!()
    }
}
