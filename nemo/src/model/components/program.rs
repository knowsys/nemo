//! This module defines [Program].

use std::fmt::Display;

use crate::model::{origin::Origin, pipeline::id::ProgramComponentId};

use super::{
    atom::Atom, rule::Rule, ComponentBehavior, ComponentIdentity, IterableComponent,
    ProgramComponent, ProgramComponentKind,
};

#[derive(Debug, Clone)]
pub struct Program {
    /// Origin of this component
    origin: Origin,

    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Atom>,
}

impl Display for Program {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Program {
    pub fn empty() -> Self {
        Self {
            origin: Origin::default(),
            rules: Vec::default(),
            facts: Vec::default(),
        }
    }

    /// Return the [Rule] at the given position.
    pub fn rule(&self, index: usize) -> &Rule {
        &self.rules[index]
    }

    /// Return the [Atom] at the given position
    pub fn fact(&self, index: usize) -> &Atom {
        &self.facts[index]
    }
}

impl ComponentBehavior for Program {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Atom
    }

    fn validate(&self) -> Result<(), super::NewValidationError> {
        todo!()
    }
}

impl ComponentIdentity for Program {
    fn id(&self) -> ProgramComponentId {
        ProgramComponentId::UNASSIGNED
    }

    fn set_id(&mut self, _id: ProgramComponentId) {}

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin
    }
}

impl IterableComponent for Program {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(self.rules.iter().map(|rule| {
            let rule: &dyn ProgramComponent = rule;
            rule
        }))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        todo!()
    }
}
