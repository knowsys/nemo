//! This module defines [Rule].

use std::fmt::Display;

use crate::model::{origin::Origin, pipeline::id::ProgramComponentId};

use super::{
    atom::Atom, ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
    ProgramComponentKind, TryAsRef,
};

#[derive(Debug, Clone)]
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

    /// Return
    pub fn body_atom(&self, index: usize) -> &Atom {
        &self.body[index]
    }

    pub fn head_atom(&self, index: usize) -> &Atom {
        &self.head[index]
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ComponentBehavior for Rule {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Rule
    }

    fn validate(&self) -> Result<(), super::NewValidationError> {
        todo!()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        todo!()
    }
}

impl ComponentIdentity for Rule {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin
    }
}

impl IterableComponent for Rule {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let atom_iter = self.head.iter().chain(self.body.iter());

        Box::new(atom_iter.map(|atom| {
            let component: &dyn ProgramComponent = atom;
            component
        }))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let atom_iter = self.head.iter_mut().chain(self.body.iter_mut());

        Box::new(atom_iter.map(|atom| {
            let component: &mut dyn ProgramComponent = atom;
            component
        }))
    }
}
