//! This module defines [Rule].

use std::{fmt::Display, io::SeekFrom};

use crate::model::{
    origin::Origin,
    pipeline::{
        address::{AddressSegment, Addressable},
        id::ProgramComponentId,
        ProgramPipeline,
    },
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

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl Addressable for Rule {
    fn next_component(&self, segment: &AddressSegment) -> Option<Box<&dyn Addressable>> {
        Some(Box::new(match segment {
            AddressSegment::Head(index) => self.head_atom(*index),
            AddressSegment::Body(index) => self.body_atom(*index),
            _ => return None,
        }))
    }

    fn address_atom(&self, segment: &AddressSegment) -> Option<&Atom> {
        Some(match segment {
            AddressSegment::Head(index) => self.head_atom(*index),
            AddressSegment::Body(index) => self.body_atom(*index),
            _ => return None,
        })
    }
}

// TODO: Decide on how to set id and origin

impl Rule {
    pub(crate) fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl ProgramPipeline {
    pub(crate) fn set_id_rule(rule: &mut Rule, id: ProgramComponentId) {
        rule.id = id;
    }
}
