//! This module defines [Atom].

use std::fmt::Display;

use crate::model::{origin::Origin, pipeline::id::ProgramComponentId};

use super::{
    ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent, ProgramComponentKind,
};

/// Atom  
#[derive(Debug, Clone)]
pub struct Atom {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Predicate
    predicate: String,
    /// Terms
    terms: Vec<String>,
}

impl Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ComponentBehavior for Atom {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Atom
    }

    fn validate(&self) -> Result<(), super::NewValidationError> {
        todo!()
    }
}

impl ComponentIdentity for Atom {
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

impl IterableComponent for Atom {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty::<&'a dyn ProgramComponent>())
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty::<&'a mut dyn ProgramComponent>())
    }
}

impl ProgramComponent for Atom {}
