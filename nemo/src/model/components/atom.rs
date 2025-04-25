//! This module defines [Atom].

use std::fmt::Display;

use crate::model::{
    origin::Origin,
    pipeline::{
        address::{AddressSegment, Addressable},
        id::ProgramComponentId,
    },
};

use super::{ProgramComponent, ProgramComponentKind};

/// Atom  
#[derive(Debug)]
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

impl ProgramComponent for Atom {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Atom
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

impl Addressable for Atom {
    fn next_component(&self, _segment: &AddressSegment) -> Option<Box<&dyn Addressable>> {
        None
    }
}
