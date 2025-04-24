//! This module defines [Atom].

use std::fmt::Display;

use crate::model::{origin::Origin, pipeline::id::ProgramComponentId};

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
}
