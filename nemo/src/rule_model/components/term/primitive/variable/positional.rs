//! This module defines [UniversalVariable].

use std::{fmt::Display, hash::Hash};

use crate::{
    rule_model::{
        components::{
            ComponentBehavior, ComponentIdentity, ComponentSource,
            IterableComponent, ProgramComponent, ProgramComponentKind,
        },
        error::ValidationReport,
        origin::Origin,
        pipeline::id::ProgramComponentId,
    },
};

/// Positional marker
///
/// Represents a marker for a predicate position
#[derive(Debug, Clone)]
pub struct PositionalMarker {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// Marked position
    position: usize,
}

impl PositionalMarker {
    /// Create a new [PositionalMarker].
    pub fn new(position: usize) -> Self {
        Self {
            origin: Origin::default(),
            id: ProgramComponentId::default(),
            position,
        }
    }

    /// Return the marked position
    pub fn position(&self) -> usize {
        self.position
    }
}

impl Display for PositionalMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pos = self.position;
        write!(f, "@{pos}")
    }
}

impl PartialEq for PositionalMarker {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}

impl Eq for PositionalMarker {}

impl PartialOrd for PositionalMarker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.position.partial_cmp(&other.position)
    }
}

impl Hash for PositionalMarker {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.position.hash(state);
    }
}

impl ComponentBehavior for PositionalMarker {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Variable
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let report = ValidationReport::default();
        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for PositionalMarker {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for PositionalMarker {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for PositionalMarker {}
