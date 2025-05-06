//! This module defines [ImportExportAttribute].

use crate::rule_model::{
    components::{
        ComponentBehavior, ComponentIdentity, IterableComponent, ProgramComponent,
        ProgramComponentKind,
    },
    error::ValidationErrorBuilder,
    origin::Origin,
    pipeline::id::ProgramComponentId,
};

/// Attribute value pairs defining import or export parameters
#[derive(Debug, Clone)]
pub struct ImportExportAttribute {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

    /// The attribute
    attribute: String,
}

impl ImportExportAttribute {
    /// Create a new [ImportExportAttribute].
    pub fn new(attribute: String) -> Self {
        Self {
            origin: Origin::Created,
            id: ProgramComponentId::default(),
            attribute,
        }
    }

    /// Return the attribute.
    pub fn value(&self) -> &str {
        &self.attribute
    }
}

impl std::fmt::Display for ImportExportAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.attribute.fmt(f)
    }
}

impl PartialEq for ImportExportAttribute {
    fn eq(&self, other: &Self) -> bool {
        self.attribute == other.attribute
    }
}

impl Eq for ImportExportAttribute {}

impl PartialOrd for ImportExportAttribute {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.attribute.partial_cmp(&other.attribute)
    }
}

impl std::hash::Hash for ImportExportAttribute {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.attribute.hash(state);
    }
}

impl ComponentBehavior for ImportExportAttribute {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Attribute
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()> {
        Some(())
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentIdentity for ImportExportAttribute {
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
        self.origin = origin;
    }
}

impl IterableComponent for ImportExportAttribute {}
