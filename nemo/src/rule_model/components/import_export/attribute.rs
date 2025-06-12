//! This module defines [ImportExportAttribute].

use crate::rule_model::{
    components::{ProgramComponent, ProgramComponentKind},
    error::ValidationErrorBuilder,
    origin::Origin,
};

/// Attribute value pairs defining import or export parameters
#[derive(Debug, Clone, Eq)]
pub struct ImportExportAttribute {
    /// Origin of this component
    origin: Origin,

    /// The attribute
    attribute: String,
}

impl ImportExportAttribute {
    /// Create a new [ImportExportAttribute].
    pub fn new(attribute: String) -> Self {
        Self {
            origin: Origin::Created,
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

impl ProgramComponent for ImportExportAttribute {
    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Attribute
    }
}
