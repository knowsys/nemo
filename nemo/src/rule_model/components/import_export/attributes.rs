//! This module defines [ImportExportAttribute]s used to specify details about
#![allow(missing_docs)]

use std::{fmt::Display, hash::Hash};

use enum_assoc::Assoc;

use crate::rule_model::syntax::import_export::attributes;

/// Supported attributes in import/export directives
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ImportExportAttribute {
    /// Location of the file
    #[assoc(name = attributes::ATTRIBUTE_NAME_RESOURCE)]
    Resource,
    /// Data types of the input relations
    #[assoc(name = attributes::ATTRIBUTE_NAME_FORMAT)]
    Format,
    /// Base IRI
    #[assoc(name = attributes::ATTRIBUTE_NAME_BASE)]
    Base,
    /// Delimiter used to separate values
    #[assoc(name = attributes::ATTRIBUTE_NAME_DSV_DELIMITER)]
    Delimiter,
    /// Compression format
    #[assoc(name = attributes::ATTRIBUTE_NAME_COMPRESSION)]
    Compression,
    /// Limit import/export to first n number of facts
    #[assoc(name = attributes::ATTRIBUTE_NAME_LIMIT)]
    Limit,
}

impl ImportExportAttribute {}

impl Display for ImportExportAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
