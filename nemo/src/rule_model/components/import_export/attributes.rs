//! This module defines [ImportExportAttribute]s used to specify details about
#![allow(missing_docs)]

use std::{fmt::Display, hash::Hash};

use enum_assoc::Assoc;

use crate::syntax::import_export::attribute;

/// Supported attributes in import/export directives
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ImportExportAttribute {
    /// Location of the file
    #[assoc(name = attribute::RESOURCE)]
    Resource,
    /// Data types of the input relations
    #[assoc(name = attribute::FORMAT)]
    Format,
    /// Base IRI
    #[assoc(name = attribute::BASE)]
    Base,
    /// Delimiter used to separate values
    #[assoc(name = attribute::DSV_DELIMITER)]
    Delimiter,
    /// Compression format
    #[assoc(name = attribute::COMPRESSION)]
    Compression,
    /// Limit import/export to first n number of facts
    #[assoc(name = attribute::LIMIT)]
    Limit,
}

impl ImportExportAttribute {}

impl Display for ImportExportAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
