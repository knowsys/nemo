//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

pub mod attributes;
pub mod compression;
pub mod file_formats;

use std::{fmt::Display, hash::Hash};

use file_formats::FileFormat;

use crate::rule_model::{error::ValidationErrorBuilder, origin::Origin};

use super::{term::map::Map, ProgramComponent, Tag};

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Debug, Clone, Eq)]
pub(crate) struct ImportExportDirective {
    /// Origin of this component
    origin: Origin,

    /// The predicate we're handling.
    predicate: Tag,
    /// The file format and resource we're using.
    format: FileFormat,
    /// The attributes we've been given.
    attributes: Map,
}

impl ImportExportDirective {
    /// Helper function for the display implementations of
    /// [ImportDirective] and [ExportDirective]
    /// to format the content of this object.
    fn display_content(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} :- {} {} .",
            self.predicate, self.format, self.attributes
        )
    }
}

impl PartialEq for ImportExportDirective {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.format == other.format
            && self.attributes == other.attributes
    }
}

impl Hash for ImportExportDirective {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.format.hash(state);
        self.attributes.hash(state);
    }
}

/// An import specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportDirective(pub(crate) ImportExportDirective);

impl ImportDirective {
    /// Create a new [ImportDirective].
    pub fn new(predicate: Tag, format: FileFormat, attributes: Map) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &Map {
        &self.0.attributes
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ImportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("@import ")?;
        self.0.display_content(f)
    }
}

impl ProgramComponent for ImportDirective {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.0.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}

/// An export specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    /// Create a new [ExportDirective].
    pub fn new(predicate: Tag, format: FileFormat, attributes: Map) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Tag {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &Map {
        &self.0.attributes
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ExportDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("@export ")?;
        self.0.display_content(f)
    }
}

impl ProgramComponent for ExportDirective {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.0.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.0.origin = origin;
        self
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        todo!()
    }
}
