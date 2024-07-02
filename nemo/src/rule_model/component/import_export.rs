//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use std::{fmt::Display, hash::Hash};

use nemo_physical::datavalues::MapDataValue;

use crate::rule_model::origin::Origin;

use super::{term::Identifier, ProgramComponent};

/// The different supported variants of the RDF format.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RdfVariant {
    /// An unspecified format, using the resource name as a heuristic.
    #[default]
    Unspecified,
    /// RDF 1.1 N-Triples
    NTriples,
    /// RDF 1.1 N-Quads
    NQuads,
    /// RDF 1.1 Turtle
    Turtle,
    /// RDF 1.1 RDF/XML
    RDFXML,
    /// RDF 1.1 TriG
    TriG,
}

impl Display for RdfVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NTriples => write!(f, "RDF N-Triples"),
            Self::NQuads => write!(f, "RDF N-Quads"),
            Self::Turtle => write!(f, "RDF Turtle"),
            Self::RDFXML => write!(f, "RDF/XML"),
            Self::TriG => write!(f, "RDF TriG"),
            Self::Unspecified => write!(f, "RDF"),
        }
    }
}

/// Supported file formats.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum FileFormat {
    /// Comma-separated values
    CSV,
    /// Delimiter-separated values
    DSV,
    /// Tab-separated values
    TSV,
    /// RDF Triples or Quads, with the given format variant.
    RDF(RdfVariant),
    /// JSON objects
    JSON,
}

impl Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DSV => write!(f, "DSV"),
            Self::CSV => write!(f, "CSV"),
            Self::TSV => write!(f, "TSV"),
            Self::JSON => write!(f, "JSON"),
            Self::RDF(variant) => write!(f, "{variant}"),
        }
    }
}

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Debug, Clone, Eq)]
pub(crate) struct ImportExportDirective {
    /// Origin of this component
    origin: Origin,

    /// The predicate we're handling.
    predicate: Identifier,
    /// The file format and resource we're using.
    format: FileFormat,
    /// The attributes we've been given.
    attributes: MapDataValue,
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
    pub fn new(predicate: Identifier, format: FileFormat, attributes: MapDataValue) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &MapDataValue {
        &self.0.attributes
    }
}

impl From<ImportExportDirective> for ImportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ImportDirective {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramComponent for ImportDirective {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
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

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
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
    pub fn new(predicate: Identifier, format: FileFormat, attributes: MapDataValue) -> Self {
        Self(ImportExportDirective {
            origin: Origin::default(),
            predicate,
            format,
            attributes,
        })
    }

    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
        &self.0.predicate
    }

    /// Return the file format.
    pub fn file_format(&self) -> FileFormat {
        self.0.format
    }

    /// Return the attributes.
    pub fn attributes(&self) -> &MapDataValue {
        &self.0.attributes
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

impl Display for ExportDirective {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramComponent for ExportDirective {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
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

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}
