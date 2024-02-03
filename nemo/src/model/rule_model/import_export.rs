//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use crate::model::{Identifier, Map};

/// An import/export specification. This object captures all information that is typically
/// present in an import or export directive in a Nemo program, including the main format,
/// optional attributes that define additional parameters, and an indentifier to map the data
/// to or from (i.e., a predicate name).
#[derive(Clone, Debug)]
pub(crate) struct ImportExportDirective {
    /// The predicate we're handling.
    pub(crate) predicate: Identifier,
    /// The file format and resource we're using.
    pub(crate) format: FileFormat,
    /// The attributes we've been given.
    pub(crate) attributes: Map,
}

impl PartialEq for ImportExportDirective {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.format == other.format
            && self.attributes == other.attributes
    }
}

impl Eq for ImportExportDirective {}

/// An import specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportDirective(pub(crate) ImportExportDirective);

impl ImportDirective {
    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
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

/// An export specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportDirective(pub(crate) ImportExportDirective);

impl ExportDirective {
    /// Return the predicate.
    pub fn predicate(&self) -> &Identifier {
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

    /// Obtain a default [ExportDirective] for the given predicate.
    pub fn default(predicate: Identifier) -> ExportDirective {
        ExportDirective(ImportExportDirective {
            format: FileFormat::CSV,
            predicate: predicate,
            attributes: Map::new(),
        })
    }
}

impl From<ImportExportDirective> for ExportDirective {
    fn from(value: ImportExportDirective) -> Self {
        Self(value)
    }
}

/// The different supported variants of the RDF format.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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

impl std::fmt::Display for RdfVariant {
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
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileFormat {
    /// Comma-separated values
    CSV,
    /// Delimiter-separated values
    DSV,
    /// Tab-separated values
    TSV,
    /// RDF Triples or Quads, with the given format variant.
    RDF(RdfVariant),
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DSV => write!(f, "DSV"),
            Self::CSV => write!(f, "CSV"),
            Self::TSV => write!(f, "TSV"),
            Self::RDF(variant) => write!(f, "{variant}"),
        }
    }
}
