//! Import and export directives are a direct representation of the syntactic information
//! given in rule files.

use nemo_physical::resource::Resource;

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

impl RdfVariant {
    /// Create an appropriate format variant from the given resource,
    /// based on the file extension.
    pub(crate) fn from_resource(resource: &Resource) -> RdfVariant {
        match resource {
            resource if Self::is_turtle(resource) => RdfVariant::Turtle,
            resource if Self::is_rdf_xml(resource) => RdfVariant::RDFXML,
            resource if Self::is_nt(resource) => RdfVariant::NTriples,
            resource if Self::is_nq(resource) => RdfVariant::NQuads,
            resource if Self::is_trig(resource) => RdfVariant::TriG,
            _ => RdfVariant::Unspecified,
        }
    }

    fn is_turtle(resource: &Resource) -> bool {
        resource.ends_with(".ttl.gz") || resource.ends_with(".ttl")
    }

    fn is_rdf_xml(resource: &Resource) -> bool {
        resource.ends_with(".rdf.gz") || resource.ends_with(".rdf")
    }

    fn is_nt(resource: &Resource) -> bool {
        resource.ends_with(".nt.gz") || resource.ends_with(".nt")
    }

    fn is_nq(resource: &Resource) -> bool {
        resource.ends_with(".nq.gz") || resource.ends_with(".nq")
    }

    fn is_trig(resource: &Resource) -> bool {
        resource.ends_with(".trig.gz") || resource.ends_with(".trig")
    }
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
