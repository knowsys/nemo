//! Handler for resources of type RDF (Rsource Description Format).

#![allow(missing_docs)]

pub mod error;
pub(crate) mod reader;
pub(crate) mod value_format;
pub(crate) mod writer;

use std::io::{BufRead, Write};

use enum_assoc::Assoc;
use nemo_physical::datasources::table_providers::TableProvider;

use oxiri::Iri;
use reader::RdfReader;
use value_format::RdfValueFormats;
use writer::RdfWriter;

use crate::{
    error::Error,
    rule_model::components::import_export::{
        compression::CompressionFormat, file_formats::FileFormat,
    },
};

use super::{Direction, ImportExportHandler, ImportExportResource, TableWriter};

/// The different supported variants of the RDF format.
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn file_format(&self) -> FileFormat)]
pub enum RdfVariant {
    /// RDF 1.1 N-Triples
    #[assoc(file_format = FileFormat::NTriples)]
    NTriples,
    /// RDF 1.1 N-Quads
    #[assoc(file_format = FileFormat::NQuads)]
    NQuads,
    /// RDF 1.1 Turtle
    #[assoc(file_format = FileFormat::Turtle)]
    Turtle,
    /// RDF 1.1 RDF/XML
    #[assoc(file_format = FileFormat::RDFXML)]
    RDFXML,
    /// RDF 1.1 TriG
    #[assoc(file_format = FileFormat::TriG)]
    TriG,
}

impl std::fmt::Display for RdfVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.file_format().fmt(f)
    }
}

/// A handler for RDF formats.
#[derive(Debug, Clone)]
pub struct RdfHandler {
    /// The resource to write to/read from.
    /// This can be [ImportExportResource::Unspecified] for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so a concrete value is required there.
    resource: ImportExportResource,
    /// Base IRI, if given.
    base: Option<Iri<String>>,
    /// The specific RDF format to be used.
    variant: RdfVariant,
    /// The list of value formats to be used for importing/exporting data.
    value_formats: RdfValueFormats,
    /// Maximum number of statements that should be imported/exported.
    limit: Option<u64>,
    /// Compression format to be used
    compression_format: CompressionFormat,
    /// Direction of the operation.
    _direction: Direction,
}

impl RdfHandler {
    /// Create a new [RdfHandler].
    pub fn new(
        resource: ImportExportResource,
        base: Option<Iri<String>>,
        variant: RdfVariant,
        value_formats: RdfValueFormats,
        limit: Option<u64>,
        compression_format: CompressionFormat,
        _direction: Direction,
    ) -> Self {
        Self {
            resource,
            base,
            variant,
            value_formats,
            limit,
            compression_format,
            _direction,
        }
    }
}

impl ImportExportHandler for RdfHandler {
    fn reader(&self, read: Box<dyn BufRead>) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(RdfReader::new(
            read,
            self.variant,
            self.base.clone(),
            self.value_formats.clone(),
            self.limit,
        )))
    }

    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(RdfWriter::new(
            writer,
            self.variant,
            self.value_formats.clone(),
            self.limit,
        )))
    }

    fn predicate_arity(&self) -> usize {
        self.value_formats.arity()
    }

    fn file_extension(&self) -> String {
        self.variant.file_format().extension().to_string()
    }

    fn compression_format(&self) -> CompressionFormat {
        self.compression_format
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        &self.resource
    }

    fn file_format(&self) -> FileFormat {
        self.variant.file_format()
    }
}
