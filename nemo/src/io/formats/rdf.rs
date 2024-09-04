//! Handler for resources of type RDF (Rsource Description Format).

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
    direction: Direction,
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
        direction: Direction,
    ) -> Self {
        Self {
            resource,
            base,
            variant,
            value_formats,
            limit,
            compression_format,
            direction,
        }
    }

    // /// Construct an RDF handler of the given variant.
    // pub(crate) fn try_new(
    //     variant: RdfVariant,
    //     attributes: &MapDataValue,
    //     direction: Direction,
    // ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
    //     // Basic checks for unsupported attributes:
    //     ImportExportHandlers::check_attributes(
    //         attributes,
    //         &[
    //             PARAMETER_NAME_RESOURCE,
    //             PARAMETER_NAME_BASE,
    //             PARAMETER_NAME_COMPRESSION,
    //             PARAMETER_NAME_FORMAT,
    //             PARAMETER_NAME_LIMIT,
    //         ],
    //     )?;

    //     let resource = ImportExportHandlers::extract_resource(attributes, direction)?;

    //     let base: Option<Iri<String>>;
    //     if let Some(base_string) =
    //         ImportExportHandlers::extract_iri(attributes, PARAMETER_NAME_BASE, true)?
    //     {
    //         if let Ok(b) = Iri::parse(base_string.clone()) {
    //             // TODO: Export should not accept base as parameter, since we cannot use it
    //             base = Some(b);
    //         } else {
    //             return Err(ImportExportError::invalid_att_value_error(
    //                 PARAMETER_NAME_BASE,
    //                 AnyDataValue::new_iri(base_string.clone()),
    //                 "must be a valid IRI",
    //             ));
    //         }
    //     } else {
    //         base = None;
    //     }

    //     let (compression_format, inner_resource) =
    //         ImportExportHandlers::extract_compression_format(attributes, &resource)?;

    //     let refined_variant: RdfVariant;
    //     if variant == RdfVariant::Unspecified {
    //         if let Some(ref res) = inner_resource {
    //             refined_variant = Self::rdf_variant_from_resource(res);
    //         } else {
    //             // We can still guess a default format based on the arity
    //             // information provided on import/export:
    //             refined_variant = RdfVariant::Unspecified;
    //         }
    //     } else {
    //         refined_variant = variant;
    //     }

    //     let value_formats = Self::extract_value_formats(attributes, refined_variant, direction)?;
    //     let limit =
    //         ImportExportHandlers::extract_unsigned_integer(attributes, PARAMETER_NAME_LIMIT, true)?;

    //     Ok(Box::new(Self {
    //         resource,
    //         base,
    //         variant: refined_variant,
    //         value_formats,
    //         limit,
    //         compression_format,
    //         direction,
    //     }))
    // }

    // fn extract_value_formats(
    //     attributes: &MapDataValue,
    //     variant: RdfVariant,
    //     direction: Direction,
    // ) -> Result<Option<Vec<RdfValueFormat>>, ImportExportError> {
    //     // Input arity for known formats:
    //     let arity = match variant {
    //         RdfVariant::Unspecified => None,
    //         RdfVariant::NTriples | RdfVariant::Turtle | RdfVariant::RDFXML => Some(3),
    //         RdfVariant::NQuads | RdfVariant::TriG => Some(4),
    //     };

    //     let value_format_strings =
    //         ImportExportHandlers::extract_value_format_strings_with_file_arity(
    //             attributes, arity, direction,
    //         )?;

    //     if let Some(format_strings) = value_format_strings {
    //         Ok(Some(Self::formats_from_strings(format_strings)?))
    //     } else {
    //         Ok(None)
    //     }
    // }

    // fn formats_from_strings(
    //     value_format_strings: Vec<String>,
    // ) -> Result<Vec<RdfValueFormat>, ImportExportError> {
    //     let mut value_formats = Vec::with_capacity(value_format_strings.len());
    //     for s in value_format_strings {
    //         value_formats.push(RdfValueFormat::from_string(s.as_str())?);
    //     }
    //     Ok(value_formats)
    // }

    // /// Extract [RdfVariant] from file extension. The resource should already
    // /// have been stripped of any compression-related extensions.
    // fn rdf_variant_from_resource(resource: &Resource) -> RdfVariant {
    //     match resource {
    //         resource if resource.ends_with(".ttl") => RdfVariant::Turtle,
    //         resource if resource.ends_with(".rdf") => RdfVariant::RDFXML,
    //         resource if resource.ends_with(".nt") => RdfVariant::NTriples,
    //         resource if resource.ends_with(".nq") => RdfVariant::NQuads,
    //         resource if resource.ends_with(".trig") => RdfVariant::TriG,
    //         _ => RdfVariant::Unspecified,
    //     }
    // }

    // /// Returns the set RDF variant, or finds a default value based on the
    // /// required arity. An error occurs if the arity is not compatible with
    // /// any variant of RDF.
    // fn rdf_variant_or_default(&self, arity: usize) -> Result<RdfVariant, ImportExportError> {
    //     if self.variant == RdfVariant::Unspecified {
    //         match arity {
    //             3 => Ok(RdfVariant::NTriples),
    //             4 => Ok(RdfVariant::NQuads),
    //             _ => Err(ImportExportError::InvalidArity { arity, expected: 3 }),
    //         }
    //     } else {
    //         Ok(self.variant)
    //     }
    // }

    // /// Returns the set value formats, or finds a default value based on the
    // /// required arity.
    // fn value_formats_or_default(&self, arity: usize) -> Vec<RdfValueFormat> {
    //     self.value_formats.clone().unwrap_or_else(|| {
    //         Self::formats_from_strings(ImportExportHandlers::default_value_format_strings(arity))
    //             .unwrap()
    //     })
    // }
}

impl ImportExportHandler for RdfHandler {
    fn file_format(&self) -> FileFormat {
        self.variant.file_format()
    }

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
        self.file_format().extension().to_string()
    }

    fn compression_format(&self) -> CompressionFormat {
        self.compression_format
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        &self.resource
    }
}
