//! Handler for resources of type RDF (Rsource Description Format).
use std::io::{BufRead, Write};

use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::{AnyDataValue, DataValueCreationError, MapDataValue},
    resource::Resource,
};

use oxiri::Iri;

use crate::{
    error::Error,
    io::{
        compression_format::CompressionFormat,
        formats::types::{Direction, TableWriter},
    },
    model::{
        FileFormat, RdfVariant, PARAMETER_NAME_BASE, PARAMETER_NAME_COMPRESSION,
        PARAMETER_NAME_FORMAT, PARAMETER_NAME_LIMIT, PARAMETER_NAME_RESOURCE, VALUE_FORMAT_ANY,
        VALUE_FORMAT_SKIP,
    },
};

use super::{
    import_export::{
        ImportExportError, ImportExportHandler, ImportExportHandlers, ImportExportResource,
    },
    rdf_reader::RdfReader,
    rdf_writer::RdfWriter,
};

use thiserror::Error;

/// Errors that can occur when reading/writing RDF resources and converting them
/// to/from [`AnyDataValue`]s.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum RdfFormatError {
    /// A problem occurred in converting an RDF term to a data value.
    #[error(transparent)]
    DataValueConversion(#[from] DataValueCreationError),
    /// Error of encountering unsupported value in subject position
    #[error("values used as subjects of RDF triples must be IRIs or nulls")]
    RdfInvalidSubject,
    /// Error of encountering RDF* features in data
    #[error("RDF* terms are not supported")]
    RdfStarUnsupported,
    /// Error in Rio's Turtle parser
    #[error(transparent)]
    RioTurtle(#[from] rio_turtle::TurtleError),
    /// Error in Rio's RDF/XML parser
    #[error(transparent)]
    RioXml(#[from] rio_xml::RdfXmlError),
    /// Unable to determine RDF format.
    #[error("could not determine which RDF parser to use for resource {0}")]
    UnknownRdfFormat(Resource),
}

/// Enum for the value formats that are supported for RDF. In many cases,
/// RDF defines how formatting should be done, so there is not much to select here.
///
/// Note that, irrespective of the format, RDF restricts the terms that are
/// allowed in subject, predicate, and graph name positions, and only such terms
/// will be handled there (others are dropped silently).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RdfValueFormat {
    /// General format that accepts any RDF term.
    Anything,
    /// Special format to indicate that the value should be skipped as if the whole
    /// column where not there.
    Skip,
}
impl RdfValueFormat {
    /// Try to convert a string name for a value format to one of the supported
    /// RDF value formats, or return an error for unsupported formats.
    pub(super) fn from_string(name: &str) -> Result<Self, ImportExportError> {
        match name {
            VALUE_FORMAT_ANY => Ok(RdfValueFormat::Anything),
            VALUE_FORMAT_SKIP => Ok(RdfValueFormat::Skip),
            _ => Err(ImportExportError::InvalidValueFormat {
                value_format: name.to_string(),
                format: FileFormat::RDF(RdfVariant::Unspecified),
            }),
        }
    }
}

/// An [ImportExportHandler] for RDF formats.
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
    /// Since the arity is known for
    value_formats: Option<Vec<RdfValueFormat>>,
    /// Maximum number of statements that should be imported/exported.
    limit: Option<u64>,
    /// Compression format to be used, if specified. This can also be inferred
    /// from the resource, if given. So the only case where `None` is possible
    /// is when no resource is given (during output).
    compression_format: Option<CompressionFormat>,
    /// Direction of the operation.
    direction: Direction,
}

impl RdfHandler {
    /// Construct an RDF handler of the given variant.
    pub(crate) fn try_new(
        variant: RdfVariant,
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // Basic checks for unsupported attributes:
        ImportExportHandlers::check_attributes(
            attributes,
            &[
                PARAMETER_NAME_RESOURCE,
                PARAMETER_NAME_BASE,
                PARAMETER_NAME_COMPRESSION,
                PARAMETER_NAME_FORMAT,
                PARAMETER_NAME_LIMIT,
            ],
        )?;

        let resource = ImportExportHandlers::extract_resource(attributes, direction)?;

        let base: Option<Iri<String>>;
        if let Some(base_string) =
            ImportExportHandlers::extract_iri(attributes, PARAMETER_NAME_BASE, true)?
        {
            if let Ok(b) = Iri::parse(base_string.clone()) {
                // TODO: Export should not accept base as parameter, since we cannot use it
                base = Some(b);
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_BASE,
                    AnyDataValue::new_iri(base_string.clone()),
                    "must be a valid IRI",
                ));
            }
        } else {
            base = None;
        }

        let (compression_format, inner_resource) =
            ImportExportHandlers::extract_compression_format(attributes, &resource)?;

        let refined_variant: RdfVariant;
        if variant == RdfVariant::Unspecified {
            if let Some(ref res) = inner_resource {
                refined_variant = Self::rdf_variant_from_resource(res);
            } else {
                // We can still guess a default format based on the arity
                // information provided on import/export:
                refined_variant = RdfVariant::Unspecified;
            }
        } else {
            refined_variant = variant;
        }

        let value_formats = Self::extract_value_formats(attributes, refined_variant, direction)?;
        let limit =
            ImportExportHandlers::extract_unsigned_integer(attributes, PARAMETER_NAME_LIMIT, true)?;

        Ok(Box::new(Self {
            resource,
            base,
            variant: refined_variant,
            value_formats,
            limit,
            compression_format,
            direction,
        }))
    }

    fn extract_value_formats(
        attributes: &MapDataValue,
        variant: RdfVariant,
        direction: Direction,
    ) -> Result<Option<Vec<RdfValueFormat>>, ImportExportError> {
        // Input arity for known formats:
        let arity = match variant {
            RdfVariant::Unspecified => None,
            RdfVariant::NTriples | RdfVariant::Turtle | RdfVariant::RDFXML => Some(3),
            RdfVariant::NQuads | RdfVariant::TriG => Some(4),
        };

        let value_format_strings =
            ImportExportHandlers::extract_value_format_strings_with_file_arity(
                attributes, arity, direction,
            )?;

        if let Some(format_strings) = value_format_strings {
            Ok(Some(Self::formats_from_strings(format_strings)?))
        } else {
            Ok(None)
        }
    }

    fn formats_from_strings(
        value_format_strings: Vec<String>,
    ) -> Result<Vec<RdfValueFormat>, ImportExportError> {
        let mut value_formats = Vec::with_capacity(value_format_strings.len());
        for s in value_format_strings {
            value_formats.push(RdfValueFormat::from_string(s.as_str())?);
        }
        Ok(value_formats)
    }

    /// Extract [RdfVariant] from file extension. The resource should already
    /// have been stripped of any compression-related extensions.
    fn rdf_variant_from_resource(resource: &Resource) -> RdfVariant {
        match resource {
            resource if resource.ends_with(".ttl") => RdfVariant::Turtle,
            resource if resource.ends_with(".rdf") => RdfVariant::RDFXML,
            resource if resource.ends_with(".nt") => RdfVariant::NTriples,
            resource if resource.ends_with(".nq") => RdfVariant::NQuads,
            resource if resource.ends_with(".trig") => RdfVariant::TriG,
            _ => RdfVariant::Unspecified,
        }
    }

    /// Returns the set RDF variant, or finds a default value based on the
    /// required arity. An error occurs if the arity is not compatible with
    /// any variant of RDF.
    fn rdf_variant_or_default(&self, arity: usize) -> Result<RdfVariant, ImportExportError> {
        if self.variant == RdfVariant::Unspecified {
            match arity {
                3 => Ok(RdfVariant::NTriples),
                4 => Ok(RdfVariant::NQuads),
                _ => Err(ImportExportError::InvalidArity { arity, expected: 3 }),
            }
        } else {
            Ok(self.variant)
        }
    }

    /// Returns the set value formats, or finds a default value based on the
    /// required arity.
    fn value_formats_or_default(&self, arity: usize) -> Vec<RdfValueFormat> {
        self.value_formats.clone().unwrap_or_else(|| {
            Self::formats_from_strings(ImportExportHandlers::default_value_format_strings(arity))
                .unwrap()
        })
    }
}

impl ImportExportHandler for RdfHandler {
    fn file_format(&self) -> FileFormat {
        FileFormat::RDF(self.variant)
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        arity: usize,
    ) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(RdfReader::new(
            read,
            self.rdf_variant_or_default(arity)?,
            self.base.clone(),
            self.value_formats_or_default(arity),
            self.limit,
        )))
    }

    fn writer(&self, writer: Box<dyn Write>, arity: usize) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(RdfWriter::new(
            writer,
            self.rdf_variant_or_default(arity)?,
            self.value_formats_or_default(arity),
            self.limit,
        )))
    }

    fn predicate_arity(&self) -> Option<usize> {
        // Our extraction ensures that there is always a suitable default
        // list of value formats if we know the RDF variant.
        match self.direction {
            Direction::Import => self.value_formats.as_ref().map(|vfs| {
                vfs.iter().fold(0, |acc, fmt| {
                    if *fmt == RdfValueFormat::Skip {
                        acc
                    } else {
                        acc + 1
                    }
                })
            }),
            Direction::Export => self.value_formats.as_ref().map(|vfs| vfs.len()),
        }
    }

    fn file_extension(&self) -> Option<String> {
        match self.variant {
            RdfVariant::Unspecified => None,
            RdfVariant::NTriples => Some("nt".to_string()),
            RdfVariant::NQuads => Some("nq".to_string()),
            RdfVariant::Turtle => Some("ttl".to_string()),
            RdfVariant::RDFXML => Some("rdf".to_string()),
            RdfVariant::TriG => Some("trig".to_string()),
        }
    }

    fn compression_format(&self) -> Option<CompressionFormat> {
        self.compression_format
    }

    fn import_export_resource(&self) -> &ImportExportResource {
        &self.resource
    }
}
