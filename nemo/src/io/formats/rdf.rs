//! Handler for resources of type RDF (Rsource Description Format).
use std::io::{BufRead, Write};

use nemo_physical::{datasources::table_providers::TableProvider, resource::Resource};

use oxiri::Iri;

use crate::{
    error::Error,
    io::{
        compression_format::CompressionFormat,
        formats::types::{Direction, TableWriter},
    },
    model::{
        Constant, FileFormat, Identifier, Map, RdfVariant, PARAMETER_NAME_BASE,
        PARAMETER_NAME_COMPRESSION, PARAMETER_NAME_RESOURCE,
    },
};

use super::{
    import_export::{ImportExportError, ImportExportHandler, ImportExportHandlers},
    rdf_reader::RdfReader,
};

/// An [ImportExportHandler] for RDF formats.
#[derive(Debug, Default, Clone)]
pub struct RdfHandler {
    /// The resource to write to/read from.
    /// This can be `None` for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so the predicate is setting there.
    resource: Option<Resource>,
    /// Base IRI, if given.
    base: Option<Iri<String>>,
    /// The specific RDF format to be used.
    variant: RdfVariant,
    /// Compression format to be used, if specified. This can also be inferred
    /// from the resource, if given. So the only case where `None` is possible
    /// is when no resource is given (during output).
    compression_format: Option<CompressionFormat>,
}

impl RdfHandler {
    /// Construct an RDF handler of the given variant.
    pub(crate) fn try_new(
        variant: RdfVariant,
        attributes: &Map,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // Basic checks for unsupported attributes:
        ImportExportHandlers::check_attributes(
            attributes,
            &vec![
                PARAMETER_NAME_RESOURCE,
                PARAMETER_NAME_BASE,
                PARAMETER_NAME_COMPRESSION,
            ],
        )?;

        let resource = ImportExportHandlers::extract_resource(attributes, direction)?;

        let base: Option<Iri<String>>;
        if let Some(base_string) =
            ImportExportHandlers::extract_iri(attributes, PARAMETER_NAME_BASE, true)?
        {
            if let Ok(b) = Iri::parse(base_string.clone()) {
                base = Some(b);
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_BASE,
                    Constant::Abstract(Identifier::new(base_string.clone())),
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
                refined_variant = variant;
            }
        } else {
            refined_variant = variant;
        }

        Ok(Box::new(Self {
            resource: resource,
            base: base,
            variant: refined_variant,
            compression_format: compression_format,
        }))
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
}

impl ImportExportHandler for RdfHandler {
    fn file_format(&self) -> FileFormat {
        FileFormat::RDF(self.variant)
    }

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        _arity: usize,
    ) -> Result<Box<dyn TableProvider>, Error> {
        // TODO: Arity is ignored. It is only relevant if the RDF variant was unspecified, since it could help to guess the format
        // in this case. But we do not yet do this.
        Ok(Box::new(RdfReader::new(
            read,
            self.variant,
            self.base.clone(),
        )))
    }

    fn writer(
        &self,
        _writer: Box<dyn Write>,
        _arity: usize,
    ) -> Result<Box<dyn TableWriter>, Error> {
        Err(ImportExportError::UnsupportedWrite(self.file_format()).into())
    }

    fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn arity(&self) -> Option<usize> {
        match self.variant {
            RdfVariant::Unspecified => None,
            RdfVariant::NTriples | RdfVariant::Turtle | RdfVariant::RDFXML => Some(3),
            RdfVariant::NQuads | RdfVariant::TriG => Some(4),
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
}
