//! The input and output formats supported by Nemo.

pub mod dsv;
pub mod json;
pub mod rdf;
pub mod sparql;

use core::fmt;
use oxiri::Iri;
use std::{
    fmt::Debug,
    io::{Read, Write},
    sync::Arc,
};

use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::AnyDataValue,
    resource::{HttpParameters, Resource},
};

use crate::error::Error;
use crate::rule_model::error::validation_error::ValidationErrorKind;

use super::{compression_format::CompressionFormat, format_builder::FormatBuilder};

const PROGRESS_NOTIFY_INCREMENT: u64 = 10_000_000;

/// Metadata associated with an imported/exported file
pub trait FileFormatMeta: Debug {
    /// The MIME type for this particular format.
    fn media_type(&self) -> String;

    /// Returns the default file extension for data of this format, if any.
    /// This will be used when making default file names.
    fn default_extension(&self) -> String;
}

/// A file format handler, that can read from a [`Read`] providing a [`TableProvider`]
pub trait ImportHandler: FileFormatMeta {
    /// Obtain a [TableProvider] for this format and the given reader, if supported.
    ///
    /// If reading is not supported, an error will be returned.
    fn reader(&self, read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, Error>;
}

/// A file format handler, that can write by wrapping a [`Write`] into a [`TableWriter`]
pub trait ExportHandler: FileFormatMeta {
    /// Obtain a [TableWriter] for this format and the given writer, if supported.
    ///
    /// If writing is not supported, an error will be returned.
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error>;
}

/// Representation of a resource (file, URL, etc.) for import or export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceSpec {
    /// A concrete resource string.
    Resource(Resource),
    /// Use stdout (only for export)
    Stdout,
}

impl ResourceSpec {
    /// Convert a simple [String] into a [ResourceSpec]
    pub(crate) fn from_string(string: String) -> Self {
        if string.is_empty() {
            Self::Stdout
        } else {
            Self::Resource(Resource::Path(string))
        }
    }

    /// Parse and valiate [String] to a [ResourceSpec].
    pub(crate) fn parse_string(string: String) -> Result<Self, ValidationErrorKind> {
        match string {
            // Convert the web-resource into a valid Iri
            string if string.starts_with("http:") || string.starts_with("https:") => {
                Iri::parse(string)
                    .map(|iri| {
                        Self::Resource(Resource::Iri {
                            iri,
                            parameters: HttpParameters::default(),
                        })
                    })
                    .map_err(|_| ValidationErrorKind::ImportExportInvalidIri)
            }

            // Strip a local iri of the prefix
            string if string.starts_with("file://") => string
                .strip_prefix("file://localhost")
                .or_else(|| string.strip_prefix("file://"))
                .map(|short_path| Self::Resource(Resource::Path(String::from(short_path))))
                .ok_or(ValidationErrorKind::ImportExportInvalidIri),
            _ => Ok(Self::from_string(string)),
        }
    }

    /// Convert a [String] to a [ResourceSpec].
    pub(crate) fn from_endpoint(endpoint: Iri<String>, query: String) -> Self {
        Self::Resource(Resource::Iri {
            iri: endpoint,
            parameters: HttpParameters { query: Some(query) },
        })
    }

    /// Retrieve the contained resource, if any.
    pub(crate) fn resource(&self) -> Option<Resource> {
        if let ResourceSpec::Resource(resource) = &self {
            Some(resource.clone())
        } else {
            None
        }
    }

    /// Will this stream be directed to the standard output
    pub fn is_stdout(&self) -> bool {
        matches!(self, Self::Stdout)
    }
}

impl fmt::Display for ResourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceSpec::Resource(resource) => f.write_str(resource.to_string().as_str()),
            ResourceSpec::Stdout => f.write_str("stdout"),
        }
    }
}

#[derive(Debug, Clone)]
/// Holds all information needed to perform an import or export operation.
pub struct FileHandler<H> {
    /// The resource which locates the file
    pub(super) resource_spec: ResourceSpec,
    /// The compression format used when importing
    pub(super) compression: CompressionFormat,
    /// The arity of the predicate related to this directive
    pub(super) predicate_arity: usize,
    /// The handler to be called
    pub(super) handler: H,
}

impl<H> FileHandler<H> {
    /// Construct a new [`FileHandler`]
    pub fn new(
        resource_spec: ResourceSpec,
        compression: CompressionFormat,
        predicate_arity: usize,
        handler: H,
    ) -> Self {
        FileHandler {
            resource_spec,
            compression,
            predicate_arity,
            handler,
        }
    }
}

/// File handler for imports
pub type Import = FileHandler<Arc<dyn ImportHandler + Send + Sync>>;

/// File handler for exports
pub type Export = FileHandler<Arc<dyn ExportHandler + Send + Sync>>;

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct MockHandler;

#[cfg(test)]
impl FileFormatMeta for MockHandler {
    fn media_type(&self) -> String {
        unimplemented!("MockHandler::media_type is a stub")
    }

    fn default_extension(&self) -> String {
        unimplemented!("MockHandler::default_extension is a stub")
    }
}

#[cfg(test)]
impl ImportHandler for MockHandler {
    fn reader(&self, _read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, Error> {
        unimplemented!("MockHandler::reader is a stub")
    }
}

#[cfg(test)]
impl ExportHandler for MockHandler {
    fn writer(&self, _writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        unimplemented!("MockHandler::writer is a stub")
    }
}

impl<T: FileFormatMeta + ?Sized + Send + Sync> FileFormatMeta for Arc<T> {
    fn media_type(&self) -> String {
        T::media_type(self)
    }

    fn default_extension(&self) -> String {
        T::default_extension(self)
    }
}

impl<H> FileHandler<H> {
    /// The resource that will be read from / written to
    pub fn resource_spec(&self) -> &ResourceSpec {
        &self.resource_spec
    }

    /// The compression format that will be applied to this stream
    pub fn compression_format(&self) -> &CompressionFormat {
        &self.compression
    }

    /// Is compression/decompression being applied to this stream
    pub fn is_compressed(&self) -> bool {
        !matches!(self.compression_format(), CompressionFormat::None)
    }

    /// Returns the expected arity of the predicate related to this directive.
    ///
    /// For import, this is the arity of the data that is created, for export it is the
    /// arity of the data that is consumed.
    pub fn predicate_arity(&self) -> usize {
        self.predicate_arity
    }
}

impl<H: FileFormatMeta> FileFormatMeta for FileHandler<H> {
    fn media_type(&self) -> String {
        let Some(addition) = self.compression_format().media_type_addition() else {
            return self.handler.media_type();
        };

        format!("{}+{}", self.handler.media_type(), addition)
    }

    fn default_extension(&self) -> String {
        let Some(addition) = self.compression_format().extension() else {
            return self.handler.default_extension();
        };

        format!("{}.{}", self.handler.default_extension(), addition)
    }
}

impl ImportHandler for Import {
    fn reader(&self, read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, Error> {
        let read = self.compression.implementation().decompress(read)?;
        self.handler.reader(read)
    }
}

impl ExportHandler for Export {
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        let writer = self.compression.implementation().compress(writer);
        self.handler.writer(writer)
    }
}

/// A trait for exporting table data, e.g., to some file.
pub trait TableWriter {
    /// Export a table.
    fn export_table_data<'a>(
        self: Box<Self>,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error>;
}
