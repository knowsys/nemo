//! The input and output formats supported by Nemo.

pub mod dsv;
pub mod json;
pub mod rdf;

use std::io::{BufRead, Write};

use dyn_clone::DynClone;

use nemo_physical::{
    datasources::table_providers::TableProvider, datavalues::AnyDataValue, resource::Resource,
};

use crate::{
    error::Error,
    rule_model::components::import_export::{
        compression::CompressionFormat, file_formats::FileFormat,
    },
};

const PROGRESS_NOTIFY_INCREMENT: u64 = 10_000_000;

/// Representation of a resource (file, URL, etc.) for import or export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportExportResource {
    /// A concrete resource string.
    Resource(Resource),
    /// Use stdout (only for export)
    Stdout,
}

impl ImportExportResource {
    /// Convert a [String] to a [ImportExportResource].
    pub(crate) fn from_string(string: String) -> Self {
        if string.is_empty() {
            Self::Stdout
        } else {
            Self::Resource(string)
        }
    }

    /// Retrieve the contained resource, if any.
    pub(crate) fn resource(&self) -> Option<Resource> {
        if let ImportExportResource::Resource(resource) = &self {
            Some(resource.clone())
        } else {
            None
        }
    }
}

/// An [ImportExportHandler] represents a data format for input and/or output, and provides
/// specific methods for handling data of that format. Each handler is configured by format-specific
/// attributes, which define the behavior in detail, including the kind of data that this format
/// is compatible with. The attributes are provided when creating the format, and should then
/// be validated.
///
/// An implementation of [ImportExportHandler] provides methods to validate and refine parameters
/// that were used with this format, to create suitable [TableProvider] and [TableWriter] objects
/// to read and write data in the given format, and to report information about the type of
/// data that this format can handle (such as predicate arity and type).
pub trait ImportExportHandler: std::fmt::Debug + DynClone + Send {
    /// Return the associated [FileFormat].
    fn file_format(&self) -> FileFormat;

    /// Obtain a [TableProvider] for this format and the given reader, if supported.
    ///
    /// If reading is not supported, an error will be returned.
    fn reader(&self, read: Box<dyn BufRead>) -> Result<Box<dyn TableProvider>, Error>;

    /// Obtain a [TableWriter] for this format and the given writer, if supported.
    ///
    /// If writing is not supported, an error will be returned.
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error>;

    /// Obtain the resource used for this data exchange.
    ///
    /// In typical cases, this is the name of a file to read from or write to.
    /// If no resource was specified, or if the resource is not identified by a
    /// name (such as stdout), then `None` is returned.
    fn resource(&self) -> Option<Resource> {
        self.import_export_resource().resource()
    }

    /// Returns true if the selected resource is stdout.
    fn resource_is_stdout(&self) -> bool {
        self.import_export_resource() == &ImportExportResource::Stdout
    }

    /// Returns the expected arity of the predicate related to this directive.
    ///
    /// For import, this is the arity of the data that is created, for export it is the
    /// arity of the data that is consumed.
    fn predicate_arity(&self) -> usize;

    /// Returns the default file extension for data of this format, if any.
    /// This will be used when making default file names.
    fn file_extension(&self) -> String;

    /// Returns the chosen compression format for imported/exported data.
    fn compression_format(&self) -> CompressionFormat;

    /// Returns the [ImportExportResource] used for this data exchange.
    fn import_export_resource(&self) -> &ImportExportResource;
}

dyn_clone::clone_trait_object!(ImportExportHandler);

/// Direction of import/export activities.
/// We often share code for the two directions, and a direction
/// is then used to enable smaller distinctions where needed.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Processing input.
    Import,
    /// Processing output.
    Export,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Import => f.write_str("import"),
            Direction::Export => f.write_str("export"),
        }
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
