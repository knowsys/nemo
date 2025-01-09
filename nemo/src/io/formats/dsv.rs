//! Handler for resources of type DSV (delimiter-separated values).

pub(crate) mod reader;
pub mod value_format;
pub(crate) mod writer;

use std::io::{BufRead, Write};

use nemo_physical::datasources::table_providers::TableProvider;
use reader::DsvReader;
use value_format::DsvValueFormats;
use writer::DsvWriter;

use crate::{
    error::Error,
    rule_model::components::import_export::{
        compression::CompressionFormat, file_formats::FileFormat,
    },
};

use super::{Direction, ImportExportHandler, ImportExportResource, TableWriter};

/// An [ImportExportHandler] for delimiter-separated values.
#[derive(Debug, Clone)]
pub struct DsvHandler {
    /// The specific delimiter for this format.
    delimiter: u8,
    /// The resource to write to/read from.
    /// This can be [ImportExportResource::Unspecified] for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so a concrete value is required there.
    resource: ImportExportResource,
    /// The list of value formats to be used for importing/exporting data.
    value_formats: DsvValueFormats,
    /// Maximum number of statements that should be imported/exported.
    limit: Option<u64>,
    /// Compression format to be used
    compression_format: CompressionFormat,
    /// Direction of the operation.
    _direction: Direction,
}

impl DsvHandler {
    /// Create a new [DsvHandler].
    pub fn new(
        delimiter: u8,
        resource: ImportExportResource,
        value_formats: DsvValueFormats,
        limit: Option<u64>,
        compression_format: CompressionFormat,
        _direction: Direction,
    ) -> Self {
        Self {
            delimiter,
            resource,
            value_formats,
            limit,
            compression_format,
            _direction,
        }
    }
}

impl ImportExportHandler for DsvHandler {
    fn file_format(&self) -> FileFormat {
        match self.delimiter {
            b',' => FileFormat::CSV,
            b'\t' => FileFormat::TSV,
            _ => FileFormat::DSV,
        }
    }

    fn reader(&self, read: Box<dyn BufRead>) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(DsvReader::new(
            read,
            self.delimiter,
            self.value_formats.clone(),
            None,
            self.limit,
        )))
    }

    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DsvWriter::new(
            self.delimiter,
            writer,
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn format_metadata() {
        for (format, delimiter) in &[
            (FileFormat::DSV, b';'),
            (FileFormat::CSV, b','),
            (FileFormat::TSV, b'\t'),
        ] {
            let handler = DsvHandler::new(
                *delimiter,
                ImportExportResource::from_string(format!("dummy.{}", format.extension())),
                DsvValueFormats::default(3),
                None,
                CompressionFormat::None,
                Direction::Import,
            );

            assert_eq!(format.extension(), handler.file_extension());
            assert_eq!(format.media_type(), handler.file_format().media_type());
            assert_eq!(3, handler.predicate_arity());
        }
    }
}
