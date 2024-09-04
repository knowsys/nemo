//! Handler for resources of type DSV (delimiter-separated values).

pub(crate) mod reader;
pub(crate) mod value_format;
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

/// Internal enum to distinguish variants of the DSV format.
#[allow(clippy::upper_case_acronyms)]
enum DsvVariant {
    /// Delimiter-separated values
    DSV,
    /// Comma-separated values
    CSV,
    /// Tab-separated values
    TSV,
}

/// An [ImportExportHandler] for delimiter-separated values.
#[derive(Debug, Clone)]
pub(crate) struct DsvHandler {
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
    direction: Direction,
}

impl DsvHandler {
    /// Create a new [DsvHandler].
    pub(crate) fn new(
        delimiter: u8,
        resource: ImportExportResource,
        value_formats: DsvValueFormats,
        limit: Option<u64>,
        compression_format: CompressionFormat,
        direction: Direction,
    ) -> Self {
        Self {
            delimiter,
            resource,
            value_formats,
            limit,
            compression_format,
            direction,
        }
    }

    // /// Construct a DSV file handler with an arbitrary delimiter.
    // pub(crate) fn try_new_dsv(
    //     attributes: &MapDataValue,
    //     direction: Direction,
    // ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
    //     Self::try_new(DsvVariant::DSV, attributes, direction)
    // }

    // /// Construct a CSV file handler.
    // pub(crate) fn try_new_csv(
    //     attributes: &MapDataValue,
    //     direction: Direction,
    // ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
    //     Self::try_new(DsvVariant::CSV, attributes, direction)
    // }

    // /// Construct a TSV file handler.
    // pub(crate) fn try_new_tsv(
    //     attributes: &MapDataValue,
    //     direction: Direction,
    // ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
    //     Self::try_new(DsvVariant::TSV, attributes, direction)
    // }

    // /// Construct a DSV handler of the given variant.
    // fn try_new(
    //     variant: DsvVariant,
    //     attributes: &MapDataValue,
    //     direction: Direction,
    // ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
    //     // Basic checks for unsupported attributes:
    //     ImportExportHandlers::check_attributes(
    //         attributes,
    //         &[
    //             PARAMETER_NAME_FORMAT,
    //             PARAMETER_NAME_RESOURCE,
    //             PARAMETER_NAME_DSV_DELIMITER,
    //             PARAMETER_NAME_COMPRESSION,
    //             PARAMETER_NAME_LIMIT,
    //         ],
    //     )?;

    //     let delimiter = Self::extract_delimiter(variant, attributes)?;
    //     let resource = ImportExportHandlers::extract_resource(attributes, direction)?;
    //     let value_formats = Self::extract_value_formats(attributes)?;
    //     let (compression_format, _) =
    //         ImportExportHandlers::extract_compression_format(attributes, &resource)?;
    //     let limit =
    //         ImportExportHandlers::extract_unsigned_integer(attributes, PARAMETER_NAME_LIMIT, true)?;

    //     Ok(Box::new(Self {
    //         delimiter,
    //         resource,
    //         value_formats,
    //         limit,
    //         compression_format,
    //         direction,
    //     }))
    // }

    // fn extract_value_formats(
    //     attributes: &MapDataValue,
    // ) -> Result<Option<Vec<DsvValueFormat>>, ImportExportError> {
    //     let value_format_strings = ImportExportHandlers::extract_value_format_strings(attributes)?;

    //     if let Some(format_strings) = value_format_strings {
    //         Ok(Some(Self::formats_from_strings(format_strings)?))
    //     } else {
    //         Ok(None)
    //     }
    // }

    // fn formats_from_strings(
    //     value_format_strings: Vec<String>,
    // ) -> Result<Vec<DsvValueFormat>, ImportExportError> {
    //     let mut value_formats = Vec::with_capacity(value_format_strings.len());
    //     for s in value_format_strings {
    //         value_formats.push(DsvValueFormat::from_string(s.as_str())?);
    //     }
    //     Ok(value_formats)
    // }

    // fn extract_delimiter(
    //     variant: DsvVariant,
    //     attributes: &MapDataValue,
    // ) -> Result<u8, ImportExportError> {
    //     let delim_opt: Option<u8>;
    //     if let Some(string) =
    //         ImportExportHandlers::extract_string(attributes, PARAMETER_NAME_DSV_DELIMITER, true)?
    //     {
    //         if string.len() == 1 {
    //             delim_opt = Some(string.as_bytes()[0]);
    //         } else {
    //             return Err(ImportExportError::invalid_att_value_error(
    //                 PARAMETER_NAME_DSV_DELIMITER,
    //                 AnyDataValue::new_plain_string(string.to_owned()),
    //                 "delimiter should be exactly one byte",
    //             ));
    //         }
    //     } else {
    //         delim_opt = None;
    //     }

    //     let delimiter: u8 = match (variant, delim_opt) {
    //         (DsvVariant::DSV, Some(delim)) => delim,
    //         (DsvVariant::DSV, None) => {
    //             return Err(ImportExportError::MissingAttribute(
    //                 PARAMETER_NAME_DSV_DELIMITER.to_string(),
    //             ));
    //         }
    //         (DsvVariant::CSV, None) => b',',
    //         (DsvVariant::TSV, None) => b'\t',
    //         (DsvVariant::CSV, Some(_)) | (DsvVariant::TSV, Some(_)) => {
    //             return Err(ImportExportError::UnknownAttribute(
    //                 PARAMETER_NAME_DSV_DELIMITER.to_string(),
    //             ));
    //         }
    //     };

    //     Ok(delimiter)
    // }

    // /// Returns the set value formats, or finds a default value based on the
    // /// required arity.
    // fn value_formats_or_default(&self, arity: usize) -> Vec<DsvValueFormat> {
    //     self.value_formats.clone().unwrap_or_else(|| {
    //         Self::formats_from_strings(ImportExportHandlers::default_value_format_strings(arity))
    //             .unwrap()
    //     })
    // }
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
