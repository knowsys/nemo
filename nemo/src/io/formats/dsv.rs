//! Handler for resources of type DSV (delimiter-separated values).

use std::io::{BufRead, Write};

use nemo_physical::datavalues::{AnyDataValue, MapDataValue};
use nemo_physical::{datasources::table_providers::TableProvider, resource::Resource};

use crate::io::compression_format::CompressionFormat;
use crate::model::{
    PARAMETER_NAME_COMPRESSION, PARAMETER_NAME_DSV_DELIMITER, PARAMETER_NAME_FORMAT,
    PARAMETER_NAME_RESOURCE,
};
use crate::{
    error::Error,
    io::formats::types::{Direction, TableWriter},
    model::FileFormat,
};

use super::dsv_reader::DsvReader;
use super::dsv_value_format::DsvValueFormat;
use super::dsv_writer::DsvWriter;
use super::import_export::{ImportExportError, ImportExportHandler, ImportExportHandlers};

/// Internal enum to distnguish variants of the DSV format.
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
    /// This can be `None` for writing, since one can generate a default file
    /// name from the exported predicate in this case. This has little chance of
    /// success for imports, so the predicate is setting there.
    resource: Option<Resource>,
    /// The list of value formats to be used for importing/exporting data.
    /// If only the arity is given, this will use the most general export format
    /// for each value (and the list will still be set). The list can be `None`
    /// if neither formats nor arity were given for writing: in this case, a default
    /// arity-based formats can be used if the arity is clear from another source.
    value_formats: Option<Vec<DsvValueFormat>>,
    /// Compression format to be used, if specified. This can also be inferred
    /// from the resource, if given. So the only case where `None` is possible
    /// is when no resource is given (during output).
    compression_format: Option<CompressionFormat>,
    /// Direction of the operation.
    direction: Direction,
}

impl DsvHandler {
    /// Construct a DSV file handler with an arbitrary delimiter.
    pub(crate) fn try_new_dsv(
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::DSV, attributes, direction)
    }

    /// Construct a CSV file handler.
    pub(crate) fn try_new_csv(
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::CSV, attributes, direction)
    }

    /// Construct a TSV file handler.
    pub(crate) fn try_new_tsv(
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        Self::try_new(DsvVariant::TSV, attributes, direction)
    }

    /// Construct a DSV handler of the given variant.
    fn try_new(
        variant: DsvVariant,
        attributes: &MapDataValue,
        direction: Direction,
    ) -> Result<Box<dyn ImportExportHandler>, ImportExportError> {
        // Basic checks for unsupported attributes:
        ImportExportHandlers::check_attributes(
            attributes,
            &vec![
                PARAMETER_NAME_FORMAT,
                PARAMETER_NAME_RESOURCE,
                PARAMETER_NAME_DSV_DELIMITER,
                PARAMETER_NAME_COMPRESSION,
            ],
        )?;

        let delimiter = Self::extract_delimiter(variant, attributes)?;
        let resource = ImportExportHandlers::extract_resource(attributes, direction)?;
        let value_formats = Self::extract_value_formats(attributes)?;
        let (compression_format, _) =
            ImportExportHandlers::extract_compression_format(attributes, &resource)?;

        Ok(Box::new(Self {
            delimiter,
            resource,
            value_formats,
            compression_format,
            direction,
        }))
    }

    fn extract_value_formats(
        attributes: &MapDataValue,
    ) -> Result<Option<Vec<DsvValueFormat>>, ImportExportError> {
        let value_format_strings = ImportExportHandlers::extract_value_format_strings(attributes)?;

        if let Some(format_strings) = value_format_strings {
            Ok(Some(Self::formats_from_strings(format_strings)?))
        } else {
            Ok(None)
        }
    }

    fn formats_from_strings(
        value_format_strings: Vec<String>,
    ) -> Result<Vec<DsvValueFormat>, ImportExportError> {
        let mut value_formats = Vec::with_capacity(value_format_strings.len());
        for s in value_format_strings {
            value_formats.push(DsvValueFormat::from_string(s.as_str())?);
        }
        Ok(value_formats)
    }

    fn extract_delimiter(
        variant: DsvVariant,
        attributes: &MapDataValue,
    ) -> Result<u8, ImportExportError> {
        let delim_opt: Option<u8>;
        if let Some(string) =
            ImportExportHandlers::extract_string(attributes, PARAMETER_NAME_DSV_DELIMITER, true)?
        {
            if string.len() == 1 {
                delim_opt = Some(string.as_bytes()[0]);
            } else {
                return Err(ImportExportError::invalid_att_value_error(
                    PARAMETER_NAME_DSV_DELIMITER,
                    AnyDataValue::new_plain_string(string.to_owned()),
                    "delimiter should be exactly one byte",
                ));
            }
        } else {
            delim_opt = None;
        }

        let delimiter: u8;
        match (variant, delim_opt) {
            (DsvVariant::DSV, Some(delim)) => {
                delimiter = delim;
            }
            (DsvVariant::DSV, None) => {
                return Err(ImportExportError::MissingAttribute(
                    PARAMETER_NAME_DSV_DELIMITER.to_string(),
                ));
            }
            (DsvVariant::CSV, None) => {
                delimiter = b',';
            }
            (DsvVariant::TSV, None) => {
                delimiter = b',';
            }
            (DsvVariant::CSV, Some(_)) | (DsvVariant::TSV, Some(_)) => {
                return Err(ImportExportError::UnknownAttribute(
                    PARAMETER_NAME_DSV_DELIMITER.to_string(),
                ));
            }
        }
        Ok(delimiter)
    }

    /// Returns the set value formats, or finds a default value based on the
    /// required arity.
    fn value_formats_or_default(&self, arity: usize) -> Vec<DsvValueFormat> {
        self.value_formats.clone().unwrap_or_else(|| {
            Self::formats_from_strings(ImportExportHandlers::default_value_format_strings(arity))
                .unwrap()
        })
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

    fn reader(
        &self,
        read: Box<dyn BufRead>,
        arity: usize,
    ) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(DsvReader::new(
            read,
            self.delimiter,
            self.value_formats_or_default(arity),
        )))
    }

    fn writer(&self, writer: Box<dyn Write>, arity: usize) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DsvWriter::new(
            self.delimiter,
            writer,
            self.value_formats_or_default(arity),
        )))
    }

    fn resource(&self) -> Option<Resource> {
        self.resource.clone()
    }

    fn predicate_arity(&self) -> Option<usize> {
        match self.direction {
            Direction::Import => self.value_formats.as_ref().map(|vfs| {
                vfs.iter().fold(0, |acc, fmt| {
                    if *fmt == DsvValueFormat::SKIP {
                        acc
                    } else {
                        acc + 1
                    }
                })
            }),
            Direction::Export => self.value_formats.as_ref().map(|vfs| vfs.len()),
        }
    }

    fn file_arity(&self) -> Option<usize> {
        match self.direction {
            Direction::Export => self.value_formats.as_ref().map(|vfs| {
                vfs.iter().fold(0, |acc, fmt| {
                    if *fmt == DsvValueFormat::SKIP {
                        acc
                    } else {
                        acc + 1
                    }
                })
            }),
            Direction::Import => self.value_formats.as_ref().map(|vfs| vfs.len()),
        }
    }

    fn file_extension(&self) -> Option<String> {
        match self.file_format() {
            FileFormat::CSV => Some("csv".to_string()),
            FileFormat::DSV => Some("dsv".to_string()),
            FileFormat::TSV => Some("tsv".to_string()),
            _ => unreachable!(),
        }
    }

    fn compression_format(&self) -> Option<CompressionFormat> {
        self.compression_format
    }
}
