//! Handler for resources of type DSV (delimiter-separated values).

pub(crate) mod reader;
pub mod value_format;
pub(crate) mod writer;

use std::{
    io::{BufReader, Read, Write},
    sync::Arc,
};

use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::{AnyDataValue, DataValue},
};
use reader::DsvReader;
use strum_macros::EnumIter;
use value_format::DsvValueFormats;
use writer::DsvWriter;

use crate::{
    error::Error,
    io::format_builder::{
        format_parameter, format_tag, value_type_matches, AnyImportExportBuilder, FormatParameter,
        Parameters, StandardParameter,
    },
    rule_model::{
        components::{import_export::Direction, term::value_type::ValueType},
        error::validation_error::ValidationErrorKind,
    },
    syntax::import_export::{attribute, file_format},
};

use super::{ExportHandler, FileFormatMeta, FormatBuilder, ImportHandler, TableWriter};

/// An [ImportExportHandler] for delimiter-separated values.
#[derive(Debug, Clone)]
pub struct DsvHandler {
    /// The specific delimiter for this format.
    delimiter: u8,
    /// The list of value formats to be used for importing/exporting data.
    value_formats: DsvValueFormats,
    /// Maximum number of statements that should be imported/exported.
    limit: Option<u64>,
    /// Whether to ignore headers
    ignore_headers: bool,
}

impl DsvHandler {
    /// Create new [`DsvHandler`] with given delimiter and arity
    pub fn new(delimiter: u8, arity: usize) -> Self {
        DsvHandler {
            delimiter,
            value_formats: DsvValueFormats::default(arity),
            limit: None,
            ignore_headers: false,
        }
    }
}

impl FileFormatMeta for DsvHandler {
    fn default_extension(&self) -> String {
        DsvVariant::from_delimiter(self.delimiter)
            .default_extension()
            .to_string()
    }

    fn media_type(&self) -> String {
        DsvVariant::from_delimiter(self.delimiter)
            .media_type()
            .to_string()
    }
}

impl ExportHandler for DsvHandler {
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(DsvWriter::new(
            self.delimiter,
            writer,
            self.value_formats.clone(),
            self.limit,
        )))
    }
}

impl ImportHandler for DsvHandler {
    fn reader(&self, read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(DsvReader::new(
            BufReader::new(read),
            self.delimiter,
            self.value_formats.clone(),
            None,
            self.limit,
            self.ignore_headers,
        )))
    }
}

#[derive(Assoc, EnumIter, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[func(fn name(&self) -> &'static str)]
#[func(fn media_type(&self) -> &'static str)]
#[func(fn default_extension(&self) -> &'static str)]
#[func(fn from_delimiter(delimiter: u8) -> Self)]
#[func(fn delimiter(&self) -> Option<u8>)]
enum DsvVariant {
    /// Comma-separated values
    #[assoc(name = file_format::CSV)]
    #[assoc(media_type = &file_format::MEDIA_TYPE_CSV)]
    #[assoc(default_extension = &file_format::EXTENSION_CSV)]
    #[assoc(from_delimiter = b',')]
    #[assoc(delimiter = b',')]
    CSV,
    /// Tab-separated values
    #[assoc(name = file_format::TSV)]
    #[assoc(media_type = &file_format::MEDIA_TYPE_TSV)]
    #[assoc(default_extension = &file_format::EXTENSION_TSV)]
    #[assoc(from_delimiter = b'\t')]
    #[assoc(delimiter = b'\t')]
    TSV,
    /// Delimiter-separated values
    #[assoc(name = file_format::DSV)]
    #[assoc(media_type = &file_format::MEDIA_TYPE_DSV)]
    #[assoc(default_extension = &file_format::EXTENSION_DSV)]
    #[assoc(from_delimiter = _)]
    DSV,
}

format_tag! {
    pub(crate) enum DsvTag(SupportedFormatTag::Dsv) {
        Dsv => file_format::DSV,
        Tsv => file_format::TSV,
        Csv => file_format::CSV,
    }
}

format_parameter! {
    pub(crate) enum DsvParameter(StandardParameter) {
        Limit(name = attribute::LIMIT, supported_types = &[ValueType::Number]),
        Delimiter(name = attribute::DSV_DELIMITER, supported_types = &[ValueType::String]),
        Format(name = attribute::FORMAT, supported_types = &[ValueType::Constant, ValueType::Tuple]),
        IgnoreHeaders(name = attribute::IGNORE_HEADERS, supported_types = &[ValueType::Boolean]),
    }
}

impl FormatParameter<DsvTag> for DsvParameter {
    fn required_for(&self, tag: DsvTag) -> bool {
        if matches!(tag, DsvTag::Dsv) {
            matches!(self, DsvParameter::Delimiter)
        } else {
            false
        }
    }

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind> {
        value_type_matches(self, &value, &self.supported_types())?;

        match self {
            DsvParameter::BaseParamType(base) => {
                FormatParameter::<DsvTag>::is_value_valid(base, value)
            }
            DsvParameter::Limit => value
                .to_u64()
                .and(Some(()))
                .ok_or(ValidationErrorKind::ImportExportLimitNegative),
            DsvParameter::Delimiter => (value.to_plain_string_unchecked().len() == 1)
                .then_some(())
                .ok_or(ValidationErrorKind::ImportExportDelimiter),
            DsvParameter::Format => DsvValueFormats::try_from(value).and(Ok(())).map_err(|_| {
                ValidationErrorKind::ImportExportValueFormat {
                    file_format: "dsv".into(),
                }
            }),
            DsvParameter::IgnoreHeaders => Ok(()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DsvBuilder {
    limit: Option<u64>,
    delimiter: u8,
    value_formats: Option<DsvValueFormats>,
    ignore_headers: bool,
}

impl From<DsvBuilder> for AnyImportExportBuilder {
    fn from(value: DsvBuilder) -> Self {
        AnyImportExportBuilder::Dsv(value)
    }
}

impl FormatBuilder for DsvBuilder {
    type Parameter = DsvParameter;

    type Tag = DsvTag;

    fn new(
        tag: Self::Tag,
        parameters: &Parameters<DsvBuilder>,
        _direction: Direction,
    ) -> Result<Self, ValidationErrorKind> {
        let value_formats = parameters
            .get_optional(DsvParameter::Format)
            .map(|value| DsvValueFormats::try_from(value).unwrap());

        let limit = parameters
            .get_optional(DsvParameter::Limit)
            .map(|value| value.to_u64_unchecked());

        let delimiter = match tag {
            DsvTag::Dsv => parameters
                .get_required(DsvParameter::Delimiter)
                .to_plain_string_unchecked()
                .bytes()
                .next()
                .unwrap(),
            DsvTag::Tsv => b'\t',
            DsvTag::Csv => b',',
        };

        let ignore_headers = parameters
            .get_optional(DsvParameter::IgnoreHeaders)
            .as_ref()
            .map(AnyDataValue::to_boolean_unchecked)
            .unwrap_or(false);

        Ok(Self {
            delimiter,
            value_formats,
            limit,
            ignore_headers,
        })
    }

    fn expected_arity(&self) -> Option<usize> {
        self.value_formats.as_ref().map(DsvValueFormats::arity)
    }

    fn build_import(&self, arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static> {
        Arc::new(DsvHandler {
            delimiter: self.delimiter,
            value_formats: self
                .value_formats
                .clone()
                .unwrap_or(DsvValueFormats::default(arity)),
            limit: self.limit,
            ignore_headers: self.ignore_headers,
        })
    }

    fn build_export(&self, arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static> {
        Arc::new(DsvHandler {
            delimiter: self.delimiter,
            value_formats: self
                .value_formats
                .clone()
                .unwrap_or(DsvValueFormats::default(arity)),
            limit: self.limit,
            ignore_headers: self.ignore_headers,
        })
    }
}

#[cfg(legacy)]
mod test {
    use super::*;

    #[test]
    fn format_metadata() {
        for (format, delimiter) in &[
            (FileFormat::DSV, b';'),
            (FileFormat::CSV, b','),
            (FileFormat::TSV, b'\t'),
        ] {
            let handler = DsvHandler {
                delimiter: *delimiter,
                resource: ResourceSpec::from_string(format!("dummy.{}", format.extension())),
                value_formats: DsvValueFormats::default(3),
                compression_format: CompressionFormat::None,
                limit: None,
            };

            assert_eq!(format.extension(), handler.default_extension());
            assert_eq!(format.media_type(), handler.media_type());
            assert_eq!(3, handler.predicate_arity());
        }
    }
}
