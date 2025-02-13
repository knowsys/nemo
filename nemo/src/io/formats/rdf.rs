//! Handler for resources of type RDF (Rsource Description Format).

#![allow(missing_docs)]

pub mod error;
pub(crate) mod reader;
pub(crate) mod value_format;
pub(crate) mod writer;

use std::{
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use nemo_physical::{
    datasources::table_providers::TableProvider,
    datavalues::{AnyDataValue, DataValue},
};

use oxiri::Iri;
use reader::RdfReader;
use strum::VariantArray;
use strum_macros::VariantArray;
use value_format::RdfValueFormats;
use writer::RdfWriter;

use crate::{
    error::Error,
    io::{
        compression_format::CompressionFormat,
        format_builder::{
            format_parameter, format_tag, value_type_matches, AnyImportExportBuilder,
            FormatParameter, Parameters, StandardParameter,
        },
    },
    rule_model::{
        components::{import_export::Direction, term::value_type::ValueType},
        error::validation_error::ValidationErrorKind,
    },
    syntax::import_export::{attribute, file_format},
};

use super::FileFormatMeta;
use super::{ExportHandler, FormatBuilder, ImportHandler, TableWriter};

/// The different supported variants of the RDF format.
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq, VariantArray)]
#[func(pub fn media_type(&self) -> &'static str)]
#[func(pub fn default_extension(&self) -> &'static str)]
#[func(pub fn arity(&self) -> usize)]
#[func(pub fn name(&self) -> &'static str)]
pub enum RdfVariant {
    /// RDF 1.1 N-Triples
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_NTRIPLES)]
    #[assoc(default_extension = file_format::EXTENSION_RDF_NTRIPLES)]
    #[assoc(arity = 3)]
    #[assoc(name = file_format::RDF_NTRIPLES)]
    NTriples,
    /// RDF 1.1 N-Quads
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_NQUADS)]
    #[assoc(default_extension = file_format::EXTENSION_RDF_NQUADS)]
    #[assoc(arity = 4)]
    #[assoc(name = file_format::RDF_NQUADS)]
    NQuads,
    /// RDF 1.1 Turtle
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_TURTLE)]
    #[assoc(default_extension = file_format::EXTENSION_RDF_TURTLE)]
    #[assoc(arity = 3)]
    #[assoc(name = file_format::RDF_TURTLE)]
    Turtle,
    /// RDF 1.1 RDF/XML
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_XML)]
    #[assoc(default_extension = file_format::EXTENSION_RDF_XML)]
    #[assoc(arity = 3)]
    #[assoc(name = file_format::RDF_XML)]
    RDFXML,
    /// RDF 1.1 TriG
    #[assoc(media_type = file_format::MEDIA_TYPE_RDF_TRIG)]
    #[assoc(default_extension = file_format::EXTENSION_RDF_TRIG)]
    #[assoc(arity = 4)]
    #[assoc(name = file_format::RDF_TRIG)]
    TriG,
}

impl std::fmt::Display for RdfVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A handler for RDF formats.
#[derive(Debug, Clone)]
pub struct RdfHandler {
    /// Base IRI, if given.
    base: Option<Iri<String>>,
    /// The specific RDF format to be used.
    variant: RdfVariant,
    /// The list of value formats to be used for importing/exporting data.
    value_formats: RdfValueFormats,
    /// Maximum number of statements that should be imported/exported.
    limit: Option<u64>,
}

impl FileFormatMeta for RdfHandler {
    fn default_extension(&self) -> String {
        self.variant.default_extension().to_string()
    }

    fn media_type(&self) -> String {
        self.variant.media_type().to_string()
    }
}

impl ImportHandler for RdfHandler {
    fn reader(&self, read: Box<dyn Read>) -> Result<Box<dyn TableProvider>, Error> {
        Ok(Box::new(RdfReader::new(
            read,
            self.variant,
            self.base.clone(),
            self.value_formats.clone(),
            self.limit,
        )))
    }
}

impl ExportHandler for RdfHandler {
    fn writer(&self, writer: Box<dyn Write>) -> Result<Box<dyn TableWriter>, Error> {
        Ok(Box::new(RdfWriter::new(
            writer,
            self.variant,
            self.value_formats.clone(),
            self.limit,
        )))
    }
}

format_tag! {
    pub(crate) enum RdfTag(SupportedFormatTag::Rdf) {
        Rdf => file_format::RDF_UNSPECIFIED,
        NTriples => file_format::RDF_NTRIPLES,
        NQuads => file_format::RDF_NQUADS,
        Turtle => file_format::RDF_TURTLE,
        Trig => file_format::RDF_TRIG,
        RdfXml => file_format::RDF_XML,
    }
}

format_parameter! {
    pub(crate) enum RdfParameter(StandardParameter) {
        Limit(name = attribute::LIMIT, supported_types = &[ValueType::Number]),
        Base(name = attribute::BASE, supported_types = &[ValueType::Constant]),
        Format(name = attribute::FORMAT, supported_types = &[ValueType::Tuple]),
    }
}

impl FormatParameter<RdfTag> for RdfParameter {
    fn required_for(&self, tag: RdfTag) -> bool {
        matches!(tag, RdfTag::Rdf)
            && matches!(
                self,
                RdfParameter::BaseParamType(StandardParameter::Resource)
            )
    }

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind> {
        value_type_matches(self, &value, self.supported_types())?;

        match self {
            RdfParameter::BaseParamType(base) => {
                FormatParameter::<RdfTag>::is_value_valid(base, value)
            }
            RdfParameter::Limit => value
                .to_u64()
                .and(Some(()))
                .ok_or(ValidationErrorKind::ImportExportLimitNegative),
            RdfParameter::Base => Ok(()),
            RdfParameter::Format => RdfValueFormats::try_from(value).and(Ok(())).map_err(|_| {
                ValidationErrorKind::ImportExportValueFormat {
                    file_format: "rdf".to_string(),
                }
            }),
        }
    }
}

impl From<RdfHandler> for AnyImportExportBuilder {
    fn from(value: RdfHandler) -> Self {
        Self::Rdf(value)
    }
}

impl FormatBuilder for RdfHandler {
    type Parameter = RdfParameter;
    type Tag = RdfTag;

    fn new(
        tag: Self::Tag,
        parameters: &Parameters<RdfHandler>,
        _direction: Direction,
    ) -> Result<Self, ValidationErrorKind> {
        let variant = match tag {
            RdfTag::Rdf => {
                let value = parameters
                    .get_required(RdfParameter::BaseParamType(StandardParameter::Resource));

                let resource = value.to_plain_string_unchecked();
                let stripped = CompressionFormat::from_resource(&resource).1;

                let Some(extension) = Path::new(&stripped).extension() else {
                    return Err(ValidationErrorKind::RdfUnspecifiedMissingExtension);
                };

                let Some(variant) = RdfVariant::VARIANTS
                    .iter()
                    .find(|variant| extension == variant.default_extension())
                else {
                    return Err(ValidationErrorKind::RdfUnspecifiedUnknownExtension(
                        extension.to_string_lossy().into_owned(),
                    ));
                };

                *variant
            }
            RdfTag::NTriples => RdfVariant::NTriples,
            RdfTag::NQuads => RdfVariant::NQuads,
            RdfTag::Turtle => RdfVariant::Turtle,
            RdfTag::Trig => RdfVariant::TriG,
            RdfTag::RdfXml => RdfVariant::RDFXML,
        };

        let base = parameters
            .get_optional(RdfParameter::Base)
            .map(|value| Iri::parse_unchecked(value.to_iri_unchecked()));

        let value_formats = match parameters.get_optional(RdfParameter::Format) {
            Some(value) => RdfValueFormats::try_from(value).unwrap(),
            None => RdfValueFormats::default(variant.arity()),
        };

        let limit = parameters
            .get_optional(RdfParameter::Limit)
            .as_ref()
            .map(AnyDataValue::to_u64_unchecked);

        Ok(Self {
            base,
            variant,
            value_formats,
            limit,
        })
    }

    fn expected_arity(&self) -> Option<usize> {
        Some(self.variant.arity())
    }

    fn build_import(&self, _arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static> {
        Arc::new(self.clone())
    }

    fn build_export(&self, _arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static> {
        Arc::new(self.clone())
    }
}
