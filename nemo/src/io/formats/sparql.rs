//! Handler for resources of type Sparql (java script object notation).

use spargebra::Query;
use std::sync::Arc;

use nemo_physical::datavalues::{AnyDataValue, DataValue};
use oxiri::Iri;

//use reader::DsvReader;
//use value_format::DsvValueFormats;
//use writer::DsvWriter;

use crate::{
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

use super::{ExportHandler, FormatBuilder, ImportHandler, ResourceSpec};

use crate::io::formats::dsv::{value_format::DsvValueFormats, DsvHandler};

format_tag! {
    pub(crate) enum SparqlTag(SupportedFormatTag::Sparql) {
        Sparql => file_format::SPARQL,
    }
}

format_parameter! {
    pub(crate) enum SparqlParameter(StandardParameter) {
        Limit(name = attribute::LIMIT, supported_types = &[ValueType::Number]),
        Format(name = attribute::FORMAT, supported_types = &[ValueType::Constant, ValueType::Tuple]),
        Base(name = attribute::BASE, supported_types = &[ValueType::Constant]),
        Endpoint(name = attribute::ENDPOINT, supported_types = &[ValueType::Constant]),
        Query(name = attribute::QUERY, supported_types = &[ValueType::String]),
        IgnoreHeaders(name = attribute::IGNORE_HEADERS, supported_types = &[ValueType::Boolean]),

    }
}

impl FormatParameter<SparqlTag> for SparqlParameter {
    fn required_for(&self, tag: SparqlTag) -> bool {
        if matches!(tag, SparqlTag::Sparql) {
            matches!(self, SparqlParameter::Endpoint) && matches!(self, SparqlParameter::Query)
        } else {
            false
        }
    }

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationErrorKind> {
        value_type_matches(self, &value, self.supported_types())?;

        match self {
            SparqlParameter::BaseParamType(base) => {
                FormatParameter::<SparqlTag>::is_value_valid(base, value)
            }
            SparqlParameter::Limit => value
                .to_u64()
                .and(Some(()))
                .ok_or(ValidationErrorKind::ImportExportLimitNegative),
            SparqlParameter::Base => Ok(()),
            SparqlParameter::Format => DsvValueFormats::try_from(value).and(Ok(())).map_err(|_| {
                ValidationErrorKind::ImportExportValueFormat {
                    file_format: "rdf".to_string(),
                }
            }),
            SparqlParameter::IgnoreHeaders => Ok(()),
            SparqlParameter::Endpoint => value
                .to_iri()
                .ok_or(ValidationErrorKind::ImportExportInvalidIri)
                .and_then(|iri| {
                    Iri::parse(iri)
                        .map(|_| ())
                        .map_err(|_| ValidationErrorKind::ImportExportInvalidIri)
                }),
            SparqlParameter::Query => value
                .to_plain_string()
                .ok_or(ValidationErrorKind::ImportExportLimitNegative)
                .and_then(|query| {
                    Query::parse(query.as_str(), None).map(|_| ()).map_err(|e| {
                        ValidationErrorKind::ImportExportInvalidSparqlQuery {
                            oxi_error: e.to_string(),
                        }
                    })
                }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SparqlBuilder {
    limit: Option<u64>,
    delimiter: u8,
    value_formats: Option<DsvValueFormats>,
    ignore_headers: bool,
    parsed_query: Query,
}

impl From<SparqlBuilder> for AnyImportExportBuilder {
    fn from(value: SparqlBuilder) -> Self {
        AnyImportExportBuilder::Sparql(value)
    }
}
impl FormatBuilder for SparqlBuilder {
    type Parameter = SparqlParameter;
    type Tag = SparqlTag;
    fn new(
        _tag: Self::Tag,
        parameters: &Parameters<SparqlBuilder>,
        _direction: Direction,
    ) -> Result<(Self, Option<ResourceSpec>), ValidationErrorKind> {
        // Copied from DsvBuilder
        let value_formats = parameters
            .get_optional(SparqlParameter::Format)
            .map(|value| DsvValueFormats::try_from(value).unwrap());

        let limit = parameters
            .get_optional(SparqlParameter::Limit)
            .map(|value| value.to_u64_unchecked());

        let ignore_headers = parameters
            .get_optional(SparqlParameter::IgnoreHeaders)
            .as_ref()
            .map(AnyDataValue::to_boolean_unchecked)
            .unwrap_or(false);

        // Sparql-specific fields
        let endpoint = Iri::parse_unchecked(
            parameters
                .get_required(SparqlParameter::Endpoint.into())
                .to_iri_unchecked(),
        );

        let query = parameters
            .get_required(SparqlParameter::Query.into())
            .to_plain_string_unchecked();

        let parsed_query = Query::parse(query.as_str(), None).unwrap();

        // Create and return a new ResourceSpec to replace the existing ResourceSpec
        let resource = ResourceSpec::from_endpoint(endpoint, query);
        Ok((
            Self {
                delimiter: b'\t',
                value_formats,
                limit,
                ignore_headers,
                parsed_query,
            },
            Some(resource),
        ))
    }

    fn expected_arity(&self) -> Option<usize> {
        let mut var_count: usize = 0;
        match &self.parsed_query {
            Query::Select { pattern, .. }
            | Query::Describe { pattern, .. }
            | Query::Construct { pattern, .. }
            | Query::Ask { pattern, .. } => pattern.on_in_scope_variable(|_| var_count += 1),
        }
        Some(var_count)
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

    fn build_export(&self, _arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static> {
        unimplemented!("Sparql export is currently not supported")
    }
}
