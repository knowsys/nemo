//! Handler for resources of type SPARQL (SPARQL query language for RDF)).

use spargebra::Query;
use std::sync::Arc;

use nemo_physical::{
    datavalues::{AnyDataValue, DataValue},
    resource::{ResourceBuilder, ResourceValidationErrorKind},
};
use oxiri::Iri;

use crate::{
    io::format_builder::{
        format_parameter, format_tag, value_type_matches, AnyImportExportBuilder, FormatParameter,
        Parameters, StandardParameter,
    },
    rule_model::{
        components::{import_export::Direction, term::value_type::ValueType},
        error::validation_error::ValidationError,
    },
    syntax::import_export::{attribute, file_format},
};

use super::{ExportHandler, FormatBuilder, ImportHandler};

use crate::io::formats::dsv::{value_format::DsvValueFormats, DsvHandler};

/// A char limit to decide if a query is send as GET or POST request
const HTTP_GET_CHAR_LIMIT: usize = 2000;

format_tag! {
    pub(crate) enum SparqlTag(SupportedFormatTag::Sparql) {
        Sparql => file_format::SPARQL,
    }
}

format_parameter! {
    pub(crate) enum SparqlParameter(StandardParameter) {
        Format(name = attribute::FORMAT, supported_types = &[ValueType::Constant, ValueType::Tuple]),
        Base(name = attribute::BASE, supported_types = &[ValueType::Constant]),
        Endpoint(name = attribute::ENDPOINT, supported_types = &[ValueType::Constant,ValueType::String]),
        Query(name = attribute::QUERY, supported_types = &[ValueType::String]),
    }
}

impl FormatParameter<SparqlTag> for SparqlParameter {
    fn required_for(&self, tag: SparqlTag) -> bool {
        matches!(tag, SparqlTag::Sparql) && matches!(self, Self::Endpoint | Self::Query)
    }

    fn is_value_valid(&self, value: AnyDataValue) -> Result<(), ValidationError> {
        value_type_matches(self, &value, self.supported_types())?;

        match self {
            SparqlParameter::BaseParamType(base) => {
                FormatParameter::<SparqlTag>::is_value_valid(base, value)
            }
            SparqlParameter::Base => Ok(()),
            SparqlParameter::Format => DsvValueFormats::try_from(value).and(Ok(())).map_err(|_| {
                ValidationError::ImportExportValueFormat {
                    file_format: "sparql".to_string(),
                }
            }),

            SparqlParameter::Endpoint => {
                let resource = ResourceBuilder::try_from(value)
                    .map_err(ValidationError::from)?
                    .finalize();
                if resource.is_http() {
                    Ok(())
                } else {
                    Err(ValidationError::InvalidHttpIri)
                }
            }
            SparqlParameter::Query => {
                let query = value.to_plain_string_unchecked();
                Query::parse(query.as_str(), None).and(Ok(())).map_err(|e| {
                    ValidationError::InvalidSparqlQuery {
                        query,
                        oxi_error: e.to_string(),
                    }
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SparqlBuilder {
    value_formats: Option<DsvValueFormats>,
    endpoint: Iri<String>,
    query: Query,
}

impl From<SparqlBuilder> for AnyImportExportBuilder {
    fn from(value: SparqlBuilder) -> Self {
        AnyImportExportBuilder::Sparql(Box::new(value))
    }
}
impl FormatBuilder for SparqlBuilder {
    type Parameter = SparqlParameter;
    type Tag = SparqlTag;
    fn new(
        _tag: Self::Tag,
        parameters: &Parameters<SparqlBuilder>,
        _direction: Direction,
    ) -> Result<Self, ValidationError> {
        // Copied from DsvBuilder
        let value_formats = parameters
            .get_optional(SparqlParameter::Format)
            .map(|value| {
                DsvValueFormats::try_from(value).expect("value formats have already been validated")
            });

        // SPARQL specific fields
        let value = parameters.get_required(SparqlParameter::Endpoint);
        let endpoint = value
            .to_plain_string()
            .or_else(|| value.to_iri())
            .map(Iri::parse_unchecked)
            .expect("Endpoint is validated already");

        let query = parameters
            .get_required(SparqlParameter::Query)
            .to_plain_string_unchecked();

        let query = Query::parse(query.as_str(), None).expect("query has already been validated");

        Ok(Self {
            value_formats,
            endpoint,
            query,
        })
    }

    /// Create new [ResourceBuilder] from endpoint parameter
    fn customize_resource_builder(
        &self,
        direction: Direction,
        _builder: Option<ResourceBuilder>,
    ) -> Result<Option<ResourceBuilder>, ResourceValidationErrorKind> {
        match direction {
            Direction::Import => {
                let mut resource_builder = ResourceBuilder::try_from(self.endpoint.clone())?;
                let query = self.query.to_string();
                if query.len() > HTTP_GET_CHAR_LIMIT {
                    resource_builder.add_post_parameter(String::from("query"), query)?;
                } else {
                    resource_builder.add_get_parameter(String::from("query"), query)?;
                }
                Ok(Some(resource_builder))
            }
            _ => Ok(None),
        }
    }

    fn expected_arity(&self) -> Option<usize> {
        let mut var_count: usize = 0;
        match &self.query {
            Query::Select { pattern, .. }
            | Query::Describe { pattern, .. }
            | Query::Construct { pattern, .. }
            | Query::Ask { pattern, .. } => pattern.on_in_scope_variable(|_| var_count += 1),
        }
        Some(var_count)
    }

    fn build_import(&self, arity: usize) -> Arc<dyn ImportHandler + Send + Sync + 'static> {
        Arc::new(DsvHandler::with_value_formats(
            b'\t',
            self.value_formats
                .clone()
                .unwrap_or(DsvValueFormats::default(arity)),
            None,
            true,
            false,
        ))
    }

    fn build_export(&self, _arity: usize) -> Arc<dyn ExportHandler + Send + Sync + 'static> {
        unimplemented!("SPARQL export is currently not supported")
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{
        ast::{directive::import::Import, ProgramAST},
        input::ParserInput,
        ParserState,
    };
    use nom::combinator::all_consuming;

    use crate::io::format_builder::FormatParameter;
    use nemo_physical::datavalues::AnyDataValue;

    use super::SparqlParameter;

    #[test]
    fn parse_query() {
        let valid_query = AnyDataValue::new_plain_string(String::from("
            PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX bd: <http://www.bigdata.com/rdf#>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX ps: <http://www.wikidata.org/prop/statement/>
            PREFIX p: <http://www.wikidata.org/prop/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?item ?itemLabel
                WHERE
                {
                ?item wdt:P31 wd:Q146. # Must be a cat
                SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } # Helps get the label in your language, if not, then default for all languages, then en language
                }
            LIMIT 10")
        );

        let query_param = SparqlParameter::Query;
        let result = query_param.is_value_valid(valid_query);
        assert!(result.is_ok());

        // Invalid because no prefixes are specified
        let invalid_query = AnyDataValue::new_plain_string(String::from("
            SELECT ?item ?itemLabel
            WHERE
            {
            ?item wdt:P31 wd:Q146. # Must be a cat
            SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } # Helps get the label in your language, if not, then default for all languages, then en language
            }
            LIMIT 10")
        );
        let result = query_param.is_value_valid(invalid_query);
        assert!(result.is_err());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_import() {
        let parser_input = ParserInput::new(
            r#"@import response :- sparql {endpoint = "http://example.org", query="SELECT ?a ?b ?c WHERE {?a ?b ?c.}", iri_fragment="section1", http_get_parameters={test=foo}, http_post_parameters={test=(foo,bar)}, http_headers={ACCEPT="html/text"}}"#,
            ParserState::default(),
        );
        let result = all_consuming(Import::parse)(parser_input);
        assert!(result.is_ok());
    }
}
