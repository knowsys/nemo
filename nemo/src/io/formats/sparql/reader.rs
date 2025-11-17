//! Reader for resources of type SPARQL (SPARQL query language for RDF).

use std::io::Read;

use nemo_physical::error::ReadingErrorKind;
use nemo_physical::management::bytesized::ByteSized;
use nemo_physical::meta::timing::TimedCode;
use nemo_physical::tabular::filters::FilterTransformPattern;
use nemo_physical::{datasources::table_providers::TableProvider, error::ReadingError};
use nemo_physical::{
    datasources::tuple_writer::TupleWriter,
    datavalues::{AnyDataValue, DataValue},
};
use oxiri::Iri;
use oxrdf::{Literal, NamedNode};
use spargebra::Query;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;

use crate::io::format_builder::FormatBuilder;
use crate::io::formats::dsv::reader::DsvReader;
use crate::io::formats::dsv::value_format::DsvValueFormats;
use crate::io::resource_providers::ResourceProvider;
use crate::io::resource_providers::http::HttpResourceProvider;
use crate::rule_model::components::import_export::Direction;
use crate::syntax::import_export::file_format::MEDIA_TYPE_TSV;

use super::{MAX_BINDINGS_PER_PAGE, QUERY_PAGE_CHAR_LIMIT, SparqlBuilder};

#[derive(Debug)]
pub(crate) struct SparqlReader {
    builder: SparqlBuilder,
    patterns: Vec<FilterTransformPattern>,
}

impl SparqlReader {
    pub fn new(builder: SparqlBuilder, patterns: Vec<FilterTransformPattern>) -> Self {
        Self { builder, patterns }
    }

    async fn execute_from_builder(
        builder: &SparqlBuilder,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        let resource_builder = builder
            .customize_resource_builder(Direction::Import, None)
            .expect("should have been validated")
            .expect("should create a resource builder");
        let resource = resource_builder.finalize();
        let provider = HttpResourceProvider {};

        provider.open_resource(&resource, MEDIA_TYPE_TSV).await
    }

    async fn execute_query(
        &self,
        endpoint: &Iri<String>,
        query: &Query,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        let builder = SparqlBuilder::with_endpoint_and_query(
            endpoint.clone(),
            query.clone(),
            self.builder.value_formats.clone(),
        );

        Self::execute_from_builder(&builder).await
    }

    async fn load_from_query(
        &self,
        query: &Query,
        tuple_writer: &mut TupleWriter<'_>,
    ) -> Result<(), ReadingError> {
        TimedCode::instance()
            .sub("Reasoning/Execution/SPARQL queries")
            .start();

        let response = self
            .execute_query(&self.builder.endpoint, query)
            .await?
            .expect("query result should not be empty");
        TimedCode::instance()
            .sub("Reasoning/Execution/SPARQL queries")
            .stop();
        Self::read_table_data(
            response,
            tuple_writer,
            self.builder.value_formats.clone(),
            self.patterns.clone(),
        )
    }

    async fn load_from_bindings(
        &self,
        bound_positions: &[usize],
        bindings: &[Vec<AnyDataValue>],
        tuple_writer: &mut TupleWriter<'_>,
    ) -> Result<(), ReadingError> {
        for (page, query) in
            self.queries_with_bindings(bound_positions, bindings, MAX_BINDINGS_PER_PAGE)
        {
            let result = self.load_from_query(&query, tuple_writer).await;

            if let Err(error) = result
                && let ReadingErrorKind::HttpTransfer(error) = error.kind()
                && let Some(code) = error.status()
                && code == reqwest::StatusCode::PAYLOAD_TOO_LARGE
            {
                if bindings.len() == 1 {
                    return Err(ReadingError::new_external(Box::new(error)));
                }

                // the page size is still too large, try half
                for page in page.chunks(page.len().div_ceil(2).max(1)) {
                    Box::pin(self.load_from_bindings(bound_positions, page, tuple_writer)).await?;
                }
            }
        }

        Ok(())
    }

    fn read_table_data(
        mut read: Box<dyn Read>,
        tuple_writer: &mut TupleWriter,
        value_formats: Option<DsvValueFormats>,
        patterns: Vec<FilterTransformPattern>,
    ) -> Result<(), ReadingError> {
        // make sure we have the whole response in one contiguous
        // buffer, otherwise the [DsvReader] will allocate for each
        // record, which is horribly inefficient if the query returns
        // lots of rows.
        let mut buf = String::new();
        let _ = read.read_to_string(&mut buf)?;

        let reader = DsvReader::new(
            Box::new(buf.as_bytes()),
            b'\t',
            value_formats.unwrap_or(DsvValueFormats::default(tuple_writer.input_column_number())),
            None,
            None,
            true,
            false,
            patterns,
        );

        reader.read(tuple_writer)
    }

    fn ground_term_from_datavalue(value: &AnyDataValue) -> Option<GroundTerm> {
        use nemo_physical::datavalues::ValueDomain;

        match value.value_domain() {
            ValueDomain::PlainString => Some(GroundTerm::Literal(Literal::new_simple_literal(
                value.to_plain_string_unchecked(),
            ))),
            ValueDomain::LanguageTaggedString => {
                let (content, language) = value.to_language_tagged_string_unchecked();
                Some(GroundTerm::Literal(
                    Literal::new_language_tagged_literal(content, language)
                        .expect("should be valid"),
                ))
            }
            ValueDomain::Iri => Some(GroundTerm::NamedNode(NamedNode::new_unchecked(
                value.to_iri_unchecked(),
            ))),
            ValueDomain::Float
            | ValueDomain::Double
            | ValueDomain::UnsignedLong
            | ValueDomain::NonNegativeLong
            | ValueDomain::UnsignedInt
            | ValueDomain::NonNegativeInt
            | ValueDomain::Long
            | ValueDomain::Int
            | ValueDomain::Boolean
            | ValueDomain::Other => Some(GroundTerm::Literal(Literal::new_typed_literal(
                value.lexical_value(),
                NamedNode::new_unchecked(value.datatype_iri()),
            ))),
            ValueDomain::Null => None,

            ValueDomain::Tuple | ValueDomain::Map => {
                unimplemented!("no support for complex values yet")
            }
        }
    }

    fn pattern_with_bindings(
        pattern: &GraphPattern,
        bound_positions: &[usize],
        bindings: &[Vec<AnyDataValue>],
    ) -> GraphPattern {
        match pattern {
            GraphPattern::Project { inner, variables } => {
                let bound_variables = bound_positions
                    .iter()
                    .map(|idx| variables[*idx].clone())
                    .collect::<Vec<_>>();
                let bindings = bindings
                    .iter()
                    .map(|row| row.iter().map(Self::ground_term_from_datavalue).collect())
                    .collect();

                let values = GraphPattern::Values {
                    variables: bound_variables,
                    bindings,
                };
                let join = GraphPattern::Join {
                    left: inner.clone(),
                    right: Box::new(values),
                };

                GraphPattern::Project {
                    inner: Box::new(join),
                    variables: variables.clone(),
                }
            }
            _ => pattern.clone(),
        }
    }

    fn query_with_bindings(
        &self,
        bound_positions: &[usize],
        bindings: &[Vec<AnyDataValue>],
    ) -> Option<Query> {
        let query = match &self.builder.query {
            q @ &Query::Construct { .. } | q @ &Query::Describe { .. } => q.clone(),
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => Query::Select {
                dataset: dataset.clone(),
                base_iri: base_iri.clone(),
                pattern: Self::pattern_with_bindings(pattern, bound_positions, bindings),
            },
            Query::Ask {
                dataset,
                pattern,
                base_iri,
            } => Query::Ask {
                dataset: dataset.clone(),
                base_iri: base_iri.clone(),
                pattern: Self::pattern_with_bindings(pattern, bound_positions, bindings),
            },
        };

        if query.to_string().len() < QUERY_PAGE_CHAR_LIMIT {
            Some(query)
        } else {
            None
        }
    }

    fn queries_with_bindings<'bindings>(
        &self,
        bound_positions: &[usize],
        bindings: &'bindings [Vec<AnyDataValue>],
        page_size: usize,
    ) -> impl Iterator<Item = (&'bindings [Vec<AnyDataValue>], Query)> {
        let mut queries = Vec::new();

        for page in bindings.chunks(page_size.max(1)) {
            match self.query_with_bindings(bound_positions, page) {
                Some(query) => {
                    queries.push((page, query));
                }
                None => queries.extend(self.queries_with_bindings(
                    bound_positions,
                    page,
                    page_size.div_ceil(2),
                )),
            }
        }

        queries.into_iter()
    }
}

impl ByteSized for SparqlReader {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

#[async_trait::async_trait(?Send)]
impl TableProvider for SparqlReader {
    fn arity(&self) -> usize {
        self.builder.expected_arity().unwrap_or_default()
    }

    async fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError> {
        tuple_writer.set_patterns(self.patterns.clone());
        self.load_from_query(&self.builder.query, tuple_writer)
            .await
    }

    fn should_import_with_bindings(
        &self,
        _bound_positions: &[usize],
        _num_bindings: usize,
    ) -> bool {
        true // TODO: use a better heuristic here
    }

    async fn provide_table_data_with_bindings(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
        bound_positions: &[usize],
        bindings: &[Vec<AnyDataValue>],
        _num_bindings: usize,
    ) -> Result<(), ReadingError> {
        tuple_writer.set_patterns(self.patterns.clone());
        self.load_from_bindings(bound_positions, bindings, tuple_writer)
            .await
    }
}
