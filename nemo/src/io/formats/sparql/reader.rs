//! Reader for resources of type SPARQL (SPARQL query language for RDF).

use std::io::Read;

use itertools::Itertools;
use nemo_physical::datasources::bindings::{Bindings, ProductBindings};
use nemo_physical::datasources::tuple_writer::TupleWriter;
use nemo_physical::error::ReadingErrorKind;
use nemo_physical::management::bytesized::ByteSized;
use nemo_physical::meta::timing::TimedCode;
use nemo_physical::tabular::filters::FilterTransformPattern;
use nemo_physical::{datasources::table_providers::TableProvider, error::ReadingError};
use oxiri::Iri;
use spargebra::Query;

use crate::io::format_builder::FormatBuilder;
use crate::io::formats::dsv::reader::DsvReader;
use crate::io::formats::dsv::value_format::DsvValueFormats;
use crate::io::resource_providers::ResourceProvider;
use crate::io::resource_providers::http::HttpResourceProvider;
use crate::rule_model::components::import_export::Direction;
use crate::syntax::import_export::file_format::MEDIA_TYPE_TSV;

use super::queries::{pattern_with_bindings, pattern_with_filters};
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
        log::debug!("sending SPARQL query: {query}");
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
        unbound_query: &Query,
        bindings: &ProductBindings,
        tuple_writer: &mut TupleWriter<'_>,
    ) -> Result<(), ReadingError> {
        for (page, query) in
            self.queries_with_bindings(unbound_query, bindings, MAX_BINDINGS_PER_PAGE)
        {
            let result = self.load_from_query(&query, tuple_writer).await;

            // TODO(mam): detect timeouts here and reduce page size
            // cf. https://github.com/knowsys/nemo/issues/712
            if let Err(error) = result
                && let ReadingErrorKind::HttpTransfer(error) = error.kind()
                && let Some(code) = error.status()
                && code == reqwest::StatusCode::PAYLOAD_TOO_LARGE
            {
                if bindings.count() == 1 {
                    return Err(ReadingError::new_external(Box::new(error)));
                }

                // the page size is still too large, try half
                for page in page.chunks(page.len().div_ceil(2).max(1)) {
                    Box::pin(self.load_from_bindings(
                        unbound_query,
                        &ProductBindings::product(page.iter().map(|bindings| {
                            (Bindings::empty(bindings.positions()), bindings.clone())
                        })),
                        tuple_writer,
                    ))
                    .await?;
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

    fn query_with_bindings(&self, query: &Query, bindings: &[Bindings]) -> Option<Query> {
        let query = match query {
            q @ &Query::Construct { .. } | q @ &Query::Describe { .. } => q.clone(),
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => Query::Select {
                dataset: dataset.clone(),
                base_iri: base_iri.clone(),
                pattern: pattern_with_bindings(pattern, bindings),
            },
            Query::Ask {
                dataset,
                pattern,
                base_iri,
            } => Query::Ask {
                dataset: dataset.clone(),
                base_iri: base_iri.clone(),
                pattern: pattern_with_bindings(pattern, bindings),
            },
        };

        if query.to_string().len() < QUERY_PAGE_CHAR_LIMIT {
            Some(query)
        } else {
            None
        }
    }

    fn bindings_chunks(bindings: &[Bindings], page_size: usize) -> Vec<Vec<Bindings>> {
        let sorted = bindings
            .iter()
            .enumerate()
            .sorted_by_cached_key(|(_, bindings)| bindings.count())
            .collect::<Vec<_>>();

        let split_index = sorted
            .iter()
            .enumerate()
            .scan(1, |count, (idx, (_, bindings))| {
                *count *= bindings.count();
                Some((idx, *count))
            })
            .find(|(_, count)| *count > page_size);

        if let Some((split_index, _)) = split_index {
            let mut result = Vec::new();
            let (prefix, rest) = sorted.split_at(split_index);
            let prefix_count = prefix
                .iter()
                .map(|(_, bindings)| bindings.count())
                .product::<usize>();
            let remaining_size = page_size.div_ceil(prefix_count).max(1);
            let the_prefix = prefix
                .iter()
                .map(|(_, bindings)| (*bindings).clone())
                .collect::<Vec<_>>();

            if rest.len() > 1 {
                for infix in rest[0..rest.len() - 1]
                    .iter()
                    .map(|(_, bindings)| bindings.chunks(1).collect::<Vec<_>>())
                    .multi_cartesian_product()
                {
                    for chunk in rest[rest.len()].1.chunks(remaining_size) {
                        let mut the_result = the_prefix.clone();
                        the_result.extend(infix.clone());
                        the_result.push(chunk);
                        result.push(the_result);
                    }
                }
            } else {
                for chunk in rest[0].1.chunks(remaining_size) {
                    let mut the_result = the_prefix.clone();
                    the_result.push(chunk);
                    result.push(the_result);
                }
            }

            result
        } else {
            vec![Vec::from(bindings)]
        }
    }

    fn queries_with_bindings(
        &self,
        query: &Query,
        bindings: &ProductBindings,
        page_size: usize,
    ) -> impl Iterator<Item = (Vec<Bindings>, Query)> {
        let mut queries = Vec::new();

        for combination in bindings.combinations() {
            for page in Self::bindings_chunks(&combination, page_size.max(1)) {
                match self.query_with_bindings(query, &page) {
                    Some(query) => {
                        queries.push((page.clone(), query));
                    }
                    None => queries.extend(self.queries_with_bindings(
                        query,
                        bindings,
                        page_size.div_ceil(2),
                    )),
                }
            }
        }

        queries.into_iter()
    }

    fn query_with_filters(&self) -> (Query, Vec<FilterTransformPattern>) {
        match &self.builder.query {
            Query::Select {
                dataset,
                pattern,
                base_iri,
            } => {
                let (pattern, _patterns) = pattern_with_filters(pattern, &self.patterns);
                (
                    Query::Select {
                        dataset: dataset.clone(),
                        pattern,
                        base_iri: base_iri.clone(),
                    },
                    self.patterns.clone(),
                )
            }
            q @ Query::Construct { .. } | q @ Query::Describe { .. } | q @ Query::Ask { .. } => {
                (q.clone(), self.patterns.clone())
            }
        }
    }
}

impl ByteSized for SparqlReader {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

#[async_trait::async_trait(?Send)]
impl TableProvider for SparqlReader {
    fn input_arity(&self) -> usize {
        self.builder.expected_arity().unwrap_or_default()
    }

    fn output_arity(&self) -> usize {
        if let Some(pattern) = self.patterns.first() &&
        let Some(arity) = pattern.expected_arity() {
            return arity;
        }

        self.input_arity()
    }

    async fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError> {
        let (query, patterns) = self.query_with_filters();
        tuple_writer.set_patterns(patterns);
        self.load_from_query(&query, tuple_writer).await
    }

    fn should_import_with_bindings(
        &self,
        _bound_positions: &[Vec<usize>],
        _num_bindings: usize,
    ) -> bool {
        true // TODO: use a better heuristic here
    }

    async fn provide_table_data_with_bindings(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
        bindings: &ProductBindings,
    ) -> Result<(), ReadingError> {
        let (query, patterns) = self.query_with_filters();
        tuple_writer.set_patterns(patterns);
        self.load_from_bindings(&query, bindings, tuple_writer)
            .await
    }
}
