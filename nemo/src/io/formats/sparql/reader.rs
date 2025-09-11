//! Reader for resources of type SPARQL (SPARQL query language for RDF).

use std::io::{BufReader, Read};

use nemo_physical::datasources::table_providers::TableProvider;
use nemo_physical::error::ReadingError;
use nemo_physical::management::bytesized::ByteSized;
use nemo_physical::resource::ResourceBuilder;
use nemo_physical::{datasources::tuple_writer::TupleWriter, datavalues::AnyDataValue};
use oxiri::Iri;
use spargebra::Query;

use crate::chase_model::components::rule::ChaseRule;
use crate::error::Error;
use crate::io::format_builder::FormatBuilder;
use crate::io::formats::dsv::reader::DsvReader;
use crate::io::formats::dsv::value_format::DsvValueFormats;
use crate::io::resource_providers::http::HttpResourceProvider;
use crate::io::resource_providers::ResourceProvider;
use crate::rule_model::components::import_export::Direction;
use crate::syntax::import_export::file_format::MEDIA_TYPE_TSV;

use super::SparqlBuilder;

#[derive(Debug)]
pub(crate) struct SparqlReader {
    builder: SparqlBuilder,
    filter_rules: Vec<ChaseRule>,
}

impl SparqlReader {
    pub fn new(builder: SparqlBuilder, filter_rules: Vec<ChaseRule>) -> Self {
        Self {
            builder,
            filter_rules,
        }
    }

    fn execute_from_builder(
        builder: &SparqlBuilder,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        let resource_builder = builder
            .customize_resource_builder(Direction::Import, None)
            .expect("should have been validated")
            .expect("should create a resource builder");
        let resource = resource_builder.finalize();
        let provider = HttpResourceProvider {};

        Ok(provider.open_resource(&resource, MEDIA_TYPE_TSV)?)
    }

    fn execute_query(
        &self,
        endpoint: &Iri<String>,
        query: &Query,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        let builder = SparqlBuilder::with_endpoint_and_query(
            endpoint.clone(),
            query.clone(),
            self.builder.value_formats.clone(),
        );

        Self::execute_from_builder(&builder)
    }

    fn read_table_data(
        read: Box<dyn Read>,
        tuple_writer: &mut TupleWriter,
        value_formats: Option<DsvValueFormats>,
    ) -> Result<(), ReadingError> {
        let reader = DsvReader::new(
            Box::new(BufReader::new(read)),
            b'\t',
            value_formats.unwrap_or(DsvValueFormats::default(tuple_writer.column_number())),
            None,
            None,
            true,
            false,
        );

        reader.read(tuple_writer)
    }
}

impl ByteSized for SparqlReader {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

impl TableProvider for SparqlReader {
    fn arity(&self) -> usize {
        self.builder.expected_arity().unwrap_or_default()
    }

    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError> {
        let response = self
            .execute_query(&self.builder.endpoint, &self.builder.query)?
            .expect("should not be empty");
        Self::read_table_data(response, tuple_writer, self.builder.value_formats)
    }

    fn should_import_with_bindings(
        &self,
        _bound_positions: &[usize],
        _num_bindings: usize,
    ) -> bool {
        true // TODO: use a better heuristic here
    }

    fn provide_table_data_with_bindings(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
        bound_positions: &[usize],
        bindings: &[Vec<AnyDataValue>],
        num_bindings: usize,
    ) -> Result<(), ReadingError> {
        log::debug!("doing SPARQL query with given {num_bindings} bindings: {bindings:?}");

        let mut query = self.builder.query.clone();

        let response = self
            .execute_query(&self.builder.endpoint, &query)?
            .expect("should not be empty");
        Self::read_table_data(response, tuple_writer, self.builder.value_formats)
    }
}
