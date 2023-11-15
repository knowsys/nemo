//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::{
    error::ReadingError, management::database::TableSource, table_reader::TableReader,
};

use crate::{
    error::Error,
    io::{
        formats::{types::Direction, DSVReader, RDFTriplesReader},
        resource_providers::ResourceProviders,
    },
    model::{NativeDataSource, PrimitiveType, TupleConstraint, TypeConstraint},
};

use super::formats::types::ImportExportSpec;

/// Manages everything related to resolving the inputs of a Nemo program.
/// Currently, this is only the resource providers.
#[derive(Debug)]
pub struct InputManager {
    resource_providers: ResourceProviders,
}

impl InputManager {
    /// Create a new [input manager][InputManager] from the given
    /// [resource providers][ResourceProviders].
    pub fn new(resource_providers: ResourceProviders) -> Self {
        Self { resource_providers }
    }

    /// Constructs a [`TableSource`] from the given [import specificiation][ImportExportSpec].
    pub fn import_table(
        &self,
        import_spec: &ImportExportSpec,
        inferred_types: Vec<PrimitiveType>,
    ) -> Result<TableSource, Error> {
        assert_eq!(import_spec.direction, Direction::Reading);

        Ok(TableSource::FileReader(import_spec.reader(
            self.resource_providers.clone(),
            &TupleConstraint::from_iter(inferred_types.into_iter().map(TypeConstraint::Exact)),
        )?))
    }

    /// Constructs a [`TableSource`] using the correct readers for a given [`NativeDataSource`]
    pub fn load_native_table_source(
        &self,
        data_source: NativeDataSource,
        logical_types: Vec<PrimitiveType>,
    ) -> Result<TableSource, Error> {
        let resolver = Box::new(NativeDataSourceResolver {
            resource_providers: self.resource_providers.clone(),
            data_source,
            logical_types,
        });

        Ok(TableSource::FileReader(resolver))
    }
}

/// Implements [TableReader] by resolving the [NativeDataSource] upon read request.
#[derive(Debug)]
pub struct NativeDataSourceResolver {
    resource_providers: ResourceProviders,
    data_source: NativeDataSource,
    logical_types: Vec<PrimitiveType>,
}

impl NativeDataSourceResolver {
    fn resolve_data_source(self) -> Result<Box<dyn TableReader>, ReadingError> {
        match &self.data_source {
            NativeDataSource::DsvFile(dsv_file) => {
                let dsv_reader = DSVReader::dsv(
                    self.resource_providers.clone(),
                    dsv_file,
                    self.logical_types.clone(),
                );
                Ok(Box::new(dsv_reader))
            }
            NativeDataSource::RdfFile(rdf_file) => {
                let rdf_reader = RDFTriplesReader::new(
                    self.resource_providers.clone(),
                    rdf_file,
                    self.logical_types.clone(),
                );
                Ok(Box::new(rdf_reader))
            }
            NativeDataSource::SparqlQuery(_) => {
                todo!("SPARQL query data sources are not yet implemented")
            }
        }
    }
}

impl TableReader for NativeDataSourceResolver {
    fn read_into_builder_proxies<'a: 'b, 'b>(
        self: Box<Self>,
        builder_proxies: &'b mut Vec<nemo_physical::builder_proxy::PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError> {
        let table_reader = self.resolve_data_source()?;
        table_reader.read_into_builder_proxies(builder_proxies)
    }
}
