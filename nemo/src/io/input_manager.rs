//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::{
    datasources::{TableProvider, TableWriter},
    error::ReadingError,
    management::database::TableSource,
};

use crate::{
    error::Error,
    io::{
        formats::{DSVReader, RDFTriplesReader},
        resource_providers::ResourceProviders,
    },
    model::NativeDataSource,
};

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

    /// Constructs a [`TableSource`] using the correct readers for a given [`NativeDataSource`]
    pub fn load_native_table_source(
        &self,
        data_source: NativeDataSource,
    ) -> Result<TableSource, Error> {
        let resolver = Box::new(NativeDataSourceResolver {
            resource_providers: self.resource_providers.clone(),
            data_source,
        });

        Ok(TableSource::FileReader(resolver))
    }
}

/// Implements [`TableProvider`] by resolving the [`NativeDataSource`] upon read request.
#[derive(Debug)]
pub struct NativeDataSourceResolver {
    resource_providers: ResourceProviders,
    data_source: NativeDataSource,
}

impl NativeDataSourceResolver {
    fn resolve_data_source(self) -> Result<Box<dyn TableProvider>, ReadingError> {
        match &self.data_source {
            NativeDataSource::DsvFile(dsv_file) => {
                let dsv_reader = DSVReader::dsv(self.resource_providers.clone(), dsv_file);
                Ok(Box::new(dsv_reader))
            }
            NativeDataSource::RdfFile(rdf_file) => {
                let rdf_reader = RDFTriplesReader::new(self.resource_providers.clone(), rdf_file);
                Ok(Box::new(rdf_reader))
            }
            NativeDataSource::SparqlQuery(_) => {
                todo!("SPARQL query data sources are not yet implemented")
            }
        }
    }
}

impl TableProvider for NativeDataSourceResolver {
    fn provide_table_data(
        self: Box<Self>,
        table_writer: &mut TableWriter,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table_reader = self.resolve_data_source()?;
        table_reader.provide_table_data(table_writer)
    }
}
