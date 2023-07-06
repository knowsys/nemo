//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::management::database::TableSource;

use crate::{
    error::Error,
    io::{
        formats::{DSVReader, RDFTriplesReader},
        resource_providers::ResourceProviders,
    },
    model::{DataSource, PrimitiveType},
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

    /// Constructs a [`TableSource`] using the correct readers for a given [`DataSource`]
    pub fn load_table_source(
        &self,
        data_source: &DataSource,
        logical_types: Vec<PrimitiveType>,
    ) -> Result<TableSource, Error> {
        match data_source {
            DataSource::DsvFile {
                resource,
                delimiter,
            } => {
                let dsv_reader = DSVReader::dsv(
                    self.resource_providers.clone(),
                    resource.clone(),
                    *delimiter,
                    logical_types,
                );
                Ok(TableSource::FileReader(Box::new(dsv_reader)))
            }
            DataSource::RdfFile { resource, base } => {
                Ok(TableSource::FileReader(Box::new(RDFTriplesReader::new(
                    self.resource_providers.clone(),
                    resource.clone(),
                    base.clone(),
                ))))
            }
            DataSource::SparqlQuery(_) => {
                todo!("SPARQL query data sources are not yet implemented")
            }
        }
    }
}
