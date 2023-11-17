//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::management::database::TableSource;

use crate::{
    error::Error,
    io::{formats::types::ImportSpec, resource_providers::ResourceProviders},
    model::{PrimitiveType, TupleConstraint},
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

    /// Constructs a [`TableSource`] from the given [import specificiation][ImportExportSpec].
    pub fn import_table(
        &self,
        import_spec: &ImportSpec,
        inferred_types: Vec<PrimitiveType>,
    ) -> Result<TableSource, Error> {
        Ok(TableSource::FileReader(import_spec.reader(
            self.resource_providers.clone(),
            &TupleConstraint::exact(inferred_types),
        )?))
    }
}
