//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::management::database::sources::TableSource;

use crate::{error::Error, io::resource_providers::ResourceProviders};

use super::formats::types::ImportSpec;

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

    /// Constructs a [`TableSource`] from the given [import specificiation][ImportSpec].
    pub fn import_table(
        &self,
        import_spec: &ImportSpec,
        arity: usize,
    ) -> Result<TableSource, Error> {
        Ok(TableSource::new(
            import_spec.reader(self.resource_providers.clone())?,
            arity,
        ))
    }
}
