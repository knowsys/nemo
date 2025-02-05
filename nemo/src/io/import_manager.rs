//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::datasources::table_providers::TableProvider;

use crate::{error::Error, io::resource_providers::ResourceProviders};

use super::formats::{FileFormatMeta, Import, ImportHandler};

/// Manages everything related to resolving the inputs of a Nemo program.
/// Currently, this is only the resource providers.
#[derive(Debug)]
pub struct ImportManager {
    resource_providers: ResourceProviders,
}

impl ImportManager {
    /// Create a new [ImportManager] from the given
    /// [resource providers][ResourceProviders].
    pub fn new(resource_providers: ResourceProviders) -> Self {
        Self { resource_providers }
    }

    /// Constructs a [TableProvider] from the given [Import].
    /// The expeced arity can reflect additional knowledge of the caller (or might be taken
    /// from the handler, if it has an arity). It is validated if the import directive is
    /// compatible with this assumption.
    pub(crate) fn table_provider_from_handler(
        &self,
        handler: &Import,
    ) -> Result<Box<dyn TableProvider>, Error> {
        let reader = self.resource_providers.open_resource(
            &handler
                .resource_spec()
                .resource()
                .expect("checked when making handler"),
            &handler.media_type(),
        )?;

        handler.reader(reader)
    }
}
