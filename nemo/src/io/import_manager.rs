//! Management of resource providers, handling of decompression and resolution of resources to readers.

use nemo_physical::datasources::table_providers::TableProvider;

use crate::{error::Error, io::resource_providers::ResourceProviders};

use super::formats::ImportExportHandler;

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

    // /// Validates the given [ImportDirective].
    // ///
    // /// TODO: Currently, this only checks the coherence of the given settings,
    // /// without actually trying to access the resource. Some set-ups, such as WASM,
    // /// may actually want to validate without such a check, but this can be done
    // /// via `resource()`.
    // pub fn validate(&self, import_directive: &ImportDirective) -> Result<(), Error> {
    //     ImportExportHandlers::import_handler(import_directive)?;
    //     Ok(())
    // }

    // /// Returns the resource that data is to be imported from according
    // /// to this [ImportDirective].
    // pub fn resource(import_directive: &ImportDirective) -> Result<String, Error> {
    //     let handler = ImportExportHandlers::import_handler(import_directive)?;
    //     if let Some(resource) = handler.resource() {
    //         Ok(resource)
    //     } else {
    //         unreachable!("handler validation should make sure that all imports have a resource");
    //     }
    // }

    // /// Constructs a [TableProvider] from the given [ImportDirective].
    // /// The arity, if given, defines the expected arity of the data: it is validated if
    // /// the import directive is compatible with this assumption.
    // pub fn table_provider(
    //     &self,
    //     import_directive: &ImportDirective,
    //     expected_arity: Option<usize>,
    // ) -> Result<Box<dyn TableProvider>, Error> {
    //     let handler = ImportExportHandlers::import_handler(import_directive)?;

    //     let arity;
    //     if let Some(expected_arity) = expected_arity {
    //         arity = expected_arity;
    //     } else if let Some(expected_arity) = handler.predicate_arity() {
    //         arity = expected_arity;
    //     } else {
    //         // Note: this only occurs if imported data is not used in any arity-determining way, which should be rare.
    //         return Err(
    //             ImportExportError::MissingAttribute(PARAMETER_NAME_FORMAT.to_string()).into(),
    //         );
    //     }
    //     self.table_provider_from_handler(&*handler, arity)
    // }

    /// Constructs a [TableProvider] from the given [ImportExportHandler].
    /// The expeced arity can reflect additional knowledge of the caller (or might be taken
    /// from the handler, if it has an arity). It is validated if the import directive is
    /// compatible with this assumption.
    pub(crate) fn table_provider_from_handler(
        &self,
        handler: &dyn ImportExportHandler,
    ) -> Result<Box<dyn TableProvider>, Error> {
        let reader = self.resource_providers.open_resource(
            &handler.resource().expect("checked when making handler"),
            handler.compression_format(),
        )?;

        handler.reader(reader)
    }
}
