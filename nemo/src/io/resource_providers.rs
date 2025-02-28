//! Resource providers for external resources that can be used in reasoning.

use std::{io::Read, path::PathBuf, rc::Rc};

// use crate::io::parser::{all_input_consumed, iri::iri};
use nemo_physical::{
    error::{ReadingError, ReadingErrorKind},
    resource::Resource,
};

/// A resource provider for files.
pub mod file;
/// A resource provider for HTTP(s) requests.
pub mod http;

/// Helper function that determines whether resource has the form of an iri.
///
/// For now, we don't validate the exact requirements but simply check
/// whether the string contains a `:`.
///
/// TODO: Revise if deemed necessary.
fn is_iri(resource: &Resource) -> bool {
    match resource {
        Resource::Path(path) => path.contains(':'),
        // TODO: We would expect Resource::Iri to be an iri. However is this verified somewhere already?
        Resource::Iri { iri, .. } => iri.to_string().contains(':'),
    }
}

/// Allows resolving resources to readers.
///
/// This allows specifying how to resolve a resource independent of how the
/// file format is going to be parsed.
pub trait ResourceProvider: std::fmt::Debug {
    /// Resolve and open a resource.
    ///
    /// The method may fail in two ways: by returning `Ok(None)` or by returning
    /// an error. The provider should inspect the resource to decide if it is
    /// responsible, and use `None` if it isn't. So `None` signifies that the given
    /// resource is not supported.
    ///
    /// If the resource is supported, the provider must try to open it with the
    /// given compression format, and return an error if this fails.
    fn open_resource(
        &self,
        resource: &Resource,
        media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError>;
}

/// A list of [ResourceProvider] sorted by decreasing priority.
///
/// This allows resolving a given resource, which may occur in a Nemo program,
/// to a reader (which return the actual by of e.g. a referenced file).
///
/// The list of [ResourceProviders] can be customized by users of the Rust nemo crate.
#[derive(Debug, Clone)]
pub struct ResourceProviders(Rc<Vec<Box<dyn ResourceProvider>>>);

impl ResourceProviders {
    /// Construct using a list of [ResourceProvider]s
    pub fn from(r: Vec<Box<dyn ResourceProvider>>) -> Self {
        Self(Rc::new(r))
    }

    /// Construct default with a base path for the `FileResourceProvider`
    pub fn with_base_path(base_path: Option<PathBuf>) -> Self {
        Self(Rc::new(vec![
            Box::<http::HttpResourceProvider>::default(),
            Box::new(file::FileResourceProvider::new(base_path)),
        ]))
    }

    /// Returns instance which is unable to resolve any resources.
    pub fn empty() -> Self {
        Self(Rc::new(vec![]))
    }

    /// Opens a resource.
    pub fn open_resource(
        &self,
        resource: &Resource,
        media_type: &str,
    ) -> Result<Box<dyn Read>, ReadingError> {
        for resource_provider in self.0.iter() {
            if let Some(reader) = resource_provider.open_resource(resource, media_type)? {
                return Ok(reader);
            }
        }

        Err(ReadingError::new(ReadingErrorKind::ResourceNotProvided)
            .with_resource(resource.clone()))
    }
}

impl Default for ResourceProviders {
    fn default() -> Self {
        Self::with_base_path(Default::default())
    }
}
