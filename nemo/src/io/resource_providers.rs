//! Resource providers for external resources that can be used in reasoning.

use std::{io::Read, rc::Rc};

use flate2::read::MultiGzDecoder;

use crate::io::parser::{all_input_consumed, iri::iri};
use nemo_physical::{error::ReadingError, table_reader::Resource};

/// A resource provider for files.
pub mod file;
/// A resource provider for HTTP(s) requests.
pub mod http;

fn is_iri(resource: &Resource) -> bool {
    all_input_consumed(iri)(resource).is_ok()
}

/// Allows resolving resources to readers.
///
/// This allows specifying how to resolve a resource independent of how the file format is going to be parsed.
pub trait ResourceProvider: std::fmt::Debug {
    /// Resolve and open a resource.
    ///
    /// This function may be called multiple times in a row, e.g. when testing if a file can be opened using gzip.
    ///
    /// The implementation can decide wether ir wants to handle the given resource, otherwise it can return `None`, and the next `ResourceProvider` will be consulted.
    fn open_resource(&self, resource: &Resource) -> Result<Option<Box<dyn Read>>, ReadingError>;
}

/// A list of [`ResourceProvider`] sorted by decreasing priority.
///
/// This allows resolving a given resource, which may occur in a Nemo program,
/// to a reader (which return the actual by of e.g. a referenced file).
///
/// The list of [`ResourceProviders`] can be customized by users of the Rust nemo crate.
#[derive(Debug, Clone)]
pub struct ResourceProviders(Rc<Vec<Box<dyn ResourceProvider>>>);

impl ResourceProviders {
    /// Construct using a list of [`ResourceProvider`]s
    pub fn from(r: Vec<Box<dyn ResourceProvider>>) -> Self {
        Self(Rc::new(r))
    }

    /// Returns instance which is unable to resolve any resources.
    pub fn empty() -> Self {
        Self(Rc::new(vec![]))
    }

    /// Resolves a resource.
    ///
    /// First checks if the resource can be opened as gzip, otherwise opens the file directly.
    pub fn open_resource(
        &self,
        resource: &Resource,
        try_gzip: bool,
    ) -> Result<Box<dyn Read>, ReadingError> {
        for resource_provider in self.0.iter() {
            if let Some(reader) = resource_provider.open_resource(resource)? {
                if !try_gzip {
                    return Ok(reader);
                }

                // Try opening with gzip
                let gz_reader = MultiGzDecoder::new(reader);

                if gz_reader.header().is_some() {
                    return Ok(Box::new(gz_reader));
                } else {
                    // Try again without gzip, otherwise go to next provider
                    if let Some(reader) = resource_provider.open_resource(resource)? {
                        return Ok(reader);
                    };
                }
            }
        }

        Err(ReadingError::ResourceNotProvided {
            resource: resource.clone(),
        })
    }
}

impl Default for ResourceProviders {
    fn default() -> Self {
        Self(Rc::new(vec![
            Box::<http::HTTPResourceProvider>::default(),
            Box::<file::FileResourceProvider>::default(),
        ]))
    }
}
