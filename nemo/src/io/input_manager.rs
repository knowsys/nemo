//! Management of resource providers, handling of decompression and resolution of resources to readers.

use std::{io::Read, path::PathBuf, rc::Rc};

use std::fs::File;

use flate2::read::GzDecoder;
use nemo_physical::{
    error::ReadingError, management::database::TableSource, table_reader::Resource,
};
use path_slash::PathBufExt;

use crate::{error::Error, model::DataSource, types::LogicalTypeEnum};

use super::{
    dsv::DSVReader,
    ntriples::NTriplesReader,
    parser::{all_input_consumed, iri::iri},
};

/// Manages everything related to resolving the inputs of a Nemo program.
/// Currently, this is only the resource providers.
#[derive(Debug)]
pub struct InputManager {
    resource_providers: ResourceProviders,
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
                let gz_reader = GzDecoder::new(reader);

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
            Box::<HTTPResourceProvider>::default(),
            Box::<FileResourceProvider>::default(),
        ]))
    }
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

fn is_iri(resource: &Resource) -> bool {
    all_input_consumed(iri)(resource).is_ok()
}

/// Resolves resources from the OS-provided file system.
///
/// Handles `file:` IRIs and non-IRI, (possibly relative) file paths.
#[derive(Debug, Clone, Copy, Default)]
pub struct FileResourceProvider {}

impl ResourceProvider for FileResourceProvider {
    fn open_resource(&self, resource: &Resource) -> Result<Option<Box<dyn Read>>, ReadingError> {
        // Try to parse as file IRI
        let path = if is_iri(resource) {
            if resource.starts_with("file://") {
                // File URI. We only support local files, i.e., URIs
                // where the host part is either empty or `localhost`.

                let path = resource
                    .strip_prefix("file://localhost")
                    .or_else(|| resource.strip_prefix("file://"))
                    .ok_or_else(|| ReadingError::InvalidFileUri(resource.to_string()))?;
                PathBuf::from_slash(path)
            } else {
                // Non-file IRI, file resource provider is not responsible
                return Ok(None);
            }
        } else {
            // Not a valid URI, interpret as path directly
            PathBuf::from(resource)
        };

        let file = File::open(path)?;
        Ok(Some(Box::new(file)))
    }
}

/// Resolves resources using HTTP or HTTPS.
///
/// Handles `http:` and `https:` IRIs.
#[derive(Debug, Clone, Copy, Default)]
pub struct HTTPResourceProvider {}

impl ResourceProvider for HTTPResourceProvider {
    fn open_resource(&self, resource: &Resource) -> Result<Option<Box<dyn Read>>, ReadingError> {
        if !is_iri(resource) {
            return Ok(None);
        }

        if !(resource.starts_with("http:") || resource.starts_with("https:")) {
            // Non-http IRI, resource provider is not responsible
            return Ok(None);
        }

        #[cfg(not(feature = "http-resource-provider"))]
        {
            panic!("Using the HTTPResourceProvider requires the http-resource-provider feature")
        }
        #[cfg(feature = "http-resource-provider")]
        {
            let response = reqwest::blocking::get(resource).unwrap();
            Ok(Some(Box::new(response)))
        }
    }
}

// See https://doc.rust-lang.org/cargo/reference/features.html#mutually-exclusive-features
#[cfg(all(feature = "js", feature = "http-resource-provider"))]
compile_error!("feature \"js\" and feature \"http-resource-provider\" cannot be enabled at the same time, because the \"reqwest\" crate does not support web assembly environments with the \"blocking\" feature");

impl InputManager {
    #[allow(missing_docs)]
    pub fn new(resource_providers: ResourceProviders) -> Self {
        Self { resource_providers }
    }

    /// Constructs a [`TableSource`] using the correct readers for a given [`DataSource`]
    pub fn load_table_source(
        &self,
        data_source: &DataSource,
        logical_types: Vec<LogicalTypeEnum>,
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
            DataSource::RdfFile(resource) => Ok(TableSource::FileReader(Box::new(
                NTriplesReader::new(self.resource_providers.clone(), resource.clone()),
            ))),
            DataSource::SparqlQuery(_) => {
                todo!("SPARQL query data sources are not yet implemented")
            }
        }
    }
}
