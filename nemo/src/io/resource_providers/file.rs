use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use nemo_physical::{error::ReadingError, resource::Resource};
use path_slash::PathBufExt;

use super::{is_iri, ResourceProvider};

/// Resolves resources from the OS-provided file system.
///
/// Handles `file:` IRIs and non-IRI, (possibly relative) file paths.
#[derive(Debug, Clone)]
pub struct FileResourceProvider {
    base_path: Option<PathBuf>,
}

impl FileResourceProvider {
    /// Create new `FileResourceProvider`
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self { base_path }
    }
}

impl FileResourceProvider {
    fn parse_resource(&self, resource: &Resource) -> Result<Option<PathBuf>, ReadingError> {
        if is_iri(resource) {
            if resource.starts_with("file://") {
                // File URI. We only support local files, i.e., URIs
                // where the host part is either empty or `localhost`.

                let path = resource
                    .strip_prefix("file://localhost")
                    .or_else(|| resource.strip_prefix("file://"))
                    .ok_or_else(|| ReadingError::InvalidFileUri(resource.to_string()))?;
                Ok(Some(PathBuf::from_slash(path)))
            } else {
                // Non-file IRI, file resource provider is not responsible
                Ok(None)
            }
        } else {
            // Not a valid URI, interpret as path directly
            Ok(Some(
                self.base_path
                    .as_ref()
                    .map(|bp| bp.join(resource))
                    .unwrap_or(resource.into()),
            ))
        }
    }
}

impl ResourceProvider for FileResourceProvider {
    fn open_resource(&self, resource: &Resource) -> Result<Option<Box<dyn BufRead>>, ReadingError> {
        // Try to parse as file IRI
        if let Some(path) = self.parse_resource(resource)? {
            let file = File::open(&path).map_err(|e| ReadingError::IOReading {
                error: e,
                filename: path.to_string_lossy().to_string(),
            })?;
            Ok(Some(Box::new(BufReader::new(file))))
        } else {
            Ok(None)
        }
    }
}
