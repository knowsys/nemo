use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use nemo_physical::{error::ReadingError, resource::Resource};
use path_slash::PathBufExt;

use crate::rule_model::components::import_export::compression::CompressionFormat;

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
        // Add error message or is the block ok?
        let Resource::Path(res_path) = resource else {unreachable!("resource should always be a Path here")};
        if is_iri(resource) {
            if res_path.starts_with("file://") {
                // File URI. We only support local files, i.e., URIs
                // where the host part is either empty or `localhost`.

                let path: &str = res_path
                    .strip_prefix("file://localhost")
                    .or_else(|| res_path.strip_prefix("file://"))
                    .ok_or_else(|| ReadingError::InvalidFileUri {resource: resource.clone()})?;
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
                    .map(|bp| bp.join(res_path))
                    .unwrap_or(res_path.into()),
            ))
        }
    }
}

impl ResourceProvider for FileResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        compression: CompressionFormat,
        _media_type: &str,
    ) -> Result<Option<Box<dyn BufRead>>, ReadingError> {
        // Try to parse as file IRI
        if let Some(path) = self.parse_resource(resource)? {
            let file = File::open(&path).map_err(|e| ReadingError::IoReading {
                error: e,
                filename: path.to_string_lossy().to_string(),
            })?;
            // Opening succeeded. Apply decompression:
            if let Some(reader) = compression.try_decompression(BufReader::new(file)) {
                Ok(Some(reader))
            } else {
                Err(ReadingError::Decompression {
                    resource: resource.to_owned(),
                    decompression_format: compression.to_string(),
                })
            }
        } else {
            Ok(None)
        }
    }
}
