use std::{fs::File, io::Read, path::PathBuf};

use nemo_physical::{
    error::{ReadingError, ReadingErrorKind},
    resource::Resource,
};
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
        // Add error message or is the block ok?
        let Resource::Path(path) = resource else {
            unreachable!("resource should always be a Path here")
        };
        
        if is_iri(resource) {
            if path.starts_with("file://") {
                // File URI. We only support local files, i.e., URIs
                // where the host part is either empty or `localhost`.

                let path: &str = path
                    .strip_prefix("file://localhost")
                    .or_else(|| path.strip_prefix("file://"))
                    .ok_or_else(|| {
                        ReadingError::new(ReadingErrorKind::InvalidFileUri)
                            .with_resource(resource.clone())
                    })?;
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
                    .map(|bp| bp.join(path))
                    .unwrap_or(path.into()),
            ))
        }
    }
}

impl ResourceProvider for FileResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        _media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        // Early return if Resource is not a local path
        if let Some(path) = resource.as_path(){
            let new_path = self.base_path
                    .as_ref()
                    .map(|bp| bp.join(path))
                    .unwrap_or(path.into());
            let file = File::open(&new_path)
                .map_err(|e| ReadingError::from(e).with_resource(resource.clone()))?;
            Ok(Some(Box::new(file)))
        } else {
            Ok(None)
        }
    }
}
