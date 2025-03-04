use std::{fs::File, io::Read, path::PathBuf};

use nemo_physical::{error::ReadingError, resource::Resource};

use super::ResourceProvider;

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

impl ResourceProvider for FileResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        _media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        // Early return if Resource is not a local path
        if resource.is_path() {
            let path = self
                .base_path
                .as_ref()
                .map(|bp| bp.join(resource.as_path()))
                .unwrap_or(resource.as_path().into());
            let file = File::open(&path)
                .map_err(|e| ReadingError::from(e).with_resource(resource.clone()))?;
            Ok(Some(Box::new(file)))
        } else {
            Ok(None)
        }
    }
}
