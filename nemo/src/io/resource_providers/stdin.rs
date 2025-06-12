use super::ResourceProvider;
use nemo_physical::{error::ReadingError, resource::Resource};
use std::io::Read;

#[derive(Debug, Clone, Copy, Default)]
/// Resolves resources for Stdin
pub struct StdinResourceProvider {}
impl ResourceProvider for StdinResourceProvider {
    fn open_resource(
        &self,
        resource: &Resource,
        _media_type: &str,
    ) -> Result<Option<Box<dyn Read>>, ReadingError> {
        if !resource.is_pipe() {
            return Ok(None);
        }
        let stdin = std::io::stdin();
        let handle = stdin.lock();
        Ok(Some(Box::new(handle)))
    }
}
