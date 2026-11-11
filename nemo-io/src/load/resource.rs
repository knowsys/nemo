//! Resources: where the bytes of a load come from.

use std::{path::PathBuf, sync::Arc};

use crate::compression::Compression;

/// Where a resource's bytes come from.
#[derive(Debug, Clone)]
pub enum Resource {
    /// A file on disk.
    Path(PathBuf),
    /// Bytes already in memory, named for diagnostics.
    ///
    /// Their compression is detected from their content, not their name.
    Bytes {
        /// The name of the resource in diagnostics.
        name: String,
        /// The content.
        bytes: Arc<[u8]>,
    },
}

impl Resource {
    /// Returns the name of the resource in diagnostics.
    pub fn name(&self) -> String {
        match self {
            Resource::Path(path) => path.display().to_string(),
            Resource::Bytes { name, .. } => name.clone(),
        }
    }

    /// Returns whether the resource's stream is read through gzip.
    ///
    /// For a file, this depends on its name, and for bytes on their content.
    pub(crate) fn compressed(&self, compression: Compression) -> bool {
        match self {
            Resource::Path(path) => compression.gzip_for_name(path),
            Resource::Bytes { bytes, .. } => compression.gzip_for_bytes(bytes),
        }
    }
}
