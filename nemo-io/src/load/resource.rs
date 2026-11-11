//! Resources: where the bytes of a load come from.

use std::{path::PathBuf, sync::Arc};

use crate::{compression::Compression, dsv::DsvOptions};

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

/// One delimiter-separated resource of a batch.
#[derive(Debug, Clone)]
pub struct DsvResource {
    /// Where the bytes come from.
    pub input: Resource,
    /// The options of the format, such as the delimiter.
    pub options: DsvOptions,
}

impl DsvResource {
    /// Creates a resource that reads the given file.
    pub fn path(path: impl Into<PathBuf>, options: DsvOptions) -> Self {
        Self {
            input: Resource::Path(path.into()),
            options,
        }
    }

    /// Creates a resource that reads bytes already in memory, under `name` in diagnostics.
    pub fn bytes(
        name: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
        options: DsvOptions,
    ) -> Self {
        Self {
            input: Resource::Bytes {
                name: name.into(),
                bytes: bytes.into(),
            },
            options,
        }
    }
}
