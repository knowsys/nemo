//! Where a load's bytes come from.

use std::{path::PathBuf, sync::Arc};

use crate::{formats::dsv::DsvOptions, stream::compression::Compression};

/// Where a resource's bytes come from.
#[derive(Debug, Clone)]
pub enum Resource {
    /// A file on disk.
    Path(PathBuf),
    /// Bytes already in memory, named for diagnostics and compression detection.
    Bytes {
        /// The name diagnostics attribute the resource to.
        name: String,
        /// The content.
        bytes: Arc<[u8]>,
    },
}

impl Resource {
    /// Returns the name diagnostics attribute this resource to.
    pub fn name(&self) -> String {
        match self {
            Resource::Path(path) => path.display().to_string(),
            Resource::Bytes { name, .. } => name.clone(),
        }
    }

    /// Returns whether the resource's stream reads through gzip:
    /// a file by its name, bytes by their content.
    pub(crate) fn compressed(&self, compression: Compression) -> bool {
        match self {
            Resource::Path(path) => compression.applies(path),
            Resource::Bytes { bytes, .. } => compression.applies_to_bytes(bytes),
        }
    }
}

/// One delimiter-separated resource of a batch.
#[derive(Debug, Clone)]
pub struct DsvResource {
    /// Where the bytes come from.
    pub input: Resource,
    /// Their shape.
    pub options: DsvOptions,
}

impl DsvResource {
    /// Creates a resource reading the given file.
    pub fn path(path: impl Into<PathBuf>, options: DsvOptions) -> Self {
        Self {
            input: Resource::Path(path.into()),
            options,
        }
    }

    /// Creates a resource reading bytes already in memory, named for diagnostics
    /// (and compression detection, under [Compression::Auto]).
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
