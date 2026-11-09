//! The trait that every writer of a format implements, and the outcome of a write.

use std::{io::Write, path::Path};

use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{
    AboutResource,
    stream::compression::{Compression, create},
};

/// A writer of one format, which serializes rows onto a byte stream.
///
/// Each format implements [write](Self::write) for a stream,
/// and the trait provides [write_path](Self::write_path) for a file.
pub trait Writer {
    /// Returns the compression the writer applies to a file.
    fn compression(&self) -> Compression;

    /// Writes rows to a stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be written.
    fn write<'a>(
        &self,
        target: impl Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic>;

    /// Writes rows to a file as [write](Self::write) does, and creates its parent directories.
    ///
    /// The file is compressed as [compression](Self::compression) says,
    /// which under [Compression::Auto] means if its name ends in `.gz`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the file if it cannot be created or written.
    fn write_path<'a>(
        &self,
        path: impl AsRef<Path>,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let path = path.as_ref();
        create(path, self.compression())
            .and_then(|target| self.write(target, rows))
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }
}

/// The outcome of one write: how many rows were written, and how many were dropped.
///
/// A row is dropped if one of its values does not fit its column's format,
/// or if the serialization has no form for the value.
#[derive(Debug, Clone, Copy, Default)]
pub struct Written {
    /// The number of rows written.
    pub rows: u64,
    /// The number of rows dropped.
    pub dropped: u64,
}
