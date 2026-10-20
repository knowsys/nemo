//! The shape every writer of a format shares, and the outcome it reports.

use std::{io::Write, path::Path};

use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{
    AboutResource,
    stream::compression::{Compression, create},
};

/// A writer of one format: rows serialized onto a byte stream.
///
/// A format's writer writes a stream, and the trait writes a file the same way.
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

    /// Writes rows to a file as [write](Self::write) does, creating its parent directories,
    /// and compressing as [compression](Self::compression) says or the file is named `.gz`.
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

/// The outcome of one write: how many rows landed, and how many were
/// dropped because a cell did not fit its declared format or the
/// serialization has no form for it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Written {
    /// Number of rows written.
    pub rows: u64,
    /// The number of rows dropped.
    pub dropped: u64,
}
