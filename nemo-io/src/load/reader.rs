//! The shape every reader of a format shares.

use std::{io::Read, path::Path};

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    AboutResource,
    load::Loaded,
    stream::compression::{Compression, open},
};

/// A reader of one format: rows from a byte stream, interned into a dictionary.
///
/// A format's reader reads a stream, and the trait reads a file the same way.
/// The condition of the reader's selection evaluates in the context the caller passes,
/// so its `NOW()` is the caller's instant.
pub trait Reader {
    /// Returns the compression the reader expects of a file.
    fn compression(&self) -> Compression;

    /// Reads all rows of a stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be read or does not hold the format.
    fn read(
        &self,
        source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic>;

    /// Reads all rows of a file as [read](Self::read) does,
    /// decompressing it as [compression](Self::compression) says or its bytes are compressed.
    ///
    /// # Errors
    ///
    /// Returns an error naming the file if it cannot be opened, or if [read](Self::read) fails.
    fn read_path(
        &self,
        path: impl AsRef<Path>,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let path = path.as_ref();
        self.read(open(path, self.compression())?, dictionary, context)
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }
}
