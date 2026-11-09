//! The trait that every reader of a format implements.

use std::{io::Read, path::Path};

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    AboutResource,
    load::Loaded,
    stream::compression::{Compression, open},
};

/// A reader of one format, which reads rows from a byte stream and interns their values into a
/// dictionary.
///
/// Each format implements [read](Self::read) for a stream,
/// and the trait provides [read_path](Self::read_path) for a file.
/// The condition of the reader's selection is evaluated in the `context` that a read is given,
/// so its `NOW()` is the instant of that context.
pub trait Reader {
    /// Returns the compression the reader expects of a file.
    fn compression(&self) -> Compression;

    /// Reads all rows of a stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be read or is not in the format.
    fn read(
        &self,
        source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic>;

    /// Reads all rows of a file as [read](Self::read) does.
    ///
    /// The file is decompressed as [compression](Self::compression) says,
    /// which under [Compression::Auto] means if its name ends in `.gz`.
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
