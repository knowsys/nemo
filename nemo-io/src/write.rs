//! The trait that every writer of a format implements, and the outcome of a write.

use std::{fs::File, io::Write, path::Path};

use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{AboutResource, compression::Compression, error::IoError};

/// A writer of one format, which serializes rows onto a byte stream.
///
/// Each format implements [write](Self::write) for a stream.
/// The trait provides [write_compressed](Self::write_compressed),
/// which also applies the writer's compression,
/// and [write_path](Self::write_path) for a file.
pub trait Writer {
    /// Returns the compression that [write_compressed](Self::write_compressed) and
    /// [write_path](Self::write_path) apply.
    fn compression(&self) -> Compression;

    /// Writes rows to a stream, without compressing or buffering them.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be written.
    fn write<'a>(
        &self,
        target: impl Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic>;

    /// Writes rows to a stream as [write](Self::write) does,
    /// compressed as [compression](Self::compression) says, and ends the stream.
    ///
    /// `name` is the name of the stream's resource.
    /// Under [Compression::Auto], the stream is compressed if this name ends in `.gz`,
    /// and a stream without a name is not compressed.
    /// The bytes are buffered, so a writer that writes single terms or cells
    /// does not write the target once for each of them.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be written, including when it is ended.
    fn write_compressed<'a>(
        &self,
        target: impl Write,
        name: Option<&Path>,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let mut stream = self.compression().wrap(target, name);
        let written = self.write(&mut stream, rows)?;
        stream.finish().map_err(IoError::output_unwritable)?;
        Ok(written)
    }

    /// Writes rows to a file as [write_compressed](Self::write_compressed) does,
    /// and creates its parent directories.
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

        create(path)
            .and_then(|file| self.write_compressed(file, Some(path), rows))
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }
}

/// Creates a file for writing, together with its parent directories.
///
/// # Errors
///
/// Returns an error naming the file if it or a parent directory cannot be created.
pub(crate) fn create(path: &Path) -> Result<File, Diagnostic> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| IoError::unwritable(path, &error))?;
    }

    File::create(path).map_err(|error| IoError::unwritable(path, &error))
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

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::{
        dsv::{DsvOptions, DsvWriter},
        file_format::RdfVariant,
        json::JsonWriter,
        rdf::{RdfWriter, RdfWriterOptions},
    };

    /// The number of rows each test export writes.
    const ROWS: usize = 1000;

    /// A target that takes at most `capacity` bytes, as a full disk does, and counts the calls
    /// that write to it.
    struct Disk {
        /// The bytes written so far.
        bytes: Vec<u8>,
        /// The number of bytes the target takes in total.
        capacity: usize,
        /// The number of calls to [write](Write::write).
        writes: usize,
    }

    impl Disk {
        /// Creates an empty target that takes at most `capacity` bytes.
        fn new(capacity: usize) -> Self {
            Self {
                bytes: Vec::new(),
                capacity,
                writes: 0,
            }
        }
    }

    impl Write for Disk {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            let room = self.capacity - self.bytes.len();
            if room == 0 && !bytes.is_empty() {
                return Err(io::ErrorKind::StorageFull.into());
            }

            let taken = bytes.len().min(room);
            self.bytes.extend_from_slice(&bytes[..taken]);
            Ok(taken)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A function that writes the test rows to a target,
    /// with one format's writer and one compression.
    type Export = Box<dyn Fn(&mut Disk) -> Result<Written, Diagnostic>>;

    /// Returns an export for each format that streams, uncompressed and through gzip,
    /// each with a label for assertion messages.
    fn exports() -> Vec<(String, Export)> {
        /// Returns the export of the test rows through a writer.
        fn export(writer: impl Writer + 'static) -> Export {
            Box::new(move |target| {
                let rows: Vec<Vec<DataValue>> = (0_i64..)
                    .take(ROWS)
                    .map(|row| {
                        vec![
                            DataValue::iri("http://example.org/g"),
                            DataValue::iri(format!("http://example.org/s{row}").as_str()),
                            DataValue::iri("http://example.org/p"),
                            DataValue::integer(row),
                        ]
                    })
                    .collect();
                writer.write_compressed(target, None, rows.iter().map(Vec::as_slice))
            })
        }

        let mut exports = Vec::new();
        for compression in [Compression::Plain, Compression::Gzip] {
            let mut dsv = DsvOptions::csv_terms(4);
            dsv.compression = compression;
            exports.push((format!("CSV, {compression:?}"), export(DsvWriter::new(dsv))));
            let mut rdf = RdfWriterOptions::new(RdfVariant::NQuads);
            rdf.compression = compression;
            exports.push((
                format!("N-Quads, {compression:?}"),
                export(RdfWriter::new(rdf)),
            ));
            exports.push((
                format!("JSON, {compression:?}"),
                export(JsonWriter { compression }),
            ));
        }

        exports
    }

    /// An export fails if its target cannot take the export's last byte,
    /// in every format and compression.
    ///
    /// The buffer and gzip hold bytes back until the stream ends,
    /// so the last write happens only when the stream is finished.
    #[test]
    fn a_target_without_room_for_the_last_byte_fails_the_export() {
        for (label, export) in exports() {
            let mut whole = Disk::new(usize::MAX);
            let written = export(&mut whole).expect("an unbounded target takes the export");
            assert_eq!(written.rows, ROWS as u64, "{label}");

            let mut short = Disk::new(whole.bytes.len() - 1);
            let failed = export(&mut short).expect_err(&label);
            assert_eq!(failed.code(), crate::codes::OUTPUT_UNWRITABLE, "{label}");
        }
    }

    /// An export writes its target in blocks, rather than once for every term or cell.
    #[test]
    fn exports_write_their_target_in_blocks() {
        for (label, export) in exports() {
            let mut target = Disk::new(usize::MAX);
            export(&mut target).expect("an unbounded target takes the export");
            assert!(
                target.writes < ROWS / 10,
                "{label}: {} writes for {ROWS} rows",
                target.writes
            );
        }
    }
}
