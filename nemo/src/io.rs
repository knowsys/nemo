//! Functionality to read and write data is implemented here.
//!
//! This module acts as a mediation layer between the logical and physical layer and offers traits to allow both layers an abstract view on the io process.

use std::io::Write;

pub mod dsv;
pub mod ntriples;
pub mod output_file_manager;
pub mod parser;

pub use output_file_manager::OutputFileManager;

use nemo_physical::dictionary::value_serializer::TrieSerializer;

use crate::error::Error;

/// A general interface for writing records of string values.
pub trait RecordWriter {
    /// Write a single record.
    fn write_record<I, T>(&mut self, record: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        // NOTE: instead of AsRef, custom conversion traits can later be set up
        T: AsRef<[u8]>;

    /// Write a trie.
    fn write_trie(&mut self, mut trie: impl TrieSerializer) -> Result<(), Error> {
        while let Some(record) = trie.next_serialized() {
            self.write_record(record)?;
        }

        Ok(())
    }
}

impl<W: Write> RecordWriter for csv::Writer<W> {
    fn write_record<I, T>(&mut self, record: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        // NOTE: instead of AsRef, custom conversion traits can later be set up
        T: AsRef<[u8]>,
    {
        self.write_record(record)?;
        Ok(())
    }
}

#[macro_export]
/// Transparently read from a file that might be compressed.
///
/// # Example
///
/// ```ignore
/// read_from_possibly_compressed_file!(file, compressed: |crdr| c, uncompressed: |urdr| u)
/// ```
/// reads from `file`, a [`PathBuf`][`std::path::PathBuf`]. If it is a
/// gzip-compressed file, this calls `c` with `crdr` bound to the
/// [`GzDecoder`][`flate2::read::GzDecoder`]. If it is not a
/// gzip-compressed file, call `u` with `urdr` bound to the
/// [`File`][`std::fs::File`].
///
/// ```ignore
/// read_from_possibly_compressed_file!(file, |rdr| a)
/// ```
/// reads from `file`, a [`Path`][`std::path::PathBuf`]. This calls
/// `a` with `rdr` bound to either the
/// [`GzDecoder`][`flate2::read::GzDecoder`] or to the
/// [`File`][`std::fs::File`].
macro_rules! read_from_possibly_compressed_file {
    ($file:expr, compressed: |$crdr:ident| $c:block, uncompressed: |$urdr:ident| $u:block $(,)?) => {{
        let $crdr =
            flate2::read::GzDecoder::new(std::fs::File::open($file.clone()).map_err(|error| {
                $crate::error::ReadingError::IOReading {
                    error,
                    filename: $file
                        .clone()
                        .to_str()
                        .expect("Path should be valid UTF-8")
                        .into(),
                }
            })?);

        if $crdr.header().is_some() {
            // file is compressed, read from here
            $c
        } else {
            let $urdr = std::fs::File::open($file.clone())?;
            $u
        }
    }};

    ($file:expr, |$rdr:ident| $a:block $(,)?) => {{
        read_from_possibly_compressed_file!($file, compressed: |$rdr| $a, uncompressed: |$rdr| $a)
    }};
}
