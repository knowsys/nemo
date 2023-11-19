//! Functionality to read and write data is implemented here.
//!
//! This module acts as a mediation layer between the logical and physical layer and offers traits to allow both layers an abstract view on the io process.

use std::{io::Write, path::PathBuf};

pub mod formats;
pub mod input_manager;
pub mod output_manager;
pub mod parser;
pub mod resource_providers;

pub use input_manager::InputManager;
pub use output_manager::OutputManager;

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

/// A trait for file formats that append a format-specific file extension to a path.
pub trait PathWithFormatSpecificExtension {
    /// Returns an appropriate file extension for this format, if
    /// necessary.
    fn extension(&self) -> Option<&str>;

    /// Augment the given [path] with an extension corresponding to this
    /// format.
    fn path_with_extension(&self, path: PathBuf) -> PathBuf {
        match self.extension() {
            Some(new_extension) => path.with_extension(match path.extension() {
                Some(existing_extension) => format!(
                    "{}.{new_extension}",
                    existing_extension.to_str().expect("valid UTF-8")
                ),
                None => new_extension.to_string(),
            }),
            None => path,
        }
    }
}
