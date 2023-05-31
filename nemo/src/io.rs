//! Functionality to read and write data is implemented here.
//!
//! This module acts as a mediation layer between the logical and physical layer and offers traits to allow both layers an abstract view on the io process.

use std::io::Write;

pub mod dsv;
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
