//! This module contains utility functions, like importing data from various sources

use std::io::Write;

pub mod builder_proxy;
pub mod dsv;
pub mod output_file_manager;
pub mod parser;

pub use output_file_manager::OutputFileManager;

use crate::{
    error::Error,
    physical::{management::database::Dict, tabular::table_types::trie::Trie},
};

/// A general interface for writing records of string values
pub trait RecordWriter {
    /// Write a single record.
    fn write_record<I, T>(&mut self, record: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        // NOTE: instead of AsRef, custom conversion traits can later be set up
        T: AsRef<[u8]>;
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

/// Write the trie into the given writer
pub fn write_table<W: RecordWriter>(mut writer: W, trie: &Trie, dict: &Dict) -> Result<(), Error> {
    let mut records = trie.records();

    while let Some(record) = records.next_record(dict) {
        writer.write_record(record)?;
    }

    Ok(())
}
