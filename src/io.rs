//! This module contains utility functions, like importing data from various sources

use std::io::Write;

pub mod builder_proxy;
pub mod dsv;
pub mod output_file_manager;
pub mod parser;

pub use output_file_manager::OutputFileManager;

use crate::error::Error;

use self::builder_proxy::PhysicalBuilderProxyEnum;

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

/// A general interface for reading tables from files
pub trait TableReader: std::fmt::Debug {
    /// read the table into multiple [`ColumnBuilderProxy`]
    fn read_into_builder_proxies<'a: 'b, 'b>(
        &self,
        builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), Error>;
}
