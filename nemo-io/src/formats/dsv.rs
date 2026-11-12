//! Delimiter-separated values (DSV), the tabular formats such as CSV and TSV.
//!
//! The *width* of a resource is the number of its columns,
//! and the *arity* is the number of columns that a reader delivers.

mod batch;
mod layout;
mod reader;
mod record_ends;
mod run;
mod writer;

pub use batch::{DsvBatch, DsvResource};
pub(crate) use layout::ColumnLayout;
pub use reader::DsvReader;
pub use writer::{DsvSink, DsvWriter, DsvWriterOptions};

use std::{io::Read, path::Path};

use csv::ReaderBuilder;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    AboutResource,
    compression::Compression,
    error::IoError,
    selection::{RowCondition, RowSelection},
    value_format::{ColumnFormat, ValueFormat},
};

/// The options of a delimiter-separated resource for reading, such as its delimiter and one
/// [ColumnFormat] per column.
///
/// A [Skip](ValueFormat::Skip) column counts toward the width but not toward the arity.
#[derive(Debug, Clone)]
pub struct DsvReaderOptions {
    /// The delimiter between the cells of a record.
    pub delimiter: u8,
    /// The format of each column of the resource.
    pub formats: Vec<ColumnFormat>,
    /// For each column of the resource, whether its cells are only checked.
    ///
    /// A column that is only checked is not delivered,
    /// but a cell that its format rejects still makes the row malformed.
    /// Checking a column rather than skipping it keeps the set of well-formed rows the same,
    /// whichever columns are delivered.
    /// A column past the end of the list is not only checked, so the list may be empty.
    pub checked_only: Vec<bool>,
    /// Whether `"` quotes cells, rather than being an ordinary character.
    pub quoting: bool,
    /// The names of the columns, in the order of [formats](Self::formats),
    /// or `None` for a resource whose columns go by position.
    ///
    /// The first record is then read as the header, and each column is found by its name,
    /// whatever its position. A column of the resource that is not named is not read.
    /// The name of a [Skip](ValueFormat::Skip) column is not looked up.
    pub columns: Option<Vec<String>>,
    /// Whether the first record is skipped, whatever it contains.
    ///
    /// It does not apply to a resource with named [columns](Self::columns),
    /// whose first record is read as the header.
    pub ignore_headers: bool,
    /// The stream compression.
    pub compression: Compression,
    /// The condition over the delivered columns, or `None` to admit all rows.
    ///
    /// A row that fails it is excluded and never interned.
    pub condition: Option<RowCondition>,
    /// The largest number of well-formed rows to read, or `None` for no limit.
    ///
    /// The rows are counted before the condition is evaluated,
    /// so the condition never makes a read go further into the resource.
    pub limit: Option<u64>,
}

impl DsvReaderOptions {
    /// Creates options for a delimiter and column formats.
    ///
    /// The options have quoting on, columns that go by position, no header, detected
    /// compression, no column that is only checked, no condition and no limit.
    pub fn new(delimiter: u8, formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self {
            delimiter,
            formats: formats.into_iter().map(Into::into).collect(),
            checked_only: Vec::new(),
            quoting: true,
            columns: None,
            ignore_headers: false,
            compression: Compression::Auto,
            condition: None,
            limit: None,
        }
    }

    /// Creates options for comma-separated columns with the given formats.
    pub fn csv(formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self::new(b',', formats)
    }

    /// Creates options for tab-separated columns with the given formats.
    pub fn tsv(formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self::new(b'\t', formats)
    }

    /// Creates options for `arity` comma-separated columns in the [Term](ValueFormat::Term)
    /// format, for tests.
    #[cfg(test)]
    pub(crate) fn csv_terms(arity: usize) -> Self {
        Self::csv(vec![ValueFormat::Term; arity])
    }

    /// Returns the arity: the number of columns that are neither skipped nor only checked.
    pub fn arity(&self) -> usize {
        (0..self.formats.len())
            .filter(|&column| self.delivers(column))
            .count()
    }

    /// Returns whether a column is delivered: neither skipped nor only checked.
    pub(crate) fn delivers(&self, column: usize) -> bool {
        self.formats[column].value != ValueFormat::Skip && !self.checks_only(column)
    }

    /// Returns whether a column's cells are only checked, not delivered.
    ///
    /// A skipped column's cells are not read, so they are never only checked.
    pub(crate) fn checks_only(&self, column: usize) -> bool {
        self.formats[column].value != ValueFormat::Skip
            && self.checked_only.get(column).copied().unwrap_or(false)
    }

    /// Returns the selection of the options: every column, and the condition over the delivered
    /// columns.
    ///
    /// The parse itself leaves out the [Skip](ValueFormat::Skip) columns and the
    /// [checked_only](Self::checked_only) ones, so the selection keeps every column it sees.
    pub(crate) fn selection(&self) -> RowSelection {
        RowSelection {
            columns: None,
            condition: self.condition.clone(),
        }
    }

    /// Returns the number of cells in the first record of a file, or `None` for an empty file.
    ///
    /// The file is read with the given delimiter, quoting and compression.
    /// If `ignore_headers` is true, the first record is skipped and the next one is counted.
    /// This gives the width of a resource whose column formats are not given.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or its first record cannot be read.
    pub fn sniff_width(
        path: impl AsRef<Path>,
        delimiter: u8,
        quoting: bool,
        ignore_headers: bool,
        compression: Compression,
    ) -> Result<Option<usize>, Diagnostic> {
        let path = path.as_ref();

        Self::sniff_width_from(
            compression.open_file(path)?,
            delimiter,
            quoting,
            ignore_headers,
        )
        .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }

    /// Returns the number of cells in the first record of a stream,
    /// as [sniff_width](Self::sniff_width) does for a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the first record cannot be read.
    pub fn sniff_width_from(
        source: impl Read,
        delimiter: u8,
        quoting: bool,
        ignore_headers: bool,
    ) -> Result<Option<usize>, Diagnostic> {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .quoting(quoting)
            .has_headers(ignore_headers)
            .flexible(true)
            .from_reader(source);
        match reader.records().next() {
            None => Ok(None),
            Some(Ok(record)) => Ok(Some(record.len())),
            Some(Err(error)) => Err(IoError::input_unreadable(&error)),
        }
    }
}

#[cfg(test)]
mod tests;
