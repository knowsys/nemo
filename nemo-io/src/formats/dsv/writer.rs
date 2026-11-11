//! Writing delimiter-separated values.

use std::io::Write;

use nemo_physical::{diagnostics::Diagnostic, value::ValueRef};

use crate::{
    compression::Compression,
    error::IoError,
    value_format::{ColumnFormat, ValueFormat},
    write::{RowSink, Writer, Written},
};

/// The options of a delimiter-separated resource for writing.
#[derive(Debug, Clone)]
pub struct DsvWriterOptions {
    /// The delimiter between the cells of a record.
    pub delimiter: u8,
    /// The format of each column of the written rows.
    ///
    /// A [Skip](ValueFormat::Skip) column is left out of every record.
    pub formats: Vec<ColumnFormat>,
    /// The names of the columns, in the order of [formats](Self::formats),
    /// or `None` to write no header.
    ///
    /// The names are written as the first record, without the name of a skipped column.
    pub columns: Option<Vec<String>>,
    /// The stream compression.
    pub compression: Compression,
}

impl DsvWriterOptions {
    /// Creates options for a delimiter and column formats, with no header and detected
    /// compression.
    pub fn new(delimiter: u8, formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self {
            delimiter,
            formats: formats.into_iter().map(Into::into).collect(),
            columns: None,
            compression: Compression::Auto,
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
}

/// A writer of delimiter-separated values.
///
/// Each value is written as its column's format [writes](ColumnFormat::write_into) it.
/// The resulting cell is quoted only if it contains the delimiter, a quote or a line break,
/// and the quotes inside it are doubled.
/// If the options name the [columns](DsvWriterOptions::columns), the names are written first,
/// as a header row.
/// A [Skip](ValueFormat::Skip) column is left out of every record,
/// and a row with a value that has no form in its column's format is dropped and counted.
#[derive(Debug, Clone)]
pub struct DsvWriter {
    /// The options of the written resources.
    options: DsvWriterOptions,
}

impl DsvWriter {
    /// Creates a writer with the given options.
    pub fn new(options: DsvWriterOptions) -> Self {
        Self { options }
    }
}

impl Writer for DsvWriter {
    type Sink<W: Write> = DsvSink<W>;

    fn compression(&self) -> Compression {
        self.options.compression
    }

    fn start<W: Write>(&self, target: W) -> Result<DsvSink<W>, Diagnostic> {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(self.options.delimiter)
            .flexible(true)
            .from_writer(target);
        if let Some(names) = &self.options.columns {
            let written = names
                .iter()
                .zip(&self.options.formats)
                .filter(|(_, format)| format.value != ValueFormat::Skip)
                .map(|(name, _)| name);
            writer
                .write_record(written)
                .map_err(IoError::output_unwritable)?;
        }

        Ok(DsvSink {
            formats: self.options.formats.clone(),
            writer,
            record: csv::ByteRecord::new(),
            cell: String::new(),
            written: Written::default(),
        })
    }
}

/// The sink of the rows of one delimiter-separated resource.
///
/// The cells of a row are formatted into buffers that the sink keeps between the rows.
#[derive(Debug)]
pub struct DsvSink<W: Write> {
    /// The format of each column of the rows.
    formats: Vec<ColumnFormat>,
    /// The writer of the records.
    writer: csv::Writer<W>,
    /// The cells of the current row.
    record: csv::ByteRecord,
    /// The text of the current cell.
    cell: String,
    /// The counts of the rows written and dropped so far.
    written: Written,
}

impl<W: Write> RowSink for DsvSink<W> {
    fn write_row(&mut self, row: &[ValueRef<'_>]) -> Result<(), Diagnostic> {
        self.record.clear();
        for (&value, format) in row.iter().zip(&self.formats) {
            if format.value == ValueFormat::Skip {
                continue;
            }

            self.cell.clear();
            if !format.write_into(value, &mut self.cell) {
                self.written.dropped += 1;
                return Ok(());
            }

            self.record.push_field(self.cell.as_bytes());
        }

        self.writer
            .write_byte_record(&self.record)
            .map_err(IoError::output_unwritable)?;
        self.written.rows += 1;

        Ok(())
    }

    fn finish(mut self) -> Result<Written, Diagnostic> {
        self.writer.flush().map_err(IoError::output_unwritable)?;

        Ok(self.written)
    }
}
