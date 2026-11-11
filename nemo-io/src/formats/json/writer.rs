//! Writing JSON records.

use std::io::Write;

use nemo_physical::{diagnostics::Diagnostic, value::ValueRef};

use crate::{
    compression::Compression,
    error::IoError,
    value_format::{ColumnFormat, ValueFormat},
    write::{RowSink, Writer, Written},
};

use super::value;

/// The options of a JSON resource for writing.
#[derive(Debug, Clone)]
pub struct JsonWriterOptions {
    /// Whether the resource has one record per line (JSON Lines),
    /// rather than being one document that holds an array of records.
    pub lines: bool,
    /// The format of each column of the written rows.
    ///
    /// A [Skip](ValueFormat::Skip) column is left out of every record.
    pub formats: Vec<ColumnFormat>,
    /// The key of each column, in the order of [formats](Self::formats),
    /// or `None` to write each record as an array.
    pub columns: Option<Vec<String>>,
    /// The stream compression.
    pub compression: Compression,
}

impl JsonWriterOptions {
    /// Creates options for a document that is an array of records with the given column
    /// formats, each record an array, with detected compression.
    pub fn new(formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self {
            lines: false,
            formats: formats.into_iter().map(Into::into).collect(),
            columns: None,
            compression: Compression::Auto,
        }
    }
}

/// A writer of JSON records: each row is one record.
///
/// The records are the elements of one array, one per line,
/// or the lines of a resource without an enclosing array.
/// A record is an array with one element per column,
/// or an object if the options name the [keys](JsonWriterOptions::columns) of the columns.
/// Each value is written by its column's format:
/// under [Term](ValueFormat::Term) the JSON value says what it is,
/// and under any other format it is the bare form of the value.
/// A [JsonReader](super::JsonReader) with the same options reads the rows back.
///
/// A row with a value that has no form in its column's format is dropped and counted.
#[derive(Debug, Clone)]
pub struct JsonWriter {
    /// The options of the written resources.
    options: JsonWriterOptions,
}

impl JsonWriter {
    /// Creates a writer with the given options.
    pub fn new(options: JsonWriterOptions) -> Self {
        Self { options }
    }
}

impl Writer for JsonWriter {
    type Sink<W: Write> = JsonSink<W>;

    fn compression(&self) -> Compression {
        self.options.compression
    }

    fn start<W: Write>(&self, mut target: W) -> Result<JsonSink<W>, Diagnostic> {
        if !self.options.lines {
            target.write_all(b"[").map_err(IoError::output_unwritable)?;
        }

        Ok(JsonSink {
            options: self.options.clone(),
            target,
            record: String::new(),
            written: Written::default(),
        })
    }
}

/// The sink of the records of one JSON resource.
#[derive(Debug)]
pub struct JsonSink<W: Write> {
    /// The options of the resource.
    options: JsonWriterOptions,
    /// The stream of the resource.
    target: W,
    /// The text of the current record.
    record: String,
    /// The counts of the rows written and dropped so far.
    written: Written,
}

impl<W: Write> RowSink for JsonSink<W> {
    fn write_row(&mut self, row: &[ValueRef<'_>]) -> Result<(), Diagnostic> {
        let record = &mut self.record;
        let keys = self.options.columns.as_deref();

        // In a document, each record but the first follows a comma.
        record.clear();
        if !self.options.lines {
            record.push_str(if self.written.rows == 0 { "\n" } else { ",\n" });
        }

        record.push(if keys.is_some() { '{' } else { '[' });
        let mut first = true;
        for (column, (&value, format)) in row.iter().zip(&self.options.formats).enumerate() {
            if format.value == ValueFormat::Skip {
                continue;
            }
            if !first {
                record.push_str(", ");
            }

            first = false;
            if let Some(keys) = keys {
                value::write_string(&keys[column], record);
                record.push_str(": ");
            }
            if !value::write(value, *format, record) {
                self.written.dropped += 1;
                return Ok(());
            }
        }

        record.push(if keys.is_some() { '}' } else { ']' });
        if self.options.lines {
            record.push('\n');
        }

        self.target
            .write_all(record.as_bytes())
            .map_err(IoError::output_unwritable)?;
        self.written.rows += 1;

        Ok(())
    }

    fn finish(mut self) -> Result<Written, Diagnostic> {
        let end: &[u8] = if self.options.lines { b"" } else { b"\n]\n" };
        self.target
            .write_all(end)
            .and_then(|()| self.target.flush())
            .map_err(IoError::output_unwritable)?;

        Ok(self.written)
    }
}
