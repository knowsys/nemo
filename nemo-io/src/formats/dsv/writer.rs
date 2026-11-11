//! Writing delimiter-separated values.

use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{
    compression::Compression,
    error::IoError,
    value_format::ValueFormat,
    write::{Writer, Written},
};

use super::DsvOptions;

/// A writer of delimiter-separated values.
///
/// Each value is written as its column's format [writes](ValueFormat::write) it.
/// The resulting cell is quoted only if it contains the delimiter, a quote or a line break,
/// and the quotes inside it are doubled.
/// No header row is written.
/// A [Skip](ValueFormat::Skip) column is left out of every record,
/// and a row with a value that does not fit its column's format is dropped and counted.
#[derive(Debug, Clone)]
pub struct DsvWriter {
    /// The options of the written resources.
    options: DsvOptions,
}

impl DsvWriter {
    /// Creates a writer with the given options.
    pub fn new(options: DsvOptions) -> Self {
        Self { options }
    }
}

impl Writer for DsvWriter {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    fn write<'a>(
        &self,
        target: impl std::io::Write,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(self.options.delimiter)
            .flexible(true)
            .from_writer(target);
        let mut written = Written::default();
        let mut record = Vec::with_capacity(self.options.formats.len());
        'rows: for row in rows {
            record.clear();
            for (value, format) in row.iter().zip(&self.options.formats) {
                if matches!(format, ValueFormat::Skip) {
                    continue;
                }
                match format.write(value.as_ref()) {
                    Some(cell) => record.push(cell),
                    None => {
                        written.dropped += 1;
                        continue 'rows;
                    }
                }
            }
            writer
                .write_record(&record)
                .map_err(IoError::output_unwritable)?;
            written.rows += 1;
        }
        writer.flush().map_err(IoError::output_unwritable)?;
        Ok(written)
    }
}
