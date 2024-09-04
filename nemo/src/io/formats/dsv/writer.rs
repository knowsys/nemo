//! The writer for DSV files.

use std::io::Write;

use csv::{Writer, WriterBuilder};
use nemo_physical::datavalues::AnyDataValue;

use crate::{
    error::Error,
    io::formats::{
        dsv::value_format::DataValueSerializerFunction, TableWriter, PROGRESS_NOTIFY_INCREMENT,
    },
};

use super::value_format::{DsvValueFormat, DsvValueFormats};

/// A writer object for writing [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the writer will use double quotes for string escaping.
///
/// Writing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// as defined by [DsvValueFormat].
pub(super) struct DsvWriter {
    /// Buffer to write into
    writer: Writer<Box<dyn Write>>,

    /// List of [DsvValueFormat] indicating for each column
    /// the type of value parser that should be use
    value_formats: DsvValueFormats,
    /// Maximum number of entries that should be written.
    limit: Option<u64>,
}

impl DsvWriter {
    pub(super) fn new(
        delimiter: u8,
        writer: Box<dyn Write>,
        value_formats: DsvValueFormats,
        limit: Option<u64>,
    ) -> Self {
        DsvWriter {
            writer: WriterBuilder::new()
                .delimiter(delimiter)
                .double_quote(true)
                .from_writer(writer),
            value_formats,
            limit,
        }
    }

    fn do_export<'a>(
        mut self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        log::info!("Starting data export");

        let serializers: Vec<DataValueSerializerFunction> = self
            .value_formats
            .iter()
            .map(|vf| vf.data_value_serializer_function())
            .collect();
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == DsvValueFormat::Skip)
            .collect();

        let stop_limit = self.limit.unwrap_or(0);

        let mut line_count: u64 = 0;
        let mut drop_count: u64 = 0;
        for record in table {
            let mut string_record = Vec::with_capacity(record.len());
            let mut complete = true;
            for (i, dv) in record.iter().enumerate() {
                assert!(i < self.value_formats.len());
                if skip[i] {
                    continue;
                }
                if let Some(stringvalue) = serializers[i](dv) {
                    string_record.push(stringvalue);
                } else {
                    complete = false;
                    break;
                }
            }

            if complete {
                self.writer.write_record(string_record)?;
                line_count += 1;
                if (line_count % PROGRESS_NOTIFY_INCREMENT) == 0 {
                    log::info!("... processed {line_count} tuples");
                }
                if line_count == stop_limit {
                    break;
                }
            } else {
                drop_count += 1;
            }
        }

        self.writer.flush()?;

        log::info!("Finished export: processed {line_count} tuples (dropped {drop_count})");

        Ok(())
    }
}

impl TableWriter for DsvWriter {
    fn export_table_data<'a>(
        self: Box<Self>,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        self.do_export(table)
    }
}

impl std::fmt::Debug for DsvWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DsvWriter")
            .field("writer", &"<unspecified std::io::Write>")
            .field("value formats", &self.value_formats)
            .finish()
    }
}
