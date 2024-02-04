//! The writer for DSV files.

use std::io::Write;

use csv::{Writer, WriterBuilder};
use nemo_physical::datavalues::AnyDataValue;

use crate::{error::Error, io::formats::types::TableWriter};

use super::dsv_value_format::DataValueSerializerFunction;
use super::dsv_value_format::DsvValueFormat;

/// A writer object for writing [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the writer will use double quotes for string escaping.
///
/// Writing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// as defined by [DsvValueFormat].
pub(super) struct DsvWriter {
    writer: Writer<Box<dyn Write>>,
    value_formats: Vec<DsvValueFormat>,
}

impl DsvWriter {
    pub(super) fn new(
        delimiter: u8,
        writer: Box<dyn Write>,
        value_formats: Vec<DsvValueFormat>,
    ) -> Self {
        DsvWriter {
            writer: WriterBuilder::new()
                .delimiter(delimiter)
                .double_quote(true)
                .from_writer(writer),
            value_formats: value_formats,
        }
    }

    fn do_export<'a>(
        mut self,
        table: Box<dyn Iterator<Item = Vec<AnyDataValue>> + 'a>,
    ) -> Result<(), Error> {
        let serializers: Vec<DataValueSerializerFunction> = self
            .value_formats
            .iter()
            .map(|vf| vf.data_value_serializer_function())
            .collect();
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == DsvValueFormat::SKIP)
            .collect();

        for record in table {
            let mut string_record = Vec::with_capacity(record.len());
            let mut complete = true;
            for (i, dv) in record.iter().enumerate() {
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
            }
        }

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
