//! The reader for DSV files.

use std::io::BufRead;
use std::mem::size_of;

use csv::{Reader, ReaderBuilder};
use nemo_physical::error::ReadingError;
use nemo_physical::management::bytesized::ByteSized;

use nemo_physical::datasources::{table_providers::TableProvider, tuple_writer::TupleWriter};

use crate::io::formats::PROGRESS_NOTIFY_INCREMENT;

use super::value_format::{DataValueParserFunction, DsvValueFormat, DsvValueFormats};

/// A reader object for reading [DSV](https://en.wikipedia.org/wiki/Delimiter-separated_values) (delimiter separated values) files.
///
/// By default the reader will assume the following for the input file:
/// - no headers are given,
/// - double quotes are allowed for string escaping
///
/// Parsing of individual values can be done in several ways (DSV does not specify a data model at this level),
/// as defined by [DsvValueFormat].
pub(super) struct DsvReader<T> {
    /// Buffer from which content is read
    read: T,

    /// Delimiter used to separate values in the file
    delimiter: u8,
    /// Escape character used
    escape: Option<u8>,
    /// List of [DsvValueFormat] indicating for each column
    /// the type of value parser that should be use
    value_formats: DsvValueFormats,
    /// Maximum number of entries that should be read.
    limit: Option<u64>,
    /// Whether to ignore headers (i.e., the first record)
    ignore_headers: bool,
}

impl<T: BufRead> DsvReader<T> {
    /// Instantiate a [DsvReader] for a given delimiter
    pub(super) fn new(
        read: T,
        delimiter: u8,
        value_formats: DsvValueFormats,
        escape: Option<u8>,
        limit: Option<u64>,
        ignore_headers: bool,
    ) -> Self {
        Self {
            read,
            delimiter,
            value_formats,
            escape,
            limit,
            ignore_headers,
        }
    }

    /// Create a low-level reader for parsing the DSV format.
    fn reader(self) -> Reader<T> {
        ReaderBuilder::new()
            .delimiter(self.delimiter)
            .escape(self.escape)
            .has_headers(self.ignore_headers)
            .double_quote(true)
            .from_reader(self.read)
    }

    /// Actually reads the data from the file, using the given parsers to convert strings to [AnyDataValue]s.
    /// If a field cannot be read or parsed, the line will be ignored
    fn read(self, tuple_writer: &mut TupleWriter) -> Result<(), ReadingError> {
        log::info!("Starting data import");

        let parsers: Vec<DataValueParserFunction> = self
            .value_formats
            .iter()
            .map(|vf| vf.data_value_parser_function())
            .collect();
        let skip: Vec<bool> = self
            .value_formats
            .iter()
            .map(|vf| *vf == DsvValueFormat::Skip)
            .collect();
        let expected_file_arity = parsers.len();
        assert_eq!(
            tuple_writer.column_number(),
            skip.iter().filter(|b| !*b).count()
        );

        let stop_limit = self.limit.unwrap_or(0);

        let mut dsv_reader = self.reader();

        let mut line_count: u64 = 0;
        let mut drop_count: u64 = 0;
        for row in dsv_reader.records().flatten() {
            for (idx, value) in row.iter().enumerate() {
                if idx >= expected_file_arity || skip[idx] {
                    continue;
                }
                if let Ok(dv) = parsers[idx](value.to_string()) {
                    tuple_writer.add_tuple_value(dv);
                } else {
                    drop_count += 1;
                    tuple_writer.drop_current_tuple();
                    break;
                }
            }

            line_count += 1;
            if line_count.is_multiple_of(PROGRESS_NOTIFY_INCREMENT) {
                log::info!("... processed {line_count} lines");
            }
            if line_count == stop_limit {
                break;
            }
        }
        log::info!("Finished import: processed {line_count} lines (dropped {drop_count})");

        Ok(())
    }
}

impl<T: BufRead> TableProvider for DsvReader<T> {
    fn provide_table_data(
        self: Box<Self>,
        tuple_writer: &mut TupleWriter,
    ) -> Result<(), ReadingError> {
        self.read(tuple_writer)
    }

    fn arity(&self) -> usize {
        self.value_formats.arity()
    }
}

impl<T> std::fmt::Debug for DsvReader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DsvReader")
            .field("read", &"<unspecified std::io::Read>")
            .field("delimiter", &self.delimiter)
            .field("escape", &self.escape)
            .field("value formats", &self.value_formats)
            .finish()
    }
}

impl<T> ByteSized for DsvReader<T> {
    fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    #[cfg(not(miri))]
    use test_log::test;

    use crate::io::formats::dsv::{
        reader::DsvReader,
        value_format::{DsvValueFormat, DsvValueFormats},
    };
    use nemo_physical::{datasources::tuple_writer::TupleWriter, management::database::Dict};

    #[test]
    fn dsv_reading_basic() {
        let data = r#"city;country;pop;fraction
        Boston;United States;4628910;3.14
        Line;too short;123
        Also-too-short
        This;line is too;123;3.14;long
        This;Line is ok;12345;3.15
        This;line;fails for the integer column;3.14
        This;line;123;fails for the double column
        "#;

        for ignore_headers in [false, true] {
            let reader = DsvReader::new(
                Box::new(data.as_bytes()),
                b';',
                DsvValueFormats::new(vec![
                    DsvValueFormat::Anything,
                    DsvValueFormat::String,
                    DsvValueFormat::Integer,
                    DsvValueFormat::Double,
                ]),
                None,
                None,
                ignore_headers,
            );
            let dict = RefCell::new(Dict::default());
            let mut tuple_writer = TupleWriter::new(&dict, 4);
            let result = reader.read(&mut tuple_writer);
            assert!(result.is_ok());
            assert_eq!(tuple_writer.size(), 2);
        }
    }
}
