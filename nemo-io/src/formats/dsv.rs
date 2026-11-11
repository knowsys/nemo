//! Delimiter-separated values (DSV), the tabular formats such as CSV and TSV.
//!
//! The *width* of a resource is its number of written columns,
//! and the *arity* is the number of columns that a reader delivers.

mod parsed;
mod run_pool;

pub(crate) use parsed::Parsed;
pub(crate) use run_pool::RunPool;

use std::{io::Read, path::Path};

use csv::ReaderBuilder;

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
};

use crate::{
    AboutResource,
    error::IoError,
    format::ValueFormat,
    load::{Loaded, Reader},
    row_filter::RowFilter,
    selection::RowSelection,
    stream::compression::{Compression, open},
    written::{Writer, Written},
};

/// The options of a delimiter-separated resource, such as its delimiter and one [ValueFormat]
/// per column.
///
/// A [Skip](ValueFormat::Skip) column counts toward the width but not toward the arity.
#[derive(Debug, Clone)]
pub struct DsvOptions {
    /// The delimiter between the cells of a record.
    pub delimiter: u8,
    /// The format of each written column.
    pub formats: Vec<ValueFormat>,
    /// For each written column, whether its cells are only checked.
    ///
    /// A column that is only checked is not delivered,
    /// but a cell that its format rejects still makes the row malformed.
    /// Checking a column rather than skipping it keeps the set of well-formed rows the same,
    /// whichever columns are delivered.
    /// A column past the end of the list is not only checked, so the list may be empty.
    pub checked_only: Vec<bool>,
    /// Whether `"` quotes cells, rather than being an ordinary character.
    pub quoting: bool,
    /// Whether the first record is skipped, whatever it contains.
    pub ignore_headers: bool,
    /// The stream compression.
    pub compression: Compression,
    /// The condition over the delivered columns, or `None` to admit all rows.
    ///
    /// A row that fails it is excluded and never interned.
    pub filter: Option<RowFilter>,
    /// The largest number of well-formed rows to read, or `None` for no limit.
    ///
    /// The rows are counted before the condition is evaluated,
    /// so the condition never makes a read go further into the resource.
    pub limit: Option<u64>,
}

impl DsvOptions {
    /// Creates options for a delimiter and column formats.
    ///
    /// The options have quoting on, no header, detected compression, no column that is only
    /// checked, no condition and no limit.
    pub fn new(delimiter: u8, formats: Vec<ValueFormat>) -> Self {
        Self {
            delimiter,
            formats,
            checked_only: Vec::new(),
            quoting: true,
            ignore_headers: false,
            compression: Compression::Auto,
            filter: None,
            limit: None,
        }
    }

    /// Creates options for comma-separated columns with the given formats.
    pub fn csv(formats: Vec<ValueFormat>) -> Self {
        Self::new(b',', formats)
    }

    /// Creates options for tab-separated columns with the given formats.
    pub fn tsv(formats: Vec<ValueFormat>) -> Self {
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
        !matches!(self.formats[column], ValueFormat::Skip) && !self.checks_only(column)
    }

    /// Returns whether a column's cells are only checked, not delivered.
    ///
    /// A skipped column's cells are not read, so they are never only checked.
    pub(crate) fn checks_only(&self, column: usize) -> bool {
        !matches!(self.formats[column], ValueFormat::Skip)
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
            filter: self.filter.clone(),
        }
    }

    /// Parses a record's cells by their formats, appending the delivered values to `values`.
    ///
    /// Returns `false`, and leaves `values` as it was, for a record of the wrong width
    /// or with a cell that its format rejects.
    fn parse_row(&self, cells: &csv::StringRecord, values: &mut Vec<DataValue>) -> bool {
        let before = values.len();
        let complete = cells.len() == self.formats.len()
            && cells
                .iter()
                .zip(&self.formats)
                .enumerate()
                .all(|(column, (cell, format))| match format {
                    ValueFormat::Skip => true,
                    _ if self.checks_only(column) => format.parse_cell(cell).is_some(),
                    _ => format.parse(cell).is_some_and(|value| {
                        values.push(value);
                        true
                    }),
                });
        if !complete {
            values.truncate(before);
        }
        complete
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
    pub fn sniff_arity(
        path: impl AsRef<Path>,
        delimiter: u8,
        quoting: bool,
        ignore_headers: bool,
        compression: Compression,
    ) -> Result<Option<usize>, Diagnostic> {
        let path = path.as_ref();
        Self::sniff_arity_from(open(path, compression)?, delimiter, quoting, ignore_headers)
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }

    /// Returns the number of cells in the first record of a stream,
    /// as [sniff_arity](Self::sniff_arity) does for a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the first record cannot be read.
    pub fn sniff_arity_from(
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
            Some(Err(error)) => Err(IoError::stream_unreadable(&error)),
        }
    }
}

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
                .map_err(IoError::stream_unwritable)?;
            written.rows += 1;
        }
        writer.flush().map_err(IoError::stream_unwritable)?;
        Ok(written)
    }
}

/// A reader of delimiter-separated values, which decodes rows into row buffers.
///
/// The reader streams: it parses records by their columns' formats into runs of bounded size,
/// and interns each run once it is full.
/// A malformed row is counted and skipped.
/// A row is malformed if it has the wrong width, if a cell's format rejects the cell,
/// or if the dictionary cannot hold one of its values.
/// The delivered rows are in the order of the file, unsorted.
///
/// # Example
///
/// ```
/// use nemo_io::{
///     dsv::{DsvOptions, DsvReader},
///     format::ValueFormat,
///     load::Reader,
/// };
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext};
///
/// let reader = DsvReader::new(DsvOptions::csv(vec![ValueFormat::Term, ValueFormat::Integer]));
/// let mut dictionary = Dictionary::new();
/// let text = "alice,1\nbob,two\ncarol,3\n";
/// let loaded = reader
///     .read(text.as_bytes(), &mut dictionary, EvaluationContext::now())
///     .expect("the text reads");
///
/// // The row whose second cell is not an integer is malformed, so it is counted, not delivered.
/// assert_eq!(loaded.rows.len(), 2);
/// assert_eq!((loaded.malformed, loaded.first_malformed), (1, Some(2)));
/// ```
#[derive(Debug, Clone)]
pub struct DsvReader {
    /// The options of the read resources.
    options: DsvOptions,
}

impl DsvReader {
    /// Creates a reader with the given options.
    pub fn new(options: DsvOptions) -> Self {
        Self { options }
    }

    /// Parses all rows of a resource into [runs](Parsed) taken from `runs`,
    /// without using a dictionary.
    ///
    /// Each run is passed to `emit` once it is full.
    /// The last run is passed when the resource ends or the [limit](DsvOptions::limit) is
    /// reached, with the number of lines that the parse consumed.
    /// If `emit` returns `false`, the parse ends.
    pub(crate) fn parse(
        &self,
        source: impl Read,
        runs: &RunPool,
        mut emit: impl FnMut(Parsed) -> bool,
    ) -> Result<(), Diagnostic> {
        let selection = self.options.selection();
        let limit = self.options.limit.unwrap_or(u64::MAX);
        let arity = self.options.arity();
        let mut run = runs.take(arity);
        // The well-formed rows so far, across the runs, which the limit counts.
        let mut well_formed = 0;
        let mut wanted = true;
        let consumed = self.records(source, |record, line| {
            if well_formed >= limit {
                return false;
            }
            let before = run.well_formed();
            run.push_record(&self.options, &selection, record, line);
            well_formed += run.well_formed() - before;
            if run.is_full() {
                wanted = emit(std::mem::replace(&mut run, runs.take(arity)));
            }
            wanted
        })?;
        if wanted {
            run.consumed_lines = consumed;
            emit(run);
        }
        Ok(())
    }

    /// Passes every record of a resource to `visit`, with its line number,
    /// until `visit` returns `false`.
    ///
    /// Returns the number of lines consumed.
    /// Fails for a record that cannot be read, such as one with broken quoting or invalid UTF-8.
    fn records(
        &self,
        source: impl Read,
        mut visit: impl FnMut(&csv::StringRecord, u64) -> bool,
    ) -> Result<u64, Diagnostic> {
        let mut reader = ReaderBuilder::new()
            .delimiter(self.options.delimiter)
            .has_headers(self.options.ignore_headers)
            .quoting(self.options.quoting)
            .flexible(true)
            .from_reader(source);

        // One record is reused, so that a row needs no allocation.
        let mut record = csv::StringRecord::new();
        while reader
            .read_record(&mut record)
            .map_err(IoError::stream_unreadable)?
        {
            let line = record.position().map_or(0, |position| position.line());
            if !visit(&record, line) {
                break;
            }
        }
        // The position after the last record is the first unread line,
        // so one line fewer was consumed.
        Ok(reader.position().line().saturating_sub(1))
    }
}

impl Reader for DsvReader {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Reads all rows of a resource, interning them as they are parsed.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails, or if a record cannot be read,
    /// such as for broken quoting or text that is not valid UTF-8.
    fn read(
        &self,
        source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let runs = RunPool::new(context);
        let mut loaded = Loaded::new(self.options.arity(), context);
        self.parse(source, &runs, |run| {
            run.encode_into(&mut loaded, dictionary, 0);
            runs.give(run);
            true
        })?;
        tracing::debug!(
            rows = loaded.rows.len(),
            malformed = loaded.malformed,
            excluded = loaded.excluded,
            errors = ?loaded.errors,
            "read a delimited source"
        );
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    use crate::{
        format::ValueFormat,
        testing::{TempDirectory, greater_than},
    };

    /// CSV rows in term syntax are read as their values.
    #[test]
    fn csv_rows_intern_and_round_trip() {
        let mut dictionary = Dictionary::new();
        let reader = DsvReader::new(DsvOptions::csv_terms(3));
        let input = "alice,42,-2.5\nbob,7,1e3\n";
        let loaded = reader
            .read(
                Cursor::new(input),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");

        assert_eq!(loaded.malformed, 0);
        assert_eq!(
            loaded.decoded(&dictionary),
            vec![
                vec![
                    DataValue::iri("alice"),
                    DataValue::integer(42),
                    DataValue::decimal(-25, -1).expect("representable"),
                ],
                vec![
                    DataValue::iri("bob"),
                    DataValue::integer(7),
                    DataValue::double(1000.0),
                ],
            ]
        );
    }

    /// A row with a cell that its format rejects, or of the wrong width, is counted as malformed.
    #[test]
    fn malformed_rows_are_counted_never_silent() {
        let mut dictionary = Dictionary::new();
        let reader = DsvReader::new(DsvOptions::csv(vec![
            ValueFormat::String,
            ValueFormat::Integer,
        ]));
        // Line 2 has a bad integer, and line 3 has the wrong width.
        let input = "a,1\nb,oops\nc\nd,4\n";
        let loaded = reader
            .read(
                Cursor::new(input),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");

        assert_eq!(loaded.rows.len(), 2);
        assert_eq!(loaded.malformed, 2);
        assert_eq!(loaded.first_malformed, Some(2));
    }

    /// A tab-separated resource is read with its own delimiter.
    #[test]
    fn delimiters_are_configurable() {
        let mut dictionary = Dictionary::new();
        let reader = DsvReader::new(DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]));
        let loaded = reader
            .read(
                Cursor::new("a\t1\n"),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");
        assert_eq!(
            loaded.decoded(&dictionary),
            vec![vec![DataValue::iri("a"), DataValue::integer(1)]]
        );
    }

    /// An empty resource loads no rows, and no malformed ones.
    #[test]
    fn empty_sources_load_empty() {
        let mut dictionary = Dictionary::new();
        let reader = DsvReader::new(DsvOptions::csv_terms(2));
        let loaded = reader
            .read(Cursor::new(""), &mut dictionary, EvaluationContext::now())
            .expect("the source reads");
        assert!(loaded.rows.is_empty());
        assert_eq!(loaded.malformed, 0);
    }

    /// A writer writes cells in term syntax, quoting a cell as CSV requires.
    #[test]
    fn writers_serialize_cells_in_the_term_format() {
        let rows: Vec<Vec<DataValue>> = vec![
            vec![DataValue::integer(42), DataValue::string("plain")],
            vec![
                DataValue::iri("Foo@en"),
                DataValue::language_string("Foo", "en").expect("the tag is valid"),
            ],
        ];
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvOptions::csv_terms(2))
            .write(&mut target, rows.iter().map(Vec::as_slice))
            .expect("the rows write");
        assert_eq!(written.rows, 2);
        assert_eq!(
            String::from_utf8(target).expect("the output is text"),
            "42,\"\"\"plain\"\"\"\nFoo@en,\"\"\"Foo\"\"@en\"\n",
        );
    }

    /// A skipped column is left out of every written record.
    #[test]
    fn skipped_columns_are_left_out_of_the_record() {
        let rows: Vec<Vec<DataValue>> = vec![
            vec![DataValue::integer(1), DataValue::string("a")],
            vec![DataValue::integer(2), DataValue::string("b")],
        ];
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvOptions::new(
            b',',
            vec![ValueFormat::Skip, ValueFormat::Term],
        ))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
        assert_eq!((written.rows, written.dropped), (2, 0));
        assert_eq!(
            String::from_utf8(target).expect("text"),
            "\"\"\"a\"\"\"\n\"\"\"b\"\"\"\n"
        );
    }

    /// A row with a value that does not fit its column's format is dropped from a write, and
    /// counted.
    #[test]
    fn declared_formats_drop_unfit_rows() {
        let rows: Vec<Vec<DataValue>> =
            vec![vec![DataValue::integer(1)], vec![DataValue::string("x")]];
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvOptions::new(b',', vec![ValueFormat::Integer]))
            .write(&mut target, rows.iter().map(Vec::as_slice))
            .expect("the rows write");
        assert_eq!((written.rows, written.dropped), (1, 1));
        assert_eq!(String::from_utf8(target).expect("text"), "1\n");
    }

    /// The integer and decimal formats write and read numbers beyond 64 bits,
    /// and the integer format rejects a fraction.
    #[test]
    fn numeric_formats_carry_wide_numbers() {
        let big = |lexical: &str| DataValue::decimal_from_lexical(lexical).expect("well-formed");
        let rows: Vec<Vec<DataValue>> = vec![
            vec![big("123456789012345678901"), big("123456789012345678901.5")],
            // A fractional value is not an integer.
            vec![big("123456789012345678901.5"), big("123456789012345678901")],
        ];
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvOptions::new(
            b',',
            vec![ValueFormat::Integer, ValueFormat::Decimal],
        ))
        .write(&mut target, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
        assert_eq!((written.rows, written.dropped), (1, 1));
        assert_eq!(
            String::from_utf8(target).expect("text"),
            "123456789012345678901,123456789012345678901.5\n"
        );

        assert_eq!(
            ValueFormat::Integer.parse("123456789012345678901"),
            Some(big("123456789012345678901"))
        );
        assert_eq!(ValueFormat::Integer.parse("123456789012345678901.5"), None);
    }

    /// A file written through gzip, as its name asks, is read back as the rows written.
    #[test]
    fn compressed_writes_read_back() {
        let directory = TempDirectory::new();
        let path = directory.path().join("rows.csv.gz");
        let rows: Vec<Vec<DataValue>> = vec![vec![
            DataValue::integer(7),
            DataValue::string("hello"),
            DataValue::iri("urn:x"),
        ]];
        DsvWriter::new(DsvOptions::new(b',', vec![ValueFormat::Term; 3]))
            .write_path(&path, rows.iter().map(Vec::as_slice))
            .expect("the rows write");

        let mut dictionary = Dictionary::new();
        let loaded = DsvReader::new(DsvOptions::new(b',', vec![ValueFormat::Term; 3]))
            .read_path(&path, &mut dictionary, EvaluationContext::now())
            .expect("the file reads back");
        assert_eq!(loaded.decoded(&dictionary), rows);
    }

    /// Writes rows in term syntax, and returns the rows read back from the result.
    fn round_trip(rows: &[Vec<DataValue>]) -> Vec<Vec<DataValue>> {
        let width = rows[0].len();
        let mut target = Vec::new();
        let written = DsvWriter::new(DsvOptions::csv_terms(width))
            .write(&mut target, rows.iter().map(Vec::as_slice))
            .expect("the rows write");
        assert_eq!(
            usize::try_from(written.rows).expect("a row count fits"),
            rows.len()
        );

        let mut dictionary = Dictionary::new();
        let loaded = DsvReader::new(DsvOptions::csv(vec![ValueFormat::Term; width]))
            .read(target.as_slice(), &mut dictionary, EvaluationContext::now())
            .expect("the output reads back");
        assert_eq!(loaded.malformed, 0, "no row is malformed");
        loaded.decoded(&dictionary)
    }

    /// Strings with quotes, backslashes and line breaks are read back as written.
    #[test]
    fn escaped_strings_read_back_as_written() {
        // The escapes are written inside the quotes, and term syntax resolves them
        // in plain strings, language strings and typed literals alike.
        let rows = vec![vec![
            DataValue::string(r#"say "hi""#),
            DataValue::string(r"a\b"),
            DataValue::string("x\ny\r"),
            DataValue::string(r#"a"b"#),
            DataValue::string("plain"),
            DataValue::language_string(r#"he said "x""#, "en").expect("valid tag"),
            DataValue::other("http://example.org/t", "q\"uote\\").expect("well-formed"),
        ]];
        assert_eq!(round_trip(&rows), rows);
    }

    /// The limit counts well-formed rows before the condition.
    #[test]
    fn the_limit_counts_rows_before_the_condition() {
        // The first three well-formed rows are 1, 2 and 3, whichever of them
        // the condition keeps. A condition reduces the rows of a read with a
        // limit, and never makes it read further into the resource.
        let mut options = DsvOptions::csv(vec![ValueFormat::Integer]);
        options.filter = Some(greater_than(0, 2));
        options.limit = Some(3);
        let mut dictionary = Dictionary::new();
        let loaded = DsvReader::new(options)
            .read(
                "1\n2\n3\n4\n5\n".as_bytes(),
                &mut dictionary,
                EvaluationContext::now(),
            )
            .expect("the source reads");
        assert_eq!((loaded.rows.len(), loaded.excluded), (1, 2));
        assert_eq!(
            dictionary.decode(loaded.rows.row(0)[0]).expect("decodes"),
            DataValue::integer(3).as_ref()
        );
    }

    /// A condition's `NOW()` is the instant of the context the read is given.
    #[test]
    fn a_condition_reads_the_instant_of_the_context_given() {
        use nemo_physical::{
            function::{OpCode, StackProgram},
            value::{DateTime, TimezoneOffset},
        };

        // The condition keeps a row equal to the year of `NOW()`, which is 1970 only in a
        // context at the epoch.
        let mut options = DsvOptions::csv(vec![ValueFormat::Integer]);
        options.filter = Some(RowFilter::new(
            vec![0],
            StackProgram::new(
                vec![OpCode::Input(0), OpCode::Now, OpCode::Year, OpCode::Equal],
                Vec::new(),
                1,
            ),
        ));
        let reader = DsvReader::new(options);
        let kept = |context| {
            reader
                .read("1970\n".as_bytes(), &mut Dictionary::new(), context)
                .expect("the source reads")
                .rows
                .len()
        };
        let epoch = DateTime::from_unix_millis(0, Some(TimezoneOffset::UTC));
        assert_eq!(kept(EvaluationContext::at(epoch)), 1);
        assert_eq!(kept(EvaluationContext::now()), 0);
    }

    /// The sniffed width is the number of cells in the first record as it is read,
    /// with the given quoting and header.
    #[test]
    fn the_width_is_sniffed_as_the_records_are_read() {
        let sniff = |text: &str, quoting, ignore_headers| {
            DsvOptions::sniff_arity_from(text.as_bytes(), b',', quoting, ignore_headers)
                .expect("the record reads")
        };
        assert_eq!(sniff("\"a,b\",c\n", true, false), Some(2));
        assert_eq!(sniff("\"a,b\",c\n", false, false), Some(3));
        assert_eq!(sniff("name\nalice,30\n", false, true), Some(2));
        assert_eq!(sniff("name\n", true, true), None);
    }
}
