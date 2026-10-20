//! Delimiter-separated values, the tabular formats.

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

/// The shape of a delimiter-separated resource: its delimiter and one [ValueFormat] per column.
///
/// A [Skip](ValueFormat::Skip) column counts toward the file's width but not the delivered arity.
#[derive(Debug, Clone)]
pub struct DsvOptions {
    /// The field delimiter.
    pub delimiter: u8,
    /// The cell format per column of the file.
    pub formats: Vec<ValueFormat>,
    /// Per column, whether its cells are only checked:
    /// a cell the column's format rejects still makes the row malformed,
    /// but the column is not delivered.
    /// The columns no rule reads are read so, which keeps the rows independent of what is read.
    /// Empty if no column is.
    pub checked_only: Vec<bool>,
    /// Whether `"` quotes fields, or is an ordinary character.
    pub quoting: bool,
    /// Whether the first record is skipped, whatever it holds.
    pub ignore_headers: bool,
    /// The stream compression.
    pub compression: Compression,
    /// A condition over the delivered (non-skipped) columns.
    ///
    /// Rows failing it are excluded, counted, and never interned.
    pub filter: Option<RowFilter>,
    /// The number of well-formed rows matching the declared types the reader takes before it
    /// stops.
    ///
    /// The rows are counted before the condition,
    /// so a condition never changes which rows a limited resource yields.
    pub limit: Option<u64>,
}

impl DsvOptions {
    /// Options for a delimiter and column formats, with quoting on, no
    /// header skipping, auto-detected compression, and every row
    /// delivered.
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

    /// Creates options for comma-separated columns,
    /// all interpreted as [Term](ValueFormat::Term), as the tests use them.
    #[cfg(test)]
    pub(crate) fn csv_terms(arity: usize) -> Self {
        Self::csv(vec![ValueFormat::Term; arity])
    }

    /// Returns the delivered arity: the number of columns neither skipped nor only checked.
    pub fn arity(&self) -> usize {
        (0..self.formats.len())
            .filter(|&column| self.delivers(column))
            .count()
    }

    /// Returns whether a column is delivered: neither skipped nor only checked.
    pub(crate) fn delivers(&self, column: usize) -> bool {
        !matches!(self.formats[column], ValueFormat::Skip) && !self.checks_only(column)
    }

    /// Returns whether a column's cells are only checked, not delivered;
    /// never for a skipped column, whose cells are not read.
    pub(crate) fn checks_only(&self, column: usize) -> bool {
        !matches!(self.formats[column], ValueFormat::Skip)
            && self.checked_only.get(column).copied().unwrap_or(false)
    }

    /// Returns the selection the options state: the declared types and the condition over the
    /// delivered columns.
    ///
    /// The [Skip](ValueFormat::Skip) formats and the [checked_only](Self::checked_only) columns
    /// decide which columns are delivered as the cells parse,
    /// so the selection keeps every column it sees.
    pub(crate) fn selection(&self) -> RowSelection {
        RowSelection {
            columns: None,
            filter: self.filter.clone(),
        }
    }

    /// Parses a record's cells by their formats, appending the delivered values to `values`.
    ///
    /// Returns `false`, with `values` as it was, for a record of the wrong width
    /// or with a cell its format rejects.
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

    /// Returns the number of columns of a file's first record, or `None` for an empty file,
    /// read with the given quoting, and past a header if the first record is one.
    ///
    /// This is how an undeclared table is sized.
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

    /// Returns the number of columns of the first record of an opened stream,
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

/// A writer of delimiter-separated values, the inverse of [DsvReader] with its options.
///
/// Cells serialize by their column's format.
/// By default that is the `term` column format:
/// integers and IRIs bare, strings quoted, and everything else in canonical typed form.
/// The CSV layer quotes a field only if it holds the delimiter, a quote, or a line break,
/// and doubles the quotes inside.
/// No header row is written.
/// A [Skip](ValueFormat::Skip) column is left out of every record,
/// and a row with a cell that does not fit its column's format is dropped and counted.
#[derive(Debug, Clone)]
pub struct DsvWriter {
    /// The target shape.
    options: DsvOptions,
}

impl DsvWriter {
    /// Creates a writer for targets of the given shape.
    pub fn new(options: DsvOptions) -> Self {
        Self { options }
    }
}

impl Writer for DsvWriter {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Writes rows to a target.
    ///
    /// # Errors
    ///
    /// Returns an error if the target cannot be written.
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
        writer.flush().map_err(|error| {
            IoError::StreamUnwritable {
                reason: error.to_string(),
            }
            .diagnostic()
        })?;
        Ok(written)
    }
}

/// A reader of delimiter-separated values, decoding rows into the physical layer's row buffers.
///
/// Reading streams: records are parsed by their columns' formats into runs of bounded size,
/// and each run is interned as it fills.
/// A malformed row is counted and skipped, never silently dropped:
/// a row of the wrong arity, with a cell its format rejects, or with a value the dictionary
/// cannot hold.
/// The delivered rows arrive in file order, unsorted, and storing them sorts them.
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
/// // The row whose second cell is not an integer is malformed: counted, not delivered.
/// assert_eq!(loaded.rows.len(), 2);
/// assert_eq!((loaded.malformed, loaded.first_malformed), (1, Some(2)));
/// ```
#[derive(Debug, Clone)]
pub struct DsvReader {
    /// The resource shape.
    options: DsvOptions,
}

impl DsvReader {
    /// Creates a reader for resources of the given shape.
    pub fn new(options: DsvOptions) -> Self {
        Self { options }
    }

    /// Parses all rows of a resource into [runs](Parsed) without touching a dictionary.
    ///
    /// These are the runs [read](Self::read) and the windows of a
    /// [ResourceBatch](crate::load::ResourceBatch) intern, taken from `runs`.
    /// Each run goes to `emit` once it is full, and the last one when the resource ends,
    /// with the lines the parse consumed.
    /// An `emit` returning `false` ends the parse.
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
        // Well-formed rows so far, across the runs: what the limit counts.
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

    /// Feeds every record of a resource to the visitor, with its line number,
    /// until the visitor returns `false`.
    ///
    /// Returns the number of lines consumed.
    /// A record-level error (broken quoting, invalid UTF-8) fails the read.
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

        // One record is reused, so a row costs no allocation.
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
        // so the consumed count is one less, the base a following
        // window of the same file continues from.
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
    /// Returns an error if the stream fails or a record cannot be read:
    /// broken quoting, or text that is not valid UTF-8.
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

    use crate::format::ValueFormat;

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

    #[test]
    fn malformed_rows_are_counted_never_silent() {
        let mut dictionary = Dictionary::new();
        let reader = DsvReader::new(DsvOptions::csv(vec![
            ValueFormat::String,
            ValueFormat::Integer,
        ]));
        // Line 2 has a bad integer, line 3 the wrong arity.
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

    #[test]
    fn compressed_writes_read_back() {
        let path =
            std::env::temp_dir().join(format!("nemo-io-writer-{}.csv.gz", std::process::id()));
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
        std::fs::remove_file(&path).ok();
        assert_eq!(loaded.rows.len(), 1);
        let read: Vec<DataValue> = loaded
            .rows
            .row(0)
            .iter()
            .map(|&value| {
                dictionary
                    .decode(value)
                    .expect("stored values decode")
                    .to_owned()
            })
            .collect();
        assert_eq!(read, rows[0], "the write inverts the read");
    }

    /// Writes rows in the `term` column format and reads them back.
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
        (0..loaded.rows.len())
            .map(|index| {
                loaded
                    .rows
                    .row(index)
                    .iter()
                    .map(|&value| dictionary.decode(value).expect("decodes").to_owned())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn escaped_strings_read_back_as_written() {
        // Quotes, backslashes, and line breaks are escaped inside the
        // quoted export form, and the term reading resolves them, in
        // plain strings, in language strings, and in typed lexicals alike.
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

    #[test]
    fn the_limit_counts_rows_before_the_condition() {
        use nemo_physical::function::{OpCode, StackProgram};

        // The first three well-formed rows are `1..=3`, whatever the
        // condition keeps of them: a pushed condition reduces a limited
        // import, it never reads further into the source for it.
        let mut options = DsvOptions::csv(vec![ValueFormat::Integer]);
        options.filter = Some(RowFilter::new(
            vec![0],
            StackProgram::new(
                vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
                vec![DataValue::integer(2)],
                1,
            ),
        ));
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

    #[test]
    fn a_condition_reads_the_callers_instant() {
        use nemo_physical::{
            function::{OpCode, StackProgram},
            value::{DateTime, TimezoneOffset},
        };

        // The row keeps 1970, the year of `NOW()` only at the epoch.
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
