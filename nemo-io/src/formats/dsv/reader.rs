//! Reading delimiter-separated values.

use std::io::Read;

use csv::ReaderBuilder;

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    compression::Compression,
    error::IoError,
    load::{Loaded, Reader},
};

use super::{ColumnLayout, DsvOptions, Parsed, ResourceLoad, RunPool};

/// A reader of delimiter-separated values, which decodes rows into row buffers.
///
/// The reader streams: it parses records by their columns' formats into runs of bounded size,
/// and interns each run once it is full.
/// A malformed row is counted and skipped.
/// A row is malformed if it has the wrong width, if a cell's format rejects the cell,
/// or if the dictionary cannot hold one of its values.
/// A row with an empty cell in a column that does not admit a missing value is counted apart,
/// unless the column's format reads the empty text.
/// If the options name the [columns](DsvOptions::columns), the first record is the header,
/// and each column is found by its name.
/// A null in a cell is a null of the resource: a label names the same null throughout one
/// read, and another null in every other read.
/// The delivered rows are in the order of the file, unsorted.
///
/// # Example
///
/// ```
/// use nemo_io::{
///     dsv::{DsvOptions, DsvReader},
///     value_format::ValueFormat,
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
/// assert_eq!((loaded.counts.malformed, loaded.counts.first_malformed), (1, Some(2)));
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
    /// `context` is the evaluation context of the options' condition.
    ///
    /// `layout` says where a record has the cell of each format, if that is known already,
    /// as it is for the windows of a resource whose header was read before.
    /// Without it, a resource with named columns starts with its header,
    /// and the cells of any other resource go by position.
    ///
    /// # Errors
    ///
    /// Returns an error if a record cannot be read,
    /// or if the header lacks one of the named columns or has it more than once.
    pub(crate) fn parse(
        &self,
        source: impl Read,
        runs: &RunPool,
        context: EvaluationContext,
        layout: Option<&ColumnLayout>,
        mut emit: impl FnMut(Parsed) -> bool,
    ) -> Result<(), Diagnostic> {
        let header = layout.is_none() && self.options.columns.is_some();
        let mut reader = self.csv_reader(source, self.options.ignore_headers || header);
        let read;
        let layout = match (layout, &self.options.columns) {
            (Some(layout), _) => layout,
            (None, Some(names)) => {
                let header = reader.headers().map_err(IoError::input_unreadable)?;
                read = ColumnLayout::of_header(header, names, &self.options.formats)?;
                &read
            }
            (None, None) => {
                read = ColumnLayout::positional(self.options.formats.len());
                &read
            }
        };

        let limit = self.options.limit.unwrap_or(u64::MAX);
        let arity = self.options.arity();
        let mut run = runs.take(arity);

        // The well-formed rows so far, across the runs, which the limit counts.
        let mut well_formed = 0;
        let mut wanted = true;
        let consumed = Self::records(reader, |record, line| {
            if well_formed >= limit {
                return false;
            }

            let before = run.well_formed();
            run.push_record(&self.options, layout, context, record, line);
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

    /// Returns where the records of a resource with named columns have the cell of each
    /// format, read from the header that `source` starts with,
    /// or `None` if the options name no columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be read,
    /// or if it lacks one of the named columns or has it more than once.
    pub(crate) fn header_layout(
        &self,
        source: impl Read,
    ) -> Result<Option<ColumnLayout>, Diagnostic> {
        let Some(names) = &self.options.columns else {
            return Ok(None);
        };

        let mut reader = self.csv_reader(source, true);
        let header = reader.headers().map_err(IoError::input_unreadable)?;

        ColumnLayout::of_header(header, names, &self.options.formats).map(Some)
    }

    /// Returns a reader of the records of a resource,
    /// which leaves out the first record if `skip_first` is true.
    fn csv_reader<R: Read>(&self, source: R, skip_first: bool) -> csv::Reader<R> {
        ReaderBuilder::new()
            .delimiter(self.options.delimiter)
            .has_headers(skip_first)
            .quoting(self.options.quoting)
            .flexible(true)
            .from_reader(source)
    }

    /// Passes every record of a resource to `visit`, with its line number,
    /// until `visit` returns `false`.
    ///
    /// Returns the number of lines consumed.
    /// Fails for a record that cannot be read, such as one with broken quoting or invalid UTF-8.
    fn records(
        mut reader: csv::Reader<impl Read>,
        mut visit: impl FnMut(&csv::StringRecord, u64) -> bool,
    ) -> Result<u64, Diagnostic> {
        // One record is reused, so that a row needs no allocation.
        let mut record = csv::StringRecord::new();
        while reader
            .read_record(&mut record)
            .map_err(IoError::input_unreadable)?
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
    /// With named columns, it also returns an error if the header lacks one of the names
    /// or has it more than once.
    fn read(
        &self,
        source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let runs = RunPool::default();
        let mut load = ResourceLoad::new(&self.options, context);
        let parsed = self.parse(source, &runs, context, None, |run| {
            run.encode_into(&mut load, dictionary, 0);
            runs.give(run);
            true
        });
        let loaded = load.finish(dictionary);
        parsed?;

        tracing::debug!(rows = loaded.rows.len(), counts = ?loaded.counts, "read a delimited source");

        Ok(loaded)
    }
}
