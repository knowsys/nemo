//! Reading delimiter-separated values.

use std::io::Read;

use csv::ReaderBuilder;

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    compression::Compression,
    error::IoError,
    load::{Loaded, Reader},
};

use super::{DsvOptions, Parsed, RunPool};

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
