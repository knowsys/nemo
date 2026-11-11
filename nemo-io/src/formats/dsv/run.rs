//! Runs of parsed records awaiting interning.

use nemo_physical::{
    dictionary::Dictionary, function::EvaluationContext, storage::StorageValue, value::DataValue,
};

use crate::{
    dsv::{ColumnLayout, DsvReaderOptions},
    load::{
        BlankNodes, Loaded, RowIntake,
        run::{Run, RunPool, line_in_resource},
    },
    value_format::{CellRead, ValueFormat},
};

/// A well-formed row that holds a null inside a cell, kept as text until its run is interned.
///
/// A null is named in the label scope of its resource, which only the interning thread reads.
#[derive(Debug, Clone, Copy)]
struct DeferredRow {
    /// The number of the run's kept rows that come before this row.
    position: usize,
    /// The line of the row in the resource.
    line: u64,
}

/// The load of one delimiter-separated resource while its runs are interned.
#[derive(Debug)]
pub(crate) struct DsvLoad<'a> {
    /// The options of the resource.
    options: &'a DsvReaderOptions,
    /// The intake of the resource's rows.
    intake: RowIntake,
    /// The nulls of the resource, once a row has named one.
    ///
    /// The label scope is opened by the first row that holds a null.
    blanks: Option<BlankNodes>,
    /// A scratch buffer for the values of a row that is read while its run is interned.
    values: Vec<DataValue>,
    /// A scratch buffer for the encoded values of a row.
    row: Vec<StorageValue>,
}

impl<'a> DsvLoad<'a> {
    /// Creates the empty load of a resource with the given options.
    ///
    /// `context` is the evaluation context of the options' condition.
    pub(crate) fn new(options: &'a DsvReaderOptions, context: EvaluationContext) -> Self {
        Self {
            options,
            intake: RowIntake::of_arity(options.arity(), options.selection(), context),
            blanks: None,
            values: Vec::new(),
            row: Vec::new(),
        }
    }

    /// Ends the load, which closes the label scope of the resource's nulls, and returns it.
    pub(crate) fn finish(self, dictionary: &mut Dictionary) -> Loaded {
        if let Some(blanks) = self.blanks {
            blanks.close(dictionary);
        }

        self.intake.finish()
    }

    /// Reads one deferred row from the text of its cells, and takes it into the load.
    ///
    /// `cells` has the byte range in `text` of the cell of each format,
    /// and `line` is the row's line.
    fn take_deferred(
        &mut self,
        cells: &[(usize, usize)],
        text: &str,
        dictionary: &mut Dictionary,
        line: u64,
    ) {
        let blanks = self
            .blanks
            .get_or_insert_with(|| BlankNodes::open(None, "document", dictionary));

        self.values.clear();
        for (column, (&(start, end), format)) in cells.iter().zip(&self.options.formats).enumerate()
        {
            if !self.options.delivers(column) {
                continue;
            }

            let mut null = |label: &str| blanks.null(label, dictionary);
            let value = match format.read(&text[start..end], &mut null) {
                CellRead::Value(value) => value.into_owned(),
                CellRead::Missing => DataValue::null(dictionary.fresh_null()),
                CellRead::Unadmitted | CellRead::Malformed => {
                    unreachable!("a deferred row is well-formed")
                }
            };
            self.values.push(value);
        }

        self.intake.take(&self.values, dictionary, line);
    }
}

/// A run of the records of a delimiter-separated resource,
/// parsed without a dictionary and awaiting interning.
///
/// A row that holds a null inside a cell is kept as text,
/// and read when the run is [interned](Self::encode_into).
#[derive(Debug)]
pub(crate) struct DsvRun {
    /// The rows that are parsed, and the text of the deferred rows.
    rows: Run,
    /// The rows that hold a null inside a cell, in the order of the resource.
    deferred: Vec<DeferredRow>,
    /// The byte range in the run's text of each cell of the deferred rows,
    /// with one range per format of the resource's options.
    deferred_cells: Vec<(usize, usize)>,
}

impl DsvRun {
    /// Returns an empty run for rows of the given arity: a spare run of the pool,
    /// or a new run if the pool has none.
    pub(crate) fn from_pool(runs: &RunPool<Self>, arity: usize) -> Self {
        match runs.take() {
            Some(mut run) => {
                run.rows.reset(arity);
                run.deferred.clear();
                run.deferred_cells.clear();

                run
            }
            None => Self {
                rows: Run::new(arity),
                deferred: Vec::new(),
                deferred_cells: Vec::new(),
            },
        }
    }

    /// Returns the number of well-formed rows that the run has counted, admitted or not.
    pub(crate) fn well_formed(&self) -> u64 {
        self.rows.counts.well_formed + self.deferred.len() as u64
    }

    /// Returns the size in bytes of the run's buffers, which a reused run keeps.
    pub(crate) fn bytes(&self) -> usize {
        self.rows.bytes()
            + self.deferred.capacity() * size_of::<DeferredRow>()
            + self.deferred_cells.capacity() * size_of::<(usize, usize)>()
    }

    /// Returns whether the run is full and should be handed on.
    pub(crate) fn is_full(&self) -> bool {
        self.rows.is_full()
    }

    /// Returns the number of lines that the parse consumed, if this is its last run,
    /// and `0` otherwise.
    pub(crate) fn consumed_lines(&self) -> u64 {
        self.rows.consumed_lines
    }

    /// Makes this the last run of a parse that consumed the given number of lines.
    pub(crate) fn end_parse(&mut self, consumed_lines: u64) {
        self.rows.consumed_lines = consumed_lines;
    }

    /// Adds one record, read with the given options,
    /// if it is well-formed and the options' condition admits it.
    ///
    /// Otherwise, the record is counted: as malformed, as a row with a missing value that its
    /// column does not admit, as excluded, or under the kind of the error that the condition
    /// raised on it.
    /// A well-formed record that holds a null inside a cell is kept as text,
    /// and is read when the run is [interned](Self::encode_into).
    /// `layout` says where the record has the cell of each format,
    /// and `context` is the evaluation context of the condition.
    pub(crate) fn push_record(
        &mut self,
        options: &DsvReaderOptions,
        layout: &ColumnLayout,
        context: EvaluationContext,
        record: &csv::StringRecord,
        line: u64,
    ) {
        if record.len() != layout.width() {
            self.rows.counts.count_malformed(line);
            return;
        }

        // A cell that is not read ends the row, except for a missing value:
        // the cells after it still decide whether the row is malformed.
        let start = self.rows.values.row_start();
        let mut holds_null = false;
        let mut unadmitted = false;
        let mut null = |_: &str| {
            holds_null = true;
            DataValue::null(0)
        };
        for (column, format) in options.formats.iter().enumerate() {
            if format.value == ValueFormat::Skip {
                continue;
            }

            let cell = &record[layout.position(column)];
            let delivered = !options.checks_only(column) && !unadmitted;
            match format.read(cell, &mut null) {
                CellRead::Value(value) if delivered => self.rows.values.push_read(value),
                CellRead::Missing if delivered => self.rows.values.push_missing(),
                CellRead::Value(_) | CellRead::Missing => {}
                CellRead::Unadmitted => unadmitted = true,
                CellRead::Malformed => {
                    self.rows.values.take_back(start);
                    self.rows.counts.count_malformed(line);
                    return;
                }
            }
        }

        if unadmitted {
            self.rows.values.take_back(start);
            self.rows.counts.count_missing(line);
        } else if holds_null {
            self.rows.values.take_back(start);
            self.defer(record, options, layout, line);
        } else {
            self.rows
                .end_row(start, options.condition.as_ref(), context, line);
        }
    }

    /// Keeps a well-formed record that holds a null inside a cell as text:
    /// the cell of each format, in the order of the formats.
    fn defer(
        &mut self,
        record: &csv::StringRecord,
        options: &DsvReaderOptions,
        layout: &ColumnLayout,
        line: u64,
    ) {
        self.deferred.push(DeferredRow {
            position: self.rows.len(),
            line,
        });
        for (column, format) in options.formats.iter().enumerate() {
            let cell = match format.value {
                ValueFormat::Skip => "",
                _ => &record[layout.position(column)],
            };
            self.deferred_cells.push(self.rows.values.keep_text(cell));
        }
    }

    /// Interns the run's rows into the load of their resource, and adds the run's counts to it.
    ///
    /// `base` is the number of lines that earlier windows of the same file consumed,
    /// and is added to the run's line numbers.
    /// A row with a value that the dictionary cannot hold is counted as malformed.
    /// A row that holds a null inside a cell is read here,
    /// with its nulls named in the label scope of the resource.
    /// The condition admits it, and a row with a missing value, here.
    pub(crate) fn encode_into(
        &self,
        load: &mut DsvLoad<'_>,
        dictionary: &mut Dictionary,
        base: u64,
    ) {
        // The rows are stored first, in line order, after the rows of every earlier run.
        // The counts of the parse are merged afterwards,
        // since its first malformed line may come before the line of a row that fails to encode.
        let width = load.options.formats.len();
        let text = self.rows.values.text();
        let mut deferred = self.deferred.iter().enumerate().peekable();
        for index in 0..self.rows.len() {
            while let Some((number, later)) = deferred.next_if(|(_, row)| row.position <= index) {
                let ranges = &self.deferred_cells[number * width..(number + 1) * width];
                load.take_deferred(ranges, text, dictionary, line_in_resource(later.line, base));
            }

            self.rows.intern_row(
                index,
                &mut load.intake,
                &mut load.blanks,
                dictionary,
                &mut load.row,
                base,
            );
        }

        for (number, later) in deferred {
            let ranges = &self.deferred_cells[number * width..(number + 1) * width];
            load.take_deferred(ranges, text, dictionary, line_in_resource(later.line, base));
        }

        load.intake.absorb(&self.rows.counts, base);
    }
}
