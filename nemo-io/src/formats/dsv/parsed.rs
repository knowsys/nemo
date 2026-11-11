//! Runs of parsed rows awaiting interning.

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    value::{DataValue, IriRef, ReadValue, ValueRef},
};

use crate::{
    dsv::DsvOptions,
    load::{BlankNodes, Loaded},
    selection::RowSelection,
    value_format::ValueFormat,
};

/// The number of cells at which a run is full and is handed on for interning.
const RUN_CELLS: usize = 1 << 14;

/// The number of bytes of text at which a run is full and is handed on for interning.
const RUN_TEXT: usize = 1 << 20;

/// One parsed cell, in the form from which the thread that owns the dictionary interns it.
#[derive(Debug, Clone, Copy)]
enum ParsedCell {
    /// A value with an inline encoding, already encoded.
    Inline(StorageValue),
    /// An IRI whose text is at this byte range of the run's text.
    Iri {
        /// Where the text starts.
        start: u32,
        /// Where the text ends.
        end: u32,
    },
    /// A plain string whose text is at this byte range of the run's text.
    String {
        /// Where the text starts.
        start: u32,
        /// Where the text ends.
        end: u32,
    },
    /// Any other value, at this index of the run's owned values.
    Owned(u32),
}

/// The lengths of a run's buffers before a row was added, for taking the row back.
#[derive(Debug, Clone, Copy)]
struct RowStart {
    /// The number of cells.
    cells: usize,
    /// The length of the text.
    text: usize,
    /// The number of owned values.
    owned: usize,
}

/// A well-formed row that holds a null, kept as text until its run is interned.
///
/// A null is named in the label scope of its resource, which only the interning thread reads.
#[derive(Debug, Clone, Copy)]
struct DeferredRow {
    /// The number of the run's parsed rows that come before this row.
    position: usize,
    /// The line of the row in the resource.
    line: u64,
}

/// What reading the deferred rows of one resource needs: the resource's options,
/// its selection, and the label scope of its nulls.
///
/// The scope is opened by the first row that holds a null.
#[derive(Debug)]
pub(crate) struct DeferredReads<'a> {
    /// The options of the resource.
    options: &'a DsvOptions,
    /// The selection of the options.
    selection: RowSelection,
    /// The nulls of the resource, once a row has named one.
    blanks: Option<BlankNodes>,
}

impl<'a> DeferredReads<'a> {
    /// Creates the state for a resource with the given options.
    pub(crate) fn new(options: &'a DsvOptions) -> Self {
        Self {
            options,
            selection: options.selection(),
            blanks: None,
        }
    }

    /// Ends the reads of the resource, which closes the label scope of its nulls.
    pub(crate) fn close(self, dictionary: &mut Dictionary) {
        if let Some(blanks) = self.blanks {
            blanks.close(dictionary);
        }
    }

    /// Reads one deferred row from the text of its cells, and takes it into `loaded`.
    ///
    /// `cells` has the byte range in `text` of each cell of the row.
    /// `values` and `row` are scratch buffers, and `line` is the row's line.
    #[allow(clippy::too_many_arguments)]
    fn take(
        &mut self,
        cells: &[(usize, usize)],
        text: &str,
        loaded: &mut Loaded,
        dictionary: &mut Dictionary,
        values: &mut Vec<DataValue>,
        row: &mut Vec<StorageValue>,
        line: u64,
    ) {
        let blanks = self
            .blanks
            .get_or_insert_with(|| BlankNodes::open(None, "document", dictionary));

        values.clear();
        for (column, (&(start, end), format)) in cells.iter().zip(&self.options.formats).enumerate()
        {
            if self.options.delivers(column) {
                let value = format
                    .parse(&text[start..end], &mut |label| {
                        blanks.null(label, dictionary)
                    })
                    .expect("a deferred row is well-formed");
                values.push(value);
            }
        }

        loaded.take(values, &self.selection, dictionary, row, line);
    }
}

/// A run: rows parsed without a dictionary and awaiting interning.
///
/// A parse of a resource, or of one [window](crate::load::ResourceBatch) of it,
/// hands its rows on in runs of bounded size,
/// and the runs are [interned](Self::encode_into) in the order they were parsed.
/// - Values with an inline encoding are encoded as they are parsed.
/// - IRIs and strings keep their text in one buffer of the run.
/// - Only values of other kinds are kept as owned values.
/// - A row that holds a null is kept as text, and read when the run is interned.
#[derive(Debug)]
pub(crate) struct Parsed {
    /// The run's counts of well-formed and dropped rows.
    ///
    /// Its rows stay empty, since the run keeps its rows in [cells](Self::cells)
    /// until they are [interned](Self::encode_into).
    loaded: Loaded,
    /// The cells of the admitted rows, row by row, with as many cells per row as the arity.
    cells: Vec<ParsedCell>,
    /// The text of the IRI and string cells.
    text: String,
    /// The values of the cells of other kinds.
    owned: Vec<DataValue>,
    /// The line in the resource of each admitted row.
    ///
    /// This list also counts the rows, which the cells alone cannot do for arity zero.
    lines: Vec<u64>,
    /// The rows that hold a null, in the order of the resource.
    deferred: Vec<DeferredRow>,
    /// The byte range in the run's text of each cell of the deferred rows,
    /// with one range per column of the resource.
    deferred_cells: Vec<(usize, usize)>,
    /// A scratch buffer for the values of a record that the condition is evaluated on.
    values: Vec<DataValue>,
    /// The number of lines that the parse consumed, set on its last run only.
    ///
    /// It is the line number at which the text after the parsed part begins, less one.
    pub(crate) consumed_lines: u64,
}

impl Parsed {
    /// Creates an empty run for rows of the given arity.
    ///
    /// `context` is the evaluation context of the selection's condition,
    /// such as the instant its `NOW()` reads.
    pub(crate) fn new(arity: usize, context: EvaluationContext) -> Self {
        Self {
            loaded: Loaded::new(arity, context),
            cells: Vec::new(),
            text: String::new(),
            owned: Vec::new(),
            lines: Vec::new(),
            deferred: Vec::new(),
            deferred_cells: Vec::new(),
            values: Vec::new(),
            consumed_lines: 0,
        }
    }

    /// Empties the run for rows of the given arity, keeping its buffers.
    ///
    /// `context` is the evaluation context of the selection's condition, as for [new](Self::new).
    pub(crate) fn reset(&mut self, arity: usize, context: EvaluationContext) {
        self.loaded = Loaded::new(arity, context);
        self.cells.clear();
        self.text.clear();
        self.owned.clear();
        self.lines.clear();
        self.deferred.clear();
        self.deferred_cells.clear();
        self.consumed_lines = 0;
    }

    /// Returns the number of well-formed rows that the run has counted, admitted or not.
    pub(crate) fn well_formed(&self) -> u64 {
        self.loaded.well_formed + self.deferred.len() as u64
    }

    /// Returns the size in bytes of the run's buffers, which a reused run keeps.
    pub(crate) fn bytes(&self) -> usize {
        self.cells.capacity() * size_of::<ParsedCell>()
            + self.text.capacity()
            + self.owned.capacity() * size_of::<DataValue>()
            + self.lines.capacity() * size_of::<u64>()
            + self.deferred.capacity() * size_of::<DeferredRow>()
            + self.deferred_cells.capacity() * size_of::<(usize, usize)>()
    }

    /// Returns whether the run is full and should be handed on.
    pub(crate) fn is_full(&self) -> bool {
        self.cells.len() >= RUN_CELLS || self.text.len() >= RUN_TEXT
    }

    /// Adds one record, read with the given options,
    /// if it is well-formed and the selection admits it.
    ///
    /// Otherwise, the record is counted as malformed, as excluded,
    /// or under the kind of the error that the condition raised on it.
    /// A well-formed record that holds a null is kept as text,
    /// and is read when the run is [interned](Self::encode_into).
    pub(crate) fn push_record(
        &mut self,
        options: &DsvOptions,
        selection: &RowSelection,
        record: &csv::StringRecord,
        line: u64,
    ) {
        if options.condition.is_none() {
            self.push_direct(options, record, line);
        } else {
            self.push_selected(options, selection, record, line);
        }
    }

    /// Adds a record when there is no condition to evaluate.
    ///
    /// Its cells go into the run as they are parsed, without first becoming owned values.
    fn push_direct(&mut self, options: &DsvOptions, record: &csv::StringRecord, line: u64) {
        let start = self.row_start();
        let mut holds_null = false;
        let mut null = |_: &str| {
            holds_null = true;
            DataValue::null(0)
        };
        let complete =
            record.len() == options.formats.len()
                && record.iter().zip(&options.formats).enumerate().all(
                    |(column, (cell, &format))| {
                        if options.checks_only(column) {
                            format.parse_cell(cell, &mut null).is_some()
                        } else {
                            self.push_cell(format, cell, &mut null)
                        }
                    },
                );
        if !complete {
            self.take_back(start);
            self.loaded.count_malformed(line);
        } else if holds_null {
            self.take_back(start);
            self.defer(record, line);
        } else {
            self.loaded.well_formed += 1;
            self.lines.push(line);
        }
    }

    /// Adds a record if it is well-formed and the selection's condition admits its values.
    fn push_selected(
        &mut self,
        options: &DsvOptions,
        selection: &RowSelection,
        record: &csv::StringRecord,
        line: u64,
    ) {
        let mut values = std::mem::take(&mut self.values);
        values.clear();
        let mut holds_null = false;
        let complete = options.parse_row(record, &mut values, &mut |_| {
            holds_null = true;
            DataValue::null(0)
        });
        if !complete {
            self.loaded.count_malformed(line);
        } else if holds_null {
            self.defer(record, line);
        } else if self.loaded.admit(selection, &values) {
            for value in values.drain(..) {
                self.push_value(value);
            }

            self.lines.push(line);
        }

        self.values = values;
    }

    /// Keeps a well-formed record that holds a null as text.
    fn defer(&mut self, record: &csv::StringRecord, line: u64) {
        self.deferred.push(DeferredRow {
            position: self.lines.len(),
            line,
        });
        for cell in record {
            let start = self.text.len();
            self.text.push_str(cell);
            self.deferred_cells.push((start, self.text.len()));
        }
    }

    /// Returns the current lengths of the run's buffers,
    /// for taking back a row that turns out to be malformed.
    fn row_start(&self) -> RowStart {
        RowStart {
            cells: self.cells.len(),
            text: self.text.len(),
            owned: self.owned.len(),
        }
    }

    /// Removes the cells, text and owned values added since `start`.
    fn take_back(&mut self, start: RowStart) {
        self.cells.truncate(start.cells);
        self.text.truncate(start.text);
        self.owned.truncate(start.owned);
    }

    /// Parses one cell by its format and adds it,
    /// or returns `false` if the format rejects the cell.
    ///
    /// An IRI or a string that appears in the cell unchanged goes into the run's text
    /// without becoming an owned value.
    /// A skipped cell adds nothing.
    /// `null` is called for each null that the cell holds.
    fn push_cell(
        &mut self,
        format: ValueFormat,
        cell: &str,
        null: &mut dyn FnMut(&str) -> DataValue,
    ) -> bool {
        if format == ValueFormat::Skip {
            return true;
        }

        match format.parse_cell(cell, null) {
            Some(ReadValue::Iri(iri)) => self.push_text(iri, true),
            Some(ReadValue::String(text)) => self.push_text(text, false),
            Some(ReadValue::Value(value)) => self.push_value(value),
            None => return false,
        }

        true
    }

    /// Adds a parsed value.
    ///
    /// The value is encoded if it has an inline encoding.
    /// Otherwise, an IRI or a string keeps its text in the run's text,
    /// and any other value is kept as an owned value.
    fn push_value(&mut self, value: DataValue) {
        let cell = match StorageValue::from_value(value.as_ref()) {
            Some(inline) => Some(ParsedCell::Inline(inline)),
            None => match value.as_ref() {
                ValueRef::Iri(iri) => self.text_cell([iri.namespace(), iri.local()], true),
                ValueRef::String(text) => self.text_cell([text, ""], false),
                _ => None,
            },
        };
        let cell = cell.unwrap_or_else(|| self.owned_cell(value));
        self.cells.push(cell);
    }

    /// Adds the cell of an IRI if `iri` is true, or of a string otherwise, with the given text.
    fn push_text(&mut self, text: &str, iri: bool) {
        let cell = match self.text_cell([text, ""], iri) {
            Some(cell) => cell,
            None if iri => self.owned_cell(DataValue::iri(text)),
            None => self.owned_cell(DataValue::string(text)),
        };
        self.cells.push(cell);
    }

    /// Appends a text, given in two parts, to the run's text,
    /// and returns its cell, of an IRI if `iri` is true and of a string otherwise.
    ///
    /// Returns `None`, and appends nothing, if the text's offsets would not fit in 32 bits.
    fn text_cell(&mut self, parts: [&str; 2], iri: bool) -> Option<ParsedCell> {
        let start = u32::try_from(self.text.len()).ok()?;
        let end = u32::try_from(self.text.len() + parts[0].len() + parts[1].len()).ok()?;
        self.text.push_str(parts[0]);
        self.text.push_str(parts[1]);

        Some(if iri {
            ParsedCell::Iri { start, end }
        } else {
            ParsedCell::String { start, end }
        })
    }

    /// Keeps a value as an owned value and returns its cell.
    fn owned_cell(&mut self, value: DataValue) -> ParsedCell {
        let index = u32::try_from(self.owned.len()).expect("a run holds fewer than 2³² values");
        self.owned.push(value);
        ParsedCell::Owned(index)
    }

    /// Interns the run's rows into `loaded`, the load being built, and adds the run's counts to
    /// it.
    ///
    /// `base` is the number of lines that earlier windows of the same file consumed,
    /// and is added to the run's line numbers.
    /// A row with a value that the dictionary cannot hold is counted as malformed.
    /// A row that holds a null is read here, with its nulls named in the label scope of
    /// `reads`, and is admitted by the selection's condition like any other row.
    pub(crate) fn encode_into(
        &self,
        loaded: &mut Loaded,
        reads: &mut DeferredReads<'_>,
        dictionary: &mut Dictionary,
        base: u64,
    ) {
        let shifted = |line: u64| if line > 0 { line + base } else { 0 };

        // The rows are stored first, in line order, after the rows of every
        // earlier run. The first malformed line of the parse is merged afterwards,
        // since it may come before the line of a row that failed to encode.
        let arity = self.loaded.rows.arity();
        let mut row = Vec::with_capacity(arity);
        let mut values = Vec::new();
        let mut deferred = self.deferred.iter().enumerate().peekable();
        for (index, &line) in self.lines.iter().enumerate() {
            while let Some((number, later)) = deferred.next_if(|(_, row)| row.position <= index) {
                let cells = self.deferred_cells(number, reads.options);
                let line = shifted(later.line);
                reads.take(
                    cells,
                    &self.text,
                    loaded,
                    dictionary,
                    &mut values,
                    &mut row,
                    line,
                );
            }

            row.clear();
            let complete = self.cells[index * arity..(index + 1) * arity]
                .iter()
                .all(|&cell| {
                    self.encode(cell, dictionary).is_some_and(|value| {
                        row.push(value);
                        true
                    })
                });
            if complete {
                loaded.rows.push_row(&row);
            } else {
                loaded.count_malformed(shifted(line));
            }
        }

        for (number, later) in deferred {
            let cells = self.deferred_cells(number, reads.options);
            let line = shifted(later.line);
            reads.take(
                cells,
                &self.text,
                loaded,
                dictionary,
                &mut values,
                &mut row,
                line,
            );
        }

        loaded.malformed += self.loaded.malformed;
        loaded.excluded += self.loaded.excluded;
        loaded.well_formed += self.loaded.well_formed;
        loaded.errors.absorb(&self.loaded.errors);
        if let Some(first) = self
            .loaded
            .first_malformed
            .map(shifted)
            .filter(|&line| line > 0)
        {
            loaded.first_malformed = Some(
                loaded
                    .first_malformed
                    .map_or(first, |known| known.min(first)),
            );
        }
    }

    /// Returns the byte ranges of the cells of the deferred row with the given number.
    fn deferred_cells(&self, number: usize, options: &DsvOptions) -> &[(usize, usize)] {
        let width = options.formats.len();

        &self.deferred_cells[number * width..(number + 1) * width]
    }

    /// Encodes one cell, or returns `None` if the dictionary cannot hold its value.
    fn encode(&self, cell: ParsedCell, dictionary: &mut Dictionary) -> Option<StorageValue> {
        let text = |start: u32, end: u32| &self.text[start as usize..end as usize];
        match cell {
            ParsedCell::Inline(value) => Some(value),
            ParsedCell::Iri { start, end } => {
                dictionary.encode(ValueRef::Iri(IriRef::new(text(start, end))))
            }
            ParsedCell::String { start, end } => {
                dictionary.encode(ValueRef::String(text(start, end)))
            }
            ParsedCell::Owned(index) => dictionary.encode(self.owned[index as usize].as_ref()),
        }
    }
}
