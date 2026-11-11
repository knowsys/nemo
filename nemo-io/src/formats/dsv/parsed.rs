//! Runs of parsed rows awaiting interning.

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    value::{DataValue, IriRef, ReadValue, ValueRef},
};

use crate::{
    dsv::DsvOptions,
    load::{BlankNodes, Loaded, RowCounts, RowIntake},
    selection::ConditionBuffers,
    value_format::{CellRead, ValueFormat},
};

/// The number of cells at which a run is full and is handed on for interning.
const RUN_CELLS: usize = 1 << 14;

/// The number of bytes of text at which a run is full and is handed on for interning.
const RUN_TEXT: usize = 1 << 20;

/// The number of the null that stands for the missing value of the first column
/// while a condition is evaluated on a row that is not interned yet.
///
/// The missing value of each further column has the next number,
/// so the nulls of one row differ, as the fresh nulls that they are interned as do.
const MISSING_NULL: u64 = u64::MAX - 0xFFFF_FFFF;

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
    /// A missing value that its column admits, which is interned as a fresh null.
    Missing,
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

/// The load of one delimiter-separated resource while its runs are interned.
#[derive(Debug)]
pub(crate) struct ResourceLoad<'a> {
    /// The options of the resource.
    options: &'a DsvOptions,
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

impl<'a> ResourceLoad<'a> {
    /// Creates the empty load of a resource with the given options.
    ///
    /// `context` is the evaluation context of the options' condition.
    pub(crate) fn new(options: &'a DsvOptions, context: EvaluationContext) -> Self {
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
    /// `cells` has the byte range in `text` of each cell of the row, and `line` is the row's
    /// line.
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
    /// The number of delivered columns.
    arity: usize,
    /// The run's counts of well-formed and dropped rows.
    counts: RowCounts,
    /// The cells of the admitted rows, row by row, with as many cells per row as the arity.
    cells: Vec<ParsedCell>,
    /// The text of the IRI and string cells, and of the deferred rows.
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
    /// The buffers of the condition's evaluations.
    buffers: ConditionBuffers,
    /// The number of lines that the parse consumed, set on its last run only.
    ///
    /// It is the line number at which the text after the parsed part begins, less one.
    pub(crate) consumed_lines: u64,
}

impl Parsed {
    /// Creates an empty run for rows of the given arity.
    pub(crate) fn new(arity: usize) -> Self {
        Self {
            arity,
            counts: RowCounts::default(),
            cells: Vec::new(),
            text: String::new(),
            owned: Vec::new(),
            lines: Vec::new(),
            deferred: Vec::new(),
            deferred_cells: Vec::new(),
            buffers: ConditionBuffers::default(),
            consumed_lines: 0,
        }
    }

    /// Empties the run for rows of the given arity, keeping its buffers.
    pub(crate) fn reset(&mut self, arity: usize) {
        self.arity = arity;
        self.counts = RowCounts::default();
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
        self.counts.well_formed + self.deferred.len() as u64
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
    /// if it is well-formed and the options' condition admits it.
    ///
    /// Otherwise, the record is counted: as malformed, as a row with a missing value that its
    /// column does not admit, as excluded, or under the kind of the error that the condition
    /// raised on it.
    /// A well-formed record that holds a null is kept as text,
    /// and is read when the run is [interned](Self::encode_into).
    /// `context` is the evaluation context of the condition.
    pub(crate) fn push_record(
        &mut self,
        options: &DsvOptions,
        context: EvaluationContext,
        record: &csv::StringRecord,
        line: u64,
    ) {
        if record.len() != options.formats.len() {
            self.counts.count_malformed(line);
            return;
        }

        // A cell that is not read ends the row, except for a missing value:
        // the cells after it still decide whether the row is malformed.
        let start = self.row_start();
        let mut holds_null = false;
        let mut unadmitted = false;
        let mut null = |_: &str| {
            holds_null = true;
            DataValue::null(0)
        };
        for (column, (cell, format)) in record.iter().zip(&options.formats).enumerate() {
            if format.value == ValueFormat::Skip {
                continue;
            }

            let delivered = !options.checks_only(column) && !unadmitted;
            match format.read(cell, &mut null) {
                CellRead::Value(value) if delivered => self.push_value(value),
                CellRead::Missing if delivered => self.cells.push(ParsedCell::Missing),
                CellRead::Value(_) | CellRead::Missing => {}
                CellRead::Unadmitted => unadmitted = true,
                CellRead::Malformed => {
                    self.take_back(start);
                    self.counts.count_malformed(line);
                    return;
                }
            }
        }

        if unadmitted {
            self.take_back(start);
            self.counts.count_missing(line);
        } else if holds_null {
            self.take_back(start);
            self.defer(record, line);
        } else {
            self.counts.well_formed += 1;
            if self.admits(options, context, start) {
                self.lines.push(line);
            } else {
                self.take_back(start);
            }
        }
    }

    /// Returns whether the options' condition admits the row whose cells were added last,
    /// from `start` on, and counts the row if it does not.
    fn admits(
        &mut self,
        options: &DsvOptions,
        context: EvaluationContext,
        start: RowStart,
    ) -> bool {
        let Some(condition) = &options.condition else {
            return true;
        };

        let cells = &self.cells[start.cells..];
        let admitted = condition.evaluate_with(context, &mut self.buffers, |position| match cells
            [position]
        {
            ParsedCell::Inline(value) => value.to_value().expect("an inline value decodes"),
            ParsedCell::Iri { start, end } => {
                ValueRef::Iri(IriRef::new(&self.text[start as usize..end as usize]))
            }
            ParsedCell::String { start, end } => {
                ValueRef::String(&self.text[start as usize..end as usize])
            }
            ParsedCell::Owned(index) => self.owned[index as usize].as_ref(),
            ParsedCell::Missing => ValueRef::Null(MISSING_NULL + position as u64),
        });

        match admitted {
            Ok(true) => true,
            Ok(false) => {
                self.counts.excluded += 1;
                false
            }
            Err(kind) => {
                self.counts.errors.record(kind);
                false
            }
        }
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
    /// for taking back a row that is not kept.
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

    /// Adds the cell of a value that was read.
    ///
    /// An IRI or a string that appears in the resource unchanged goes into the run's text
    /// without becoming an owned value.
    fn push_value(&mut self, value: ReadValue<'_>) {
        let cell = match value {
            ReadValue::Iri(iri) => self
                .text_cell([iri, ""], true)
                .unwrap_or_else(|| self.owned_cell(DataValue::iri(iri))),
            ReadValue::String(text) => self
                .text_cell([text, ""], false)
                .unwrap_or_else(|| self.owned_cell(DataValue::string(text))),
            ReadValue::Value(value) => self.value_cell(value),
        };
        self.cells.push(cell);
    }

    /// Returns the cell of an owned value.
    ///
    /// The value is encoded if it has an inline encoding.
    /// Otherwise, an IRI or a string keeps its text in the run's text,
    /// and a value of another kind is kept as an owned value.
    fn value_cell(&mut self, value: DataValue) -> ParsedCell {
        if let Some(inline) = StorageValue::from_value(value.as_ref()) {
            return ParsedCell::Inline(inline);
        }

        let text = match value.as_ref() {
            ValueRef::Iri(iri) => self.text_cell([iri.namespace(), iri.local()], true),
            ValueRef::String(text) => self.text_cell([text, ""], false),
            _ => None,
        };

        text.unwrap_or_else(|| self.owned_cell(value))
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

    /// Interns the run's rows into the load of their resource, and adds the run's counts to it.
    ///
    /// `base` is the number of lines that earlier windows of the same file consumed,
    /// and is added to the run's line numbers.
    /// A row with a value that the dictionary cannot hold is counted as malformed.
    /// A row that holds a null is read here, with its nulls named in the label scope of the
    /// resource, and is admitted by the condition like any other row.
    pub(crate) fn encode_into(
        &self,
        load: &mut ResourceLoad<'_>,
        dictionary: &mut Dictionary,
        base: u64,
    ) {
        let shifted = |line: u64| if line > 0 { line + base } else { 0 };

        // The rows are stored first, in line order, after the rows of every earlier run.
        // The counts of the parse are merged afterwards,
        // since its first malformed line may come before the line of a row that fails to encode.
        let width = load.options.formats.len();
        let mut row = std::mem::take(&mut load.row);
        let mut deferred = self.deferred.iter().enumerate().peekable();
        for (index, &line) in self.lines.iter().enumerate() {
            while let Some((number, later)) = deferred.next_if(|(_, row)| row.position <= index) {
                let cells = &self.deferred_cells[number * width..(number + 1) * width];
                load.take_deferred(cells, &self.text, dictionary, shifted(later.line));
            }

            row.clear();
            let complete = self.cells[index * self.arity..(index + 1) * self.arity]
                .iter()
                .all(|&cell| {
                    self.encode(cell, dictionary).is_some_and(|value| {
                        row.push(value);
                        true
                    })
                });
            if complete {
                load.intake.store(&row);
            } else {
                load.intake.count_malformed(shifted(line));
            }
        }

        for (number, later) in deferred {
            let cells = &self.deferred_cells[number * width..(number + 1) * width];
            load.take_deferred(cells, &self.text, dictionary, shifted(later.line));
        }

        load.row = row;
        load.intake.absorb(&self.counts, base);
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
            ParsedCell::Missing => {
                let null = dictionary.fresh_null();
                dictionary.encode(ValueRef::Null(null))
            }
        }
    }
}
