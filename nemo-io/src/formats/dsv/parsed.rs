//! Runs of parsed rows awaiting interning.

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    value::{DataValue, IriRef, ValueRef},
};

use crate::{
    format::{CellValue, ValueFormat},
    formats::dsv::DsvOptions,
    load::Loaded,
    selection::RowSelection,
};

/// The most cells a run holds before it is handed on.
const RUN_CELLS: usize = 1 << 14;

/// The most bytes of text a run holds before it is handed on.
const RUN_TEXT: usize = 1 << 20;

/// One parsed cell, in the form the thread owning the dictionary interns it from.
#[derive(Debug, Clone, Copy)]
enum Cell {
    /// A value with an inline encoding, encoded already.
    Inline(StorageValue),
    /// An IRI, its text at this byte range of the run's text.
    Iri {
        /// Where the text starts.
        start: u32,
        /// Where the text ends.
        end: u32,
    },
    /// A plain string, its text at this byte range of the run's text.
    String {
        /// Where the text starts.
        start: u32,
        /// Where the text ends.
        end: u32,
    },
    /// Any other value, at this index of the run's owned values.
    Owned(u32),
}

/// Where a run stood before a row was added, to take the row back.
#[derive(Debug, Clone, Copy)]
struct RowStart {
    /// The number of cells.
    cells: usize,
    /// The length of the text.
    text: usize,
    /// The number of owned values.
    owned: usize,
}

/// A run of one window's rows, parsed without a dictionary and awaiting interning.
///
/// A window's parse hands its rows on in runs of bounded size,
/// and the runs are [interned](Self::encode_into) in the order they were parsed.
/// Values with an inline encoding are encoded as they are parsed,
/// IRIs and strings keep their text in one buffer of the run,
/// and only values of other kinds are held as owned values.
#[derive(Debug)]
pub(crate) struct Parsed {
    /// The run's counts of dropped rows.
    ///
    /// Its rows stay empty, since interning moves the run's rows into the load it joins.
    loaded: Loaded,
    /// The cells of the admitted rows, row-major, the load's arity per row.
    cells: Vec<Cell>,
    /// The text of the IRI and string cells.
    text: String,
    /// The values of the cells of other kinds.
    owned: Vec<DataValue>,
    /// The resource line of each admitted row.
    ///
    /// These also count the rows, which the cells alone cannot for all-skip (nullary) resources.
    lines: Vec<u64>,
    /// The values of the record being admitted, if a selection judges them.
    values: Vec<DataValue>,
    /// The lines the window consumed up to the end of this run,
    /// set on the window's last run only.
    ///
    /// A following window of the same file counts its lines from here.
    pub(crate) consumed_lines: u64,
}

impl Parsed {
    /// Creates an empty run of the given arity, its conditions evaluated in `context`.
    pub(crate) fn new(arity: usize, context: EvaluationContext) -> Self {
        Self {
            loaded: Loaded::new(arity, context),
            cells: Vec::new(),
            text: String::new(),
            owned: Vec::new(),
            lines: Vec::new(),
            values: Vec::new(),
            consumed_lines: 0,
        }
    }

    /// Empties the run for rows of the given arity, keeping its buffers.
    pub(crate) fn reset(&mut self, arity: usize, context: EvaluationContext) {
        self.loaded = Loaded::new(arity, context);
        self.cells.clear();
        self.text.clear();
        self.owned.clear();
        self.lines.clear();
        self.consumed_lines = 0;
    }

    /// Returns the well-formed rows the run has taken, admitted or not.
    pub(crate) fn well_formed(&self) -> u64 {
        self.loaded.well_formed
    }

    /// Returns the bytes the run's buffers hold, which a reused run keeps.
    pub(crate) fn bytes(&self) -> usize {
        self.cells.capacity() * size_of::<Cell>()
            + self.text.capacity()
            + self.owned.capacity() * size_of::<DataValue>()
            + self.lines.capacity() * size_of::<u64>()
    }

    /// Returns whether the run is full and should be handed on.
    pub(crate) fn is_full(&self) -> bool {
        self.cells.len() >= RUN_CELLS || self.text.len() >= RUN_TEXT
    }

    /// Adds one record of a resource read with the given options, the
    /// rows of the selection admitted, or counts it as malformed or
    /// dropped.
    pub(crate) fn push_record(
        &mut self,
        options: &DsvOptions,
        selection: &RowSelection,
        record: &csv::StringRecord,
        line: u64,
    ) {
        if options.filter.is_none() {
            self.push_direct(options, record, line);
        } else {
            self.push_selected(options, selection, record, line);
        }
    }

    /// Adds a record no selection judges: its cells go into the run as
    /// they parse, without an owned value first.
    fn push_direct(&mut self, options: &DsvOptions, record: &csv::StringRecord, line: u64) {
        let start = self.row_start();
        let complete =
            record.len() == options.formats.len()
                && record.iter().zip(&options.formats).enumerate().all(
                    |(column, (cell, &format))| {
                        if options.checks_only(column) {
                            format.parse_cell(cell).is_some()
                        } else {
                            self.push_cell(format, cell)
                        }
                    },
                );
        if complete {
            self.loaded.well_formed += 1;
            self.lines.push(line);
        } else {
            self.take_back(start);
            self.loaded.count_malformed(line);
        }
    }

    /// Adds a record the selection judges by its values, if it admits them.
    fn push_selected(
        &mut self,
        options: &DsvOptions,
        selection: &RowSelection,
        record: &csv::StringRecord,
        line: u64,
    ) {
        let mut values = std::mem::take(&mut self.values);
        values.clear();
        if !options.parse_row(record, &mut values) {
            self.loaded.count_malformed(line);
        } else if self.loaded.admit(selection, &values) {
            for value in values.drain(..) {
                self.push_value(value);
            }
            self.lines.push(line);
        }
        self.values = values;
    }

    /// Returns the current ends of the run's buffers, for taking back a row that turns out
    /// malformed.
    fn row_start(&self) -> RowStart {
        RowStart {
            cells: self.cells.len(),
            text: self.text.len(),
            owned: self.owned.len(),
        }
    }

    /// Takes back the cells added since `start`.
    fn take_back(&mut self, start: RowStart) {
        self.cells.truncate(start.cells);
        self.text.truncate(start.text);
        self.owned.truncate(start.owned);
    }

    /// Parses one cell by its format and adds it,
    /// or returns `false` if the format rejects the cell.
    ///
    /// An IRI or a string the cell holds as it is goes into the run's text without an owned value.
    /// A skipped cell adds nothing.
    fn push_cell(&mut self, format: ValueFormat, cell: &str) -> bool {
        if format == ValueFormat::Skip {
            return true;
        }
        match format.parse_cell(cell) {
            Some(CellValue::Iri(iri)) => self.push_text(iri, true),
            Some(CellValue::String(text)) => self.push_text(text, false),
            Some(CellValue::Value(value)) => self.push_value(value),
            None => return false,
        }
        true
    }

    /// Adds a parsed value: encoded if it has an inline encoding,
    /// its text if it is an IRI or a string, and owned otherwise.
    fn push_value(&mut self, value: DataValue) {
        let cell = match StorageValue::from_value(value.as_ref()) {
            Some(inline) => Some(Cell::Inline(inline)),
            None => match value.as_ref() {
                ValueRef::Iri(iri) => self.text_cell([iri.namespace(), iri.local()], true),
                ValueRef::String(text) => self.text_cell([text, ""], false),
                _ => None,
            },
        };
        let cell = cell.unwrap_or_else(|| self.owned_cell(value));
        self.cells.push(cell);
    }

    /// Adds a cell whose text is its value, an IRI's or a string's.
    fn push_text(&mut self, text: &str, iri: bool) {
        let cell = match self.text_cell([text, ""], iri) {
            Some(cell) => cell,
            None if iri => self.owned_cell(DataValue::iri(text)),
            None => self.owned_cell(DataValue::string(text)),
        };
        self.cells.push(cell);
    }

    /// Appends the text, given in parts, and returns its cell,
    /// or `None` if the run's text would outgrow the cell's offsets.
    fn text_cell(&mut self, parts: [&str; 2], iri: bool) -> Option<Cell> {
        let start = u32::try_from(self.text.len()).ok()?;
        let end = u32::try_from(self.text.len() + parts[0].len() + parts[1].len()).ok()?;
        self.text.push_str(parts[0]);
        self.text.push_str(parts[1]);
        Some(if iri {
            Cell::Iri { start, end }
        } else {
            Cell::String { start, end }
        })
    }

    /// Keeps a value as it is and returns its cell.
    fn owned_cell(&mut self, value: DataValue) -> Cell {
        let index = u32::try_from(self.owned.len()).expect("a run holds fewer than 2³² values");
        self.owned.push(value);
        Cell::Owned(index)
    }

    /// Interns the run's rows into a running load,
    /// the line numbers shifted by the lines earlier windows of the same file consumed.
    /// A row with a value the dictionary cannot hold is malformed.
    pub(crate) fn encode_into(&self, loaded: &mut Loaded, dictionary: &mut Dictionary, base: u64) {
        let shifted = |line: u64| if line > 0 { line + base } else { 0 };
        // The rows are stored first, in line order, after every earlier
        // run's lines; the parse's first malformed line is merged after,
        // since it may precede an encoding failure's.
        let arity = self.loaded.rows.arity();
        let mut row = Vec::with_capacity(arity);
        for (index, &line) in self.lines.iter().enumerate() {
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

    /// Encodes one cell, or `None` if the dictionary cannot hold its value.
    fn encode(&self, cell: Cell, dictionary: &mut Dictionary) -> Option<StorageValue> {
        let text = |start: u32, end: u32| &self.text[start as usize..end as usize];
        match cell {
            Cell::Inline(value) => Some(value),
            Cell::Iri { start, end } => {
                dictionary.encode(ValueRef::Iri(IriRef::new(text(start, end))))
            }
            Cell::String { start, end } => dictionary.encode(ValueRef::String(text(start, end))),
            Cell::Owned(index) => dictionary.encode(self.owned[index as usize].as_ref()),
        }
    }
}
