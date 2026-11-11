//! Runs of rows: parsed away from the dictionary, awaiting interning.
//!
//! A *run* is a bounded portion of the rows of a resource.
//! A worker parses the rows of a run without the dictionary,
//! and the thread that owns the dictionary interns them afterwards, run by run, in order.

use std::sync::{Mutex, PoisonError};

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    value::{DataValue, IriRef, ReadValue, ValueRef},
};

use crate::{
    load::{BlankNodes, RowCounts, RowIntake},
    selection::{ConditionBuffers, RowCondition},
};

/// The number of cells at which a run is full and is handed on for interning.
const RUN_CELLS: usize = 1 << 14;

/// The number of bytes of text at which a run is full and is handed on for interning.
const RUN_TEXT: usize = 1 << 20;

/// One parsed cell, in the form from which the thread that owns the dictionary interns it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RunCell {
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
    /// The null that a label names in the resource, with the label at this byte range of the
    /// run's text.
    Labeled {
        /// Where the label starts.
        start: u32,
        /// Where the label ends.
        end: u32,
    },
}

/// The lengths of a run's buffers before a row was added, for taking the row back.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowStart {
    /// The number of cells.
    cells: usize,
    /// The length of the text.
    text: usize,
    /// The number of owned values.
    owned: usize,
}

/// The parsed cells of a run, with the text and the owned values that they refer to.
///
/// - Values with an inline encoding are encoded as they are parsed.
/// - IRIs, strings and the labels of nulls keep their text in one buffer.
/// - Only values of other kinds are kept as owned values.
#[derive(Debug, Default)]
pub(crate) struct RunValues {
    /// The cells, row by row.
    cells: Vec<RunCell>,
    /// The text of the IRI and string cells and of the labels, and any text that a run keeps
    /// beside its cells.
    text: String,
    /// The values of the cells of other kinds.
    owned: Vec<DataValue>,
}

impl RunValues {
    /// Empties the buffers, keeping their allocations.
    pub(crate) fn clear(&mut self) {
        self.cells.clear();
        self.text.clear();
        self.owned.clear();
    }

    /// Returns the text buffer.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Appends a text that the run keeps beside its cells, and returns its byte range.
    pub(crate) fn keep_text(&mut self, text: &str) -> (usize, usize) {
        let start = self.text.len();
        self.text.push_str(text);

        (start, self.text.len())
    }

    /// Returns the size in bytes of the buffers, which a reused run keeps.
    pub(crate) fn bytes(&self) -> usize {
        self.cells.capacity() * size_of::<RunCell>()
            + self.text.capacity()
            + self.owned.capacity() * size_of::<DataValue>()
    }

    /// Returns whether the run is full and should be handed on.
    pub(crate) fn is_full(&self) -> bool {
        self.cells.len() >= RUN_CELLS || self.text.len() >= RUN_TEXT
    }

    /// Returns the current lengths of the buffers, for taking back a row that is not kept.
    pub(crate) fn row_start(&self) -> RowStart {
        RowStart {
            cells: self.cells.len(),
            text: self.text.len(),
            owned: self.owned.len(),
        }
    }

    /// Removes the cells, text and owned values added since `start`.
    pub(crate) fn take_back(&mut self, start: RowStart) {
        self.cells.truncate(start.cells);
        self.text.truncate(start.text);
        self.owned.truncate(start.owned);
    }

    /// Adds the cell of a value that was read.
    ///
    /// An IRI or a string that appears in the resource unchanged goes into the text
    /// without becoming an owned value.
    pub(crate) fn push_read(&mut self, value: ReadValue<'_>) {
        match value {
            ReadValue::Iri(iri) => self.push_iri(iri),
            ReadValue::String(text) => self.push_string(text),
            ReadValue::Value(value) => self.push_value(value),
        }
    }

    /// Adds the cell of an IRI with the given text.
    pub(crate) fn push_iri(&mut self, iri: &str) {
        let cell = self
            .text_cell([iri, ""], true)
            .unwrap_or_else(|| self.owned_cell(DataValue::iri(iri)));
        self.cells.push(cell);
    }

    /// Adds the cell of a plain string with the given text.
    pub(crate) fn push_string(&mut self, text: &str) {
        let cell = self
            .text_cell([text, ""], false)
            .unwrap_or_else(|| self.owned_cell(DataValue::string(text)));
        self.cells.push(cell);
    }

    /// Adds the cell of an owned value.
    ///
    /// The value is encoded if it has an inline encoding.
    /// Otherwise, an IRI or a string keeps its text in the run's text,
    /// and a value of another kind is kept as an owned value.
    pub(crate) fn push_value(&mut self, value: DataValue) {
        let cell = match StorageValue::from_value(value.as_ref()) {
            Some(inline) => RunCell::Inline(inline),
            None => {
                let text = match value.as_ref() {
                    ValueRef::Iri(iri) => self.text_cell([iri.namespace(), iri.local()], true),
                    ValueRef::String(text) => self.text_cell([text, ""], false),
                    _ => None,
                };

                text.unwrap_or_else(|| self.owned_cell(value))
            }
        };
        self.cells.push(cell);
    }

    /// Adds the cell of a missing value that its column admits.
    pub(crate) fn push_missing(&mut self) {
        self.cells.push(RunCell::Missing);
    }

    /// Adds the cell of the null that a label names in the resource.
    pub(crate) fn push_labeled(&mut self, label: &str) {
        let (start, end) = self.keep_text(label);
        let cell = match (u32::try_from(start), u32::try_from(end)) {
            (Ok(start), Ok(end)) => RunCell::Labeled { start, end },
            // A label beyond the range of the offsets is the label of no other null.
            _ => RunCell::Missing,
        };
        self.cells.push(cell);
    }

    /// Appends a text, given in two parts, to the run's text,
    /// and returns its cell, of an IRI if `iri` is true and of a string otherwise.
    ///
    /// Returns `None`, and appends nothing, if the text's offsets would not fit in 32 bits.
    fn text_cell(&mut self, parts: [&str; 2], iri: bool) -> Option<RunCell> {
        let start = u32::try_from(self.text.len()).ok()?;
        let end = u32::try_from(self.text.len() + parts[0].len() + parts[1].len()).ok()?;
        self.text.push_str(parts[0]);
        self.text.push_str(parts[1]);

        Some(if iri {
            RunCell::Iri { start, end }
        } else {
            RunCell::String { start, end }
        })
    }

    /// Keeps a value as an owned value and returns its cell.
    fn owned_cell(&mut self, value: DataValue) -> RunCell {
        let index = u32::try_from(self.owned.len()).expect("a run holds fewer than 2³² values");
        self.owned.push(value);

        RunCell::Owned(index)
    }

    /// Returns the value of a cell, or `None` for a cell that holds a null,
    /// which has its number only once it is interned.
    fn value(&self, cell: RunCell) -> Option<ValueRef<'_>> {
        let text = |start: u32, end: u32| &self.text[start as usize..end as usize];
        match cell {
            RunCell::Inline(value) => Some(value.to_value().expect("an inline value decodes")),
            RunCell::Iri { start, end } => Some(ValueRef::Iri(IriRef::new(text(start, end)))),
            RunCell::String { start, end } => Some(ValueRef::String(text(start, end))),
            RunCell::Owned(index) => Some(self.owned[index as usize].as_ref()),
            RunCell::Missing | RunCell::Labeled { .. } => None,
        }
    }

    /// Encodes one cell, or returns `None` if the dictionary cannot hold its value.
    ///
    /// `blanks` are the nulls that the resource names.
    /// If there are none yet, a labeled null opens a label scope of the resource's own.
    pub(crate) fn encode(
        &self,
        cell: RunCell,
        dictionary: &mut Dictionary,
        blanks: &mut Option<BlankNodes>,
    ) -> Option<StorageValue> {
        let text = |start: u32, end: u32| &self.text[start as usize..end as usize];
        match cell {
            RunCell::Inline(value) => Some(value),
            RunCell::Iri { start, end } => {
                dictionary.encode(ValueRef::Iri(IriRef::new(text(start, end))))
            }
            RunCell::String { start, end } => dictionary.encode(ValueRef::String(text(start, end))),
            RunCell::Owned(index) => dictionary.encode(self.owned[index as usize].as_ref()),
            RunCell::Missing => {
                let null = dictionary.fresh_null();
                dictionary.encode(ValueRef::Null(null))
            }
            RunCell::Labeled { start, end } => {
                let blanks =
                    blanks.get_or_insert_with(|| BlankNodes::open(None, "document", dictionary));
                let null = blanks.null(text(start, end), dictionary);
                dictionary.encode(null.as_ref())
            }
        }
    }
}

/// Returns a line of a part of a resource as a line of the resource.
///
/// `base` is the number of lines before the part.
/// A line that is not known stays `0`.
pub(crate) fn line_in_resource(line: u64, base: u64) -> u64 {
    if line > 0 { line + base } else { 0 }
}

/// A run of the rows of a resource, parsed without a dictionary and awaiting interning.
///
/// A parse of a resource, or of one window of it, hands its rows on in runs of bounded size,
/// and the runs are interned in the order they were parsed.
///
/// A row is added cell by cell to the run's [values](Self::values),
/// and [end_row](Self::end_row) keeps it if the resource's condition admits it.
/// A row that holds a null is kept without asking the condition:
/// its nulls have their numbers only once the row is [interned](Self::intern_row),
/// so the condition is evaluated then.
#[derive(Debug)]
pub(crate) struct Run {
    /// The number of delivered columns.
    arity: usize,
    /// The run's counts of well-formed and dropped rows.
    pub(crate) counts: RowCounts,
    /// The cells of the kept rows, row by row, with as many cells per row as the arity.
    pub(crate) values: RunValues,
    /// The line in the resource of each kept row, or `0` where it is not known.
    ///
    /// This list also counts the rows, which the cells alone cannot do for arity zero.
    lines: Vec<u64>,
    /// The buffers of the condition's evaluations.
    buffers: ConditionBuffers,
    /// The number of lines that the parse consumed, set on its last run only.
    ///
    /// It is the line number at which the text after the parsed part begins, less one.
    pub(crate) consumed_lines: u64,
}

impl Run {
    /// Creates an empty run for rows of the given arity.
    pub(crate) fn new(arity: usize) -> Self {
        Self {
            arity,
            counts: RowCounts::default(),
            values: RunValues::default(),
            lines: Vec::new(),
            buffers: ConditionBuffers::default(),
            consumed_lines: 0,
        }
    }

    /// Returns an empty run for rows of the given arity: a spare run of the pool,
    /// or a new run if the pool has none.
    pub(crate) fn from_pool(runs: &RunPool<Self>, arity: usize) -> Self {
        match runs.take() {
            Some(mut run) => {
                run.reset(arity);

                run
            }
            None => Self::new(arity),
        }
    }

    /// Empties the run for rows of the given arity, keeping its buffers.
    pub(crate) fn reset(&mut self, arity: usize) {
        self.arity = arity;
        self.counts = RowCounts::default();
        self.values.clear();
        self.lines.clear();
        self.consumed_lines = 0;
    }

    /// Returns the number of rows that the run keeps.
    pub(crate) fn len(&self) -> usize {
        self.lines.len()
    }

    /// Returns the size in bytes of the run's buffers, which a reused run keeps.
    pub(crate) fn bytes(&self) -> usize {
        self.values.bytes() + self.lines.capacity() * size_of::<u64>()
    }

    /// Returns whether the run is full and should be handed on.
    pub(crate) fn is_full(&self) -> bool {
        self.values.is_full()
    }

    /// Ends a well-formed row at `line`, whose cells were added to the values from `start` on,
    /// and returns whether the run keeps it.
    ///
    /// The row is counted as well-formed.
    /// If `condition` does not admit it, its cells are taken back, and it is counted as
    /// excluded, or under the kind of the error that the condition raised on it.
    /// A row that holds a null is kept, and the condition is evaluated when it is interned.
    /// `context` is the evaluation context of the condition.
    pub(crate) fn end_row(
        &mut self,
        start: RowStart,
        condition: Option<&RowCondition>,
        context: EvaluationContext,
        line: u64,
    ) -> bool {
        self.counts.well_formed += 1;

        let row = &self.values.cells[start.cells..];
        let admitted = match condition {
            Some(condition) if !holds_null(row) => {
                let values = &self.values;
                let result = condition.evaluate_with(context, &mut self.buffers, |position| {
                    values.value(row[position]).expect("the row holds no null")
                });

                self.counts.count_condition(result)
            }
            _ => true,
        };

        if admitted {
            self.lines.push(line);
        } else {
            self.values.take_back(start);
        }

        admitted
    }

    /// Interns the kept row at `index` into the intake of its resource.
    ///
    /// - A row with a value that the dictionary cannot hold is counted as malformed.
    /// - A row that holds a null is admitted here by the intake's condition,
    ///   and counted by the intake if the condition does not admit it.
    ///
    /// `blanks` are the nulls that the resource names,
    /// `encoded` is a scratch buffer for the encoded row,
    /// and `base` is the number of lines before the run's part of the resource.
    pub(crate) fn intern_row(
        &self,
        index: usize,
        intake: &mut RowIntake,
        blanks: &mut Option<BlankNodes>,
        dictionary: &mut Dictionary,
        encoded: &mut Vec<StorageValue>,
        base: u64,
    ) {
        let row = &self.values.cells[index * self.arity..(index + 1) * self.arity];

        encoded.clear();
        let complete = row.iter().all(|&cell| {
            self.values
                .encode(cell, dictionary, blanks)
                .is_some_and(|value| {
                    encoded.push(value);
                    true
                })
        });
        if !complete {
            intake.count_malformed(line_in_resource(self.lines[index], base));
            return;
        }

        if intake.selection().condition.is_some() && holds_null(row) {
            let dictionary = &*dictionary;
            let admitted = intake.admits(|position| {
                self.values.value(row[position]).unwrap_or_else(|| {
                    dictionary
                        .decode(encoded[position])
                        .expect("the null was interned")
                })
            });
            if !admitted {
                return;
            }
        }

        intake.store(encoded);
    }

    /// Interns the run's rows into the intake of their resource, as
    /// [intern_row](Self::intern_row) does, and adds the run's counts to the intake's.
    pub(crate) fn intern(
        &self,
        intake: &mut RowIntake,
        blanks: &mut Option<BlankNodes>,
        dictionary: &mut Dictionary,
        encoded: &mut Vec<StorageValue>,
        base: u64,
    ) {
        for index in 0..self.len() {
            self.intern_row(index, intake, blanks, dictionary, encoded, base);
        }

        intake.absorb(&self.counts, base);
    }
}

/// Returns whether a row holds a null.
fn holds_null(row: &[RunCell]) -> bool {
    row.iter()
        .any(|cell| matches!(cell, RunCell::Missing | RunCell::Labeled { .. }))
}

/// A pool of runs that were interned and wait to be filled again.
///
/// Runs are filled on one thread and interned on another.
/// Handing each interned run back to the pool keeps its buffers,
/// so after the first few runs a parse allocates nothing per run.
/// This also keeps the heap from fragmenting
/// into thousands of buffers allocated on one thread and freed on another.
#[derive(Debug)]
pub(crate) struct RunPool<R> {
    /// The runs waiting to be reused.
    spare: Mutex<Vec<R>>,
}

impl<R> Default for RunPool<R> {
    fn default() -> Self {
        Self {
            spare: Mutex::new(Vec::new()),
        }
    }
}

impl<R> RunPool<R> {
    /// Returns a spare run to fill again, if there is one.
    pub(crate) fn take(&self) -> Option<R> {
        self.spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
    }

    /// Keeps an interned run for reuse.
    pub(crate) fn give(&self, run: R) {
        self.spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(run);
    }
}
