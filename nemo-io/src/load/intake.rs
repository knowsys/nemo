//! The intake of the rows of one resource into a load.

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    trie::RowBuffer,
    value::{DataValue, ValueRef},
};

use crate::{
    load::{Loaded, RowCounts},
    selection::{ConditionBuffers, RowSelection},
};

/// The intake of the rows of one resource, which builds the resource's [load](Loaded).
///
/// A reader decides which rows are well-formed.
/// It passes each well-formed row to the intake,
/// which evaluates the condition of the selection on the row, interns and stores the row if
/// the condition admits it, and counts it otherwise.
/// So the condition and the counts mean the same in every format.
#[derive(Debug)]
pub(crate) struct RowIntake {
    /// The selection of the read.
    selection: RowSelection,
    /// The evaluation context of the selection's condition.
    context: EvaluationContext,
    /// The load built so far.
    loaded: Loaded,
    /// A scratch buffer for the encoded values of a row.
    row: Vec<StorageValue>,
    /// The buffers of the condition's evaluations.
    buffers: ConditionBuffers,
}

impl RowIntake {
    /// Creates the intake of a resource with `width` columns, of which `selection` delivers
    /// some.
    ///
    /// `context` is the evaluation context of the selection's condition,
    /// such as the instant that its `NOW()` reads.
    pub(crate) fn new(width: usize, selection: &RowSelection, context: EvaluationContext) -> Self {
        Self::of_arity(selection.arity(width), selection.clone(), context)
    }

    /// Creates the intake of rows with `arity` delivered columns,
    /// which the condition of `selection` admits.
    pub(crate) fn of_arity(
        arity: usize,
        selection: RowSelection,
        context: EvaluationContext,
    ) -> Self {
        Self {
            selection,
            context,
            loaded: Loaded {
                rows: RowBuffer::new(arity),
                counts: RowCounts::default(),
            },
            row: Vec::with_capacity(arity),
            buffers: ConditionBuffers::default(),
        }
    }

    /// Returns the selection of the read.
    pub(crate) fn selection(&self) -> &RowSelection {
        &self.selection
    }

    /// Returns the number of well-formed rows taken so far, admitted or not.
    pub(crate) fn well_formed(&self) -> u64 {
        self.loaded.counts.well_formed
    }

    /// Counts one malformed row at `line`, which is `0` if the line is not known.
    pub(crate) fn count_malformed(&mut self, line: u64) {
        self.loaded.counts.count_malformed(line);
    }

    /// Counts one row with a missing value that its column's format does not admit,
    /// at `line`, which is `0` if the line is not known.
    pub(crate) fn count_missing(&mut self, line: u64) {
        self.loaded.counts.count_missing(line);
    }

    /// Takes one well-formed row, and returns whether it was stored.
    ///
    /// `kept` contains the values of the delivered columns,
    /// and `line` is the row's line, or `0` if it is not known.
    /// A row that the condition does not admit is counted as excluded,
    /// or under the kind of the error that the condition raised on it.
    /// An admitted row with a value that the dictionary cannot hold is counted as malformed.
    pub(crate) fn take(
        &mut self,
        kept: &[DataValue],
        dictionary: &mut Dictionary,
        line: u64,
    ) -> bool {
        self.loaded.counts.well_formed += 1;
        if !self.admits(|position| kept[position].as_ref()) {
            return false;
        }

        self.row.clear();
        let complete = kept.iter().all(|value| {
            dictionary.encode(value.as_ref()).is_some_and(|encoded| {
                self.row.push(encoded);
                true
            })
        });
        if complete {
            self.loaded.rows.push_row(&self.row);
        } else {
            self.loaded.counts.count_malformed(line);
        }

        complete
    }

    /// Returns whether the selection's condition admits a row.
    ///
    /// `value` returns the row's value at a delivered column.
    /// A row that the condition does not admit is counted as excluded,
    /// or under the kind of the error that the condition raised on it.
    pub(crate) fn admits<'a>(&'a mut self, value: impl Fn(usize) -> ValueRef<'a>) -> bool {
        let Some(condition) = &self.selection.condition else {
            return true;
        };

        let result = condition.evaluate_with(self.context, &mut self.buffers, value);

        self.loaded.counts.count_condition(result)
    }

    /// Stores a row that is admitted and encoded already.
    pub(crate) fn store(&mut self, row: &[StorageValue]) {
        self.loaded.rows.push_row(row);
    }

    /// Adds the counts of a part of the resource that was counted apart,
    /// as [RowCounts::absorb] does.
    pub(crate) fn absorb(&mut self, part: &RowCounts, base: u64) {
        self.loaded.counts.absorb(part, base);
    }

    /// Ends the intake and returns the load.
    pub(crate) fn finish(self) -> Loaded {
        self.loaded
    }
}
