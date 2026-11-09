//! Loading: reading resources into row buffers.
//!
//! The rows read from one resource form a *load*, a [Loaded].
//! A row of a resource is *well-formed* if the reader can parse every cell that it reads of the
//! row, and *malformed* otherwise.
//! A well-formed row is *admitted* if the condition of the
//! [selection](crate::selection::RowSelection) holds for it, and only admitted rows are stored.
//! An admitted row with a value that the dictionary cannot hold is counted as malformed after
//! all.

mod batch;
mod reader;
mod resource;

pub use batch::ResourceBatch;
pub use reader::Reader;
pub use resource::{DsvResource, Resource};

use nemo_physical::{
    dictionary::Dictionary,
    function::EvaluationContext,
    storage::StorageValue,
    trie::RowBuffer,
    value::{DataValue, ErrorCounts},
};

use crate::selection::RowSelection;

/// The rows loaded from one resource, and the counts of the rows dropped.
///
/// The rows are in the resource's order, unsorted.
/// Every dropped row is counted, so that it can be reported as a warning or an error.
/// The dropped rows are counted by cause, since each cause asks the user for a different fix.
#[derive(Debug)]
pub struct Loaded {
    /// The loaded rows, encoded by the dictionary.
    pub rows: RowBuffer,
    /// The number of malformed rows, which were skipped.
    pub malformed: u64,
    /// The line of the first malformed row, if it is known.
    pub first_malformed: Option<u64>,
    /// The number of well-formed rows that the condition of the resource's
    /// [selection](crate::selection::RowSelection) excluded.
    pub excluded: u64,
    /// The number of well-formed rows on which the evaluation of the selection's condition
    /// failed, per error kind.
    ///
    /// These rows are dropped, and not counted as [excluded](Self::excluded).
    pub errors: ErrorCounts,
    /// The number of well-formed rows, admitted or not.
    ///
    /// A limit counts these rows, so the condition never makes a read with a limit go further
    /// into the resource.
    pub(crate) well_formed: u64,
    /// The evaluation context of the selection's condition, which the read was given.
    context: EvaluationContext,
}

impl Loaded {
    /// Creates an empty load of the given arity.
    ///
    /// `context` is the evaluation context of the selection's condition.
    pub(crate) fn new(arity: usize, context: EvaluationContext) -> Self {
        Self {
            rows: RowBuffer::new(arity),
            malformed: 0,
            first_malformed: None,
            excluded: 0,
            errors: ErrorCounts::default(),
            well_formed: 0,
            context,
        }
    }

    /// Counts one malformed row at `line`, which is `0` if the line is not known.
    ///
    /// Rows arrive in the resource's order,
    /// so the first known line is the line of the first malformed row.
    pub(crate) fn count_malformed(&mut self, line: u64) {
        self.malformed += 1;
        if line > 0 {
            self.first_malformed.get_or_insert(line);
        }
    }

    /// [Admits](Self::admit) and [stores](Self::store) one well-formed row,
    /// and returns whether it was stored.
    ///
    /// `kept` contains the parsed values of the delivered columns.
    /// `row` is a scratch buffer for the encoded values,
    /// and `line` is the row's line, or `0` if it is not known.
    pub(crate) fn take(
        &mut self,
        kept: &[DataValue],
        selection: &RowSelection,
        dictionary: &mut Dictionary,
        row: &mut Vec<StorageValue>,
        line: u64,
    ) -> bool {
        self.admit(selection, kept) && self.store(kept, dictionary, row, line)
    }

    /// Counts a well-formed row of delivered values, and returns whether it is admitted.
    ///
    /// A row that is not admitted is counted as [excluded](Self::excluded),
    /// or, if the evaluation of the condition failed, under the error's kind in
    /// [errors](Self::errors).
    pub(crate) fn admit(&mut self, selection: &RowSelection, kept: &[DataValue]) -> bool {
        self.well_formed += 1;
        match selection.evaluate(self.context, kept) {
            Ok(true) => true,
            Ok(false) => {
                self.excluded += 1;
                false
            }
            Err(kind) => {
                self.errors.record(kind);
                false
            }
        }
    }

    /// Interns and stores an admitted row, and returns whether it was stored.
    ///
    /// `row` is a scratch buffer for the encoded values.
    /// If the dictionary cannot hold a value, the row is counted as malformed at `line`.
    pub(crate) fn store(
        &mut self,
        kept: &[DataValue],
        dictionary: &mut Dictionary,
        row: &mut Vec<StorageValue>,
        line: u64,
    ) -> bool {
        row.clear();
        let complete = kept.iter().all(|value| {
            dictionary.encode(value.as_ref()).is_some_and(|encoded| {
                row.push(encoded);
                true
            })
        });
        if complete {
            self.rows.push_row(row);
        } else {
            self.count_malformed(line);
        }
        complete
    }

    /// Decodes every loaded row back into owned values.
    #[cfg(test)]
    pub(crate) fn decoded(&self, dictionary: &Dictionary) -> Vec<Vec<DataValue>> {
        (0..self.rows.len())
            .map(|index| {
                self.rows
                    .row(index)
                    .iter()
                    .map(|&value| {
                        dictionary
                            .decode(value)
                            .expect("loaded values decode")
                            .to_owned()
                    })
                    .collect()
            })
            .collect()
    }
}
