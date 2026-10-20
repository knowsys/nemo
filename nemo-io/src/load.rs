//! Staged loading: resources into row buffers.

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

/// The rows loaded from one resource.
///
/// Rows arrive in the resource's order, unsorted, and storing them, or building a trie,
/// sorts them.
/// Rows the reader dropped were counted, never silently lost,
/// so that they can be reported as a warning or an error.
/// They are counted by cause, since each cause asks the user for a different fix.
#[derive(Debug)]
pub struct Loaded {
    /// The loaded rows, dictionary-encoded.
    pub rows: RowBuffer,
    /// Number of malformed rows skipped.
    pub malformed: u64,
    /// The line of the first malformed row, if it is known.
    pub first_malformed: Option<u64>,
    /// Number of well-formed rows that the resource's
    /// [selection](crate::selection::RowSelection) excluded.
    pub excluded: u64,
    /// The number of well-formed rows on which the selection's condition errored,
    /// per error kind.
    ///
    /// They are dropped, and counted like the evaluation errors of any other condition.
    pub errors: ErrorCounts,
    /// The number of well-formed rows, admitted or not.
    ///
    /// This is what a limit counts, so a condition never changes which rows a limited resource
    /// yields.
    pub(crate) well_formed: u64,
    /// The context the selection's condition evaluates in, the caller's.
    context: EvaluationContext,
}

impl Loaded {
    /// Creates an empty load of the given arity whose condition evaluates in `context`.
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

    /// Counts one malformed row, reported at `line`, which is `0` if it is not known.
    ///
    /// Rows arrive in the resource's order, so the first line reported is the first malformed one.
    pub(crate) fn count_malformed(&mut self, line: u64) {
        self.malformed += 1;
        if line > 0 {
            self.first_malformed.get_or_insert(line);
        }
    }

    /// Takes one well-formed row, the tail of every reader's loop, and returns whether it was
    /// stored.
    ///
    /// The values the selection keeps, parsed, are [admitted](Self::admit)
    /// and then [stored](Self::store).
    /// `row` is scratch for the encoded values,
    /// and `line` is the line a malformed row is reported at, `0` if it is not known.
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

    /// Returns whether a well-formed row of delivered values is admitted,
    /// counting it if it is not: excluded by the condition, or errored under the error's kind.
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

    /// Interns an admitted row and stores it, and returns whether it was stored.
    ///
    /// `row` is scratch for the encoded values.
    /// A value the dictionary cannot hold makes the row malformed at `line`.
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
