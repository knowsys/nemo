//! The counts of the rows that a load dropped.

use nemo_physical::value::{ErrorCounts, EvaluationError};

/// The counts of the rows of one resource that a load did not store, by cause.
///
/// Every such row is counted, so that it can be reported as a warning or an error.
/// The rows are counted by cause, since each cause asks the user for a different fix.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowCounts {
    /// The number of malformed rows.
    pub malformed: u64,
    /// The line of the first malformed row, if it is known.
    pub first_malformed: Option<u64>,
    /// The number of rows with a missing value in a column whose format does not admit one.
    ///
    /// Such a row is not counted as [malformed](Self::malformed),
    /// unless another of its cells makes it malformed.
    pub missing: u64,
    /// The line of the first row with such a missing value, if it is known.
    pub first_missing: Option<u64>,
    /// The number of well-formed rows that the condition of the resource's
    /// [selection](crate::selection::RowSelection) excluded.
    pub excluded: u64,
    /// The number of well-formed rows on which the evaluation of the selection's condition
    /// failed, per error kind.
    ///
    /// These rows are not counted as [excluded](Self::excluded).
    pub errors: ErrorCounts,
    /// The number of well-formed rows, admitted or not.
    ///
    /// A limit counts these rows, so the condition never makes a read with a limit go further
    /// into the resource.
    pub(crate) well_formed: u64,
}

impl RowCounts {
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

    /// Counts one row with a missing value that its column's format does not admit,
    /// at `line`, which is `0` if the line is not known.
    pub(crate) fn count_missing(&mut self, line: u64) {
        self.missing += 1;
        if line > 0 {
            self.first_missing.get_or_insert(line);
        }
    }

    /// Counts a well-formed row by the result of the condition on it,
    /// and returns whether the condition admits the row.
    ///
    /// A row that the condition does not admit is counted as excluded,
    /// or under the kind of the error that the condition raised on it.
    pub(crate) fn count_condition(&mut self, result: Result<bool, EvaluationError>) -> bool {
        match result {
            Ok(true) => return true,
            Ok(false) => self.excluded += 1,
            Err(kind) => self.errors.record(kind),
        }

        false
    }

    /// Adds the counts of a part of the same resource.
    ///
    /// The part numbers its lines from 1, and `base` is the number of lines before the part,
    /// which is added to its line numbers.
    pub(crate) fn absorb(&mut self, part: &RowCounts, base: u64) {
        let earlier = |known: Option<u64>, line: Option<u64>| {
            let line = line.filter(|&line| line > 0).map(|line| line + base);

            match (known, line) {
                (Some(known), Some(line)) => Some(known.min(line)),
                (known, line) => known.or(line),
            }
        };

        self.malformed += part.malformed;
        self.first_malformed = earlier(self.first_malformed, part.first_malformed);
        self.missing += part.missing;
        self.first_missing = earlier(self.first_missing, part.first_missing);
        self.excluded += part.excluded;
        self.errors.absorb(&part.errors);
        self.well_formed += part.well_formed;
    }
}
