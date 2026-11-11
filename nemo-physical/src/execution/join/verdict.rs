//! The verdict of a search on one row.

use crate::storage::StorageValue;

/// The verdict of a search on a row that it is offered.
///
/// A search, such as [find_row](crate::plan::OperationPlan::find_row),
/// offers rows until one is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The row is accepted, and the search ends with it.
    Accept,
    /// The row is rejected, and with it every row that has the same values
    /// in its first `prefix` columns.
    ///
    /// With the number of the row's columns as the prefix, only the row itself is rejected,
    /// and with 0, every row, which ends the search.
    /// The search passes over the rejected rows that follow the row in storage order.
    Reject {
        /// The number of leading columns that the rejected rows share with the row.
        prefix: usize,
    },
}

impl Verdict {
    /// Returns the verdict that accepts a row if `accepted`, and rejects that row alone otherwise.
    pub fn for_row(row: &[StorageValue], accepted: bool) -> Self {
        if accepted {
            Self::Accept
        } else {
            Self::Reject { prefix: row.len() }
        }
    }
}
