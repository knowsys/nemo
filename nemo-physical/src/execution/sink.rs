//! The row sinks: consumers of a join's rows.
//!
//! A join drains its rows into a sink,
//! handing each over with the shallowest layer that changed since the previous row.
//! A group is a run of rows that agree on a number of leading layers,
//! so a change at one of those layers starts a new group.

mod aggregating;
mod evaluating;
mod projecting;
mod row_set;

pub(crate) use aggregating::Aggregator;
pub(crate) use evaluating::{PendingRows, RowEvaluator};
pub(crate) use projecting::{ProjectingSink, ProjectionScratch};
pub(crate) use row_set::RowSet;

use crate::{storage::StorageValue, trie::RowBuffer};

/// A consumer of a join's rows.
///
/// The drain loop is monomorphized over the sink,
/// so a sink costs only the work it does per row.
pub(crate) trait RowSink {
    /// Accepts one row, with the shallowest layer whose value differs from the previous row;
    /// 0 for the first row.
    fn accept(&mut self, row: &[StorageValue], changed: usize);
}

/// A buffer appends every row it accepts.
impl RowSink for RowBuffer {
    #[inline]
    fn accept(&mut self, row: &[StorageValue], _changed: usize) {
        self.push_row(row);
    }
}
