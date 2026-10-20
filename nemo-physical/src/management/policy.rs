//! Merge policies: the choice of the subtables a consolidation merges next.
//!
//! Merging trades the rows a merge rewrites
//! against the width of later reads, the number of subtables a read covers.

use std::ops::Range;

use crate::management::{Revision, subtable::Subtable};

/// The policy that chooses the subtables a consolidation merges next.
///
/// A consolidation receives walls:
/// revisions at which reads must still be able to start or end.
/// No merge crosses one, since a merged subtable is only read as a whole.
/// The walls thus divide a table into compartments,
/// the longest sequences of adjacent subtables with no wall between them,
/// and the policy plans each compartment on its own.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     management::{MergePolicy, ColumnOrder, Revision, TableId, TableManager},
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// /// Stores a batch of one row at the given revision.
/// fn add(manager: &mut TableManager, table: TableId, revision: u64) {
///     let mut rows = RowBuffer::new(1);
///     rows.push_row(&[StorageValue::term(revision)]);
///     manager.add_rows(table, Revision(revision), ColumnOrder::identity(1), rows);
/// }
///
/// // A growing table: after each batch, a read covers at most two subtables.
/// let mut manager = TableManager::new();
/// let growing = manager.register("growing", 1);
/// for revision in 1..=8 {
///     add(&mut manager, growing, revision);
///     manager.consolidate_with(growing, &[], MergePolicy::TwoLevel);
///     assert!(manager.blocks(growing).len() <= 2);
/// }
///
/// // A finished table merges whole, but never across a wall.
/// let finished = manager.register("finished", 1);
/// for revision in 1..=4 {
///     add(&mut manager, finished, revision);
/// }
/// manager.consolidate_with(finished, &[Revision(3)], MergePolicy::Whole);
/// assert_eq!(
///     manager.blocks(finished),
///     vec![Revision(1)..Revision(3), Revision(3)..Revision(5)]
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePolicy {
    /// Merges the newest subtable of a compartment with the ones before it,
    /// taking them newest first and stopping at the first one
    /// that holds more rows than the merge has gathered so far.
    Doubling,
    /// Holds each compartment to two subtables:
    /// the leader, its oldest subtable, and the suffix, which collects the newer batches.
    /// Each new batch merges into the suffix,
    /// until the rows these merges rewrote catch up with the leader's rows.
    /// Then the suffix folds into the leader, and the next batch starts a new suffix.
    ///
    /// A read covers at most two subtables per compartment,
    /// and a compartment that grows to n rows has its merges rewrite O(n·√n) rows in total.
    TwoLevel,
    /// Merges every compartment into one subtable.
    ///
    /// Suits a table that no longer grows:
    /// it pays for one merge, after which a read covers one subtable per compartment.
    Whole,
}

impl MergePolicy {
    /// Returns the range of revisions to merge next within a compartment,
    /// or `None` if the policy merges nothing there.
    ///
    /// A proposal covers at least two subtables.
    pub(super) fn propose(self, compartment: &[Subtable]) -> Option<Range<Revision>> {
        match self {
            MergePolicy::Doubling => Self::doubling(compartment),
            MergePolicy::TwoLevel => Self::two_level(compartment),
            MergePolicy::Whole => Self::whole(compartment),
        }
    }

    /// Returns the merge of a whole compartment, if it holds several subtables.
    fn whole(compartment: &[Subtable]) -> Option<Range<Revision>> {
        let [first, .., last] = compartment else {
            return None;
        };

        Some(first.range.start..last.range.end)
    }

    /// Returns the merge the doubling cascade proposes within a compartment.
    fn doubling(compartment: &[Subtable]) -> Option<Range<Revision>> {
        let (newest, older) = compartment.split_last()?;

        let mut merged = newest.row_count();
        let mut start = older.len();
        for subtable in older.iter().rev() {
            if subtable.row_count() > merged {
                break;
            }
            merged += subtable.row_count();
            start -= 1;
        }

        older
            .get(start)
            .map(|oldest| oldest.range.start..newest.range.end)
    }

    /// Returns the merge the two-level schedule proposes within a compartment.
    fn two_level(compartment: &[Subtable]) -> Option<Range<Revision>> {
        let [leader, suffix @ ..] = compartment else {
            return None;
        };
        let (Some(oldest), Some(newest)) = (suffix.first(), suffix.last()) else {
            return None;
        };

        // A suffix of several subtables owes one more merge.
        // Counting it in decides the fold before that merge happens,
        // so that the fold writes those rows once.
        let rewritten = suffix
            .iter()
            .map(|subtable| subtable.rewritten)
            .fold(0, usize::saturating_add);
        let owed = if suffix.len() >= 2 {
            suffix.iter().map(Subtable::row_count).sum()
        } else {
            0
        };

        if rewritten.saturating_add(owed) >= leader.row_count() {
            Some(leader.range.start..newest.range.end)
        } else if suffix.len() >= 2 {
            Some(oldest.range.start..newest.range.end)
        } else {
            None
        }
    }
}
