//! Subtables: the rows of one block, in one or more column orders.

use std::ops::Range;

use crate::{
    management::{ColumnOrder, Revision, row_revisions::RowRevisions},
    storage::StorageValue,
    trie::{SegmentedRowBuffer, Trie},
};

/// The rows of one block of a table, stored in one or more arrangements.
#[derive(Debug)]
pub(super) struct Subtable {
    /// The block, the range of revisions whose rows the subtable stores.
    pub(super) range: Range<Revision>,
    /// The arrangements of the rows, one per column order, and at least one.
    ///
    /// The first is in the order in which the table's batches arrive.
    arrangements: Vec<Arrangement>,
    /// The revision at which each row was added.
    revisions: RowRevisions,

    /// The number of rows written by the merges that produced the subtable,
    /// including the rows written for the subtables that they merged.
    ///
    /// It is kept for [TwoLevel](super::MergePolicy::TwoLevel).
    pub(super) rewritten: usize,
}

/// The rows of a subtable, arranged in one column order.
#[derive(Debug)]
struct Arrangement {
    /// The column order.
    order: ColumnOrder,
    /// The rows in that order.
    trie: Trie,
}

impl Subtable {
    /// Creates the subtable of a batch added at a revision,
    /// from its rows in the given order.
    pub(super) fn new(revision: Revision, order: ColumnOrder, trie: Trie) -> Self {
        Self {
            range: revision..Revision(revision.0 + 1),
            arrangements: vec![Arrangement { order, trie }],
            revisions: RowRevisions::Start,
            rewritten: 0,
        }
    }

    /// Merges consecutive subtables of a table into one,
    /// which stores every column order that they all store.
    ///
    /// The merged block spans from the first block's start to the last block's end.
    /// A row that several subtables contain keeps the earliest revision at which it was added.
    ///
    /// Panics if there are no subtables, or if their batches arrived in different orders.
    pub(super) fn merge(subtables: &[Subtable]) -> Self {
        let (Some(first), Some(last)) = (subtables.first(), subtables.last()) else {
            panic!("a consolidation range must contain at least one subtable");
        };
        debug_assert!(
            subtables.is_sorted_by(|before, after| before.range.end <= after.range.start),
            "each merged block ends before the next one starts"
        );

        let arrival = first.arrival_order();
        debug_assert!(
            subtables
                .iter()
                .all(|subtable| subtable.arrival_order() == arrival),
            "merged subtables arrive in one column order"
        );

        // The row revisions are merged along with the first arrangement, by row index.
        let start = first.range.start.0;
        let arrived: Vec<&Trie> = subtables
            .iter()
            .map(|subtable| &subtable.arrangements[0].trie)
            .collect();
        let (trie, revisions) =
            RowRevisions::merge(&arrived, last.range.end.0 - start, |source, row| {
                let subtable = &subtables[source];
                subtable.range.start.0 - start + subtable.revisions.distance(row)
            });
        let mut arrangements = vec![Arrangement {
            order: arrival.clone(),
            trie,
        }];

        for order in first.orders().skip(1) {
            let sources: Option<Vec<&Trie>> = subtables
                .iter()
                .map(|subtable| subtable.trie(order))
                .collect();
            if let Some(sources) = sources {
                arrangements.push(Arrangement {
                    order: order.clone(),
                    trie: Trie::merge(&sources),
                });
            }
        }
        let rows = arrangements[0].trie.row_count();
        let rewritten = subtables
            .iter()
            .map(|subtable| subtable.rewritten)
            .fold(rows, usize::saturating_add);

        Self {
            range: first.range.start..last.range.end,
            arrangements,
            revisions,
            rewritten,
        }
    }

    /// Returns the revision at which the row was added,
    /// or `None` if the subtable does not contain it.
    ///
    /// The row's columns must be in the order in which the table's batches arrive.
    pub(super) fn revision_of(&self, row: &[StorageValue]) -> Option<Revision> {
        let index = self.arrangements[0].trie.index_of(row)?;
        Some(Revision(
            self.range.start.0 + self.revisions.distance(index),
        ))
    }

    /// Returns the column order in which the subtable's batches arrived.
    pub(super) fn arrival_order(&self) -> &ColumnOrder {
        &self.arrangements[0].order
    }

    /// Returns the trie in the given order, if the subtable stores one.
    pub(super) fn trie(&self, order: &ColumnOrder) -> Option<&Trie> {
        self.arrangements
            .iter()
            .find(|arrangement| &arrangement.order == order)
            .map(|arrangement| &arrangement.trie)
    }

    /// Returns the column orders the subtable stores.
    pub(super) fn orders(&self) -> impl Iterator<Item = &ColumnOrder> {
        self.arrangements
            .iter()
            .map(|arrangement| &arrangement.order)
    }

    /// Returns the number of rows, which is the same in every arrangement.
    pub(super) fn row_count(&self) -> usize {
        self.arrangements[0].trie.row_count()
    }

    /// Returns the allocated bytes of the arrangements and the row revisions.
    pub(super) fn memory_bytes(&self) -> usize {
        self.arrangements
            .iter()
            .map(|arrangement| arrangement.trie.memory_bytes())
            .sum::<usize>()
            + self.revisions.memory_bytes()
    }

    /// Builds the arrangement in the given order from the closest stored arrangement,
    /// unless the subtable already stores it.
    ///
    /// Returns the number of rows of the built arrangement, or `None` if none was built.
    pub(super) fn ensure_order(&mut self, order: &ColumnOrder) -> Option<usize> {
        if self.trie(order).is_some() {
            return None;
        }

        let arrangement = self.closest(order).reordered(order);
        let rows = arrangement.trie.row_count();
        self.arrangements.push(arrangement);
        Some(rows)
    }

    /// Returns the stored arrangement closest to the given order.
    ///
    /// The closest arrangement shares the longest prefix with the order,
    /// and among those, it has the least order.
    /// An arrangement built from it needs sorting only within the groups of that prefix,
    /// as [reordered](Arrangement::reordered) explains.
    /// The tie-break makes the choice independent of the order in which the arrangements
    /// were built.
    fn closest(&self, target: &ColumnOrder) -> &Arrangement {
        self.arrangements
            .iter()
            .min_by_key(|&arrangement| {
                (
                    target.arity() - arrangement.order.shared_prefix(target),
                    &arrangement.order,
                )
            })
            .expect("a subtable stores at least one arrangement")
    }
}

impl Arrangement {
    /// Returns an arrangement of the rows in the target order.
    ///
    /// The rows are read in this arrangement's order,
    /// so they are already sorted by the leading columns that the two orders share.
    /// Only the rows within each group of equal shared columns need sorting.
    fn reordered(&self, target: &ColumnOrder) -> Self {
        let arity = self.trie.arity();
        if arity == 0 {
            return Self {
                order: target.clone(),
                trie: Trie::nullary(!self.trie.is_empty()),
            };
        }

        let indices = target.reorder_from(&self.order);
        let mut rows = SegmentedRowBuffer::new(arity);
        let buffer = rows.begin_segment();
        let mut reordered = vec![StorageValue::term(0); arity];

        self.trie.for_each_row(|row| {
            for (value, &position) in reordered.iter_mut().zip(&indices) {
                *value = row[position];
            }
            buffer.push_row(&reordered);
        });

        rows.sort_last_segment(self.order.shared_prefix(target));

        Self {
            order: target.clone(),
            trie: Trie::from_runs(arity, &[rows.segment(0)]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::assert_panics;

    /// Returns the subtable of an empty binary batch at the given revision.
    fn batch(revision: u64, order: &ColumnOrder) -> Subtable {
        Subtable::new(Revision(revision), order.clone(), Trie::empty(2))
    }

    /// A merge needs at least one subtable, blocks in ascending order and one arrival order,
    /// but allows gaps between the blocks.
    #[test]
    fn a_merge_needs_disjoint_blocks_in_order_and_one_arrival_order() {
        let natural = ColumnOrder::identity(2);
        let swapped = ColumnOrder::new(&[1, 0]).expect("valid");

        assert_panics("at least one subtable", || Subtable::merge(&[]));
        assert_panics("one column order", || {
            Subtable::merge(&[batch(1, &natural), batch(2, &swapped)])
        });
        // Blocks out of revision order and overlapping blocks, which only debug builds check.
        if cfg!(debug_assertions) {
            assert_panics("ends before", || {
                Subtable::merge(&[batch(2, &natural), batch(1, &natural)])
            });
            assert_panics("ends before", || {
                Subtable::merge(&[batch(1, &natural), batch(1, &natural)])
            });
        }

        // Revisions between the blocks may be missing.
        let merged = Subtable::merge(&[batch(1, &natural), batch(3, &natural)]);
        assert_eq!(merged.range, Revision(1)..Revision(4));
    }
}
