//! Tables: the rows of one table, held by subtables, each in one or more arrangements.

use std::ops::Range;

use crate::{
    management::{ArrangementWork, ColumnOrder, MergePolicy, Revision, subtable::Subtable},
    trie::Trie,
};

/// Identifies a table of a [TableManager](super::TableManager).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(u32);

impl TableId {
    /// Creates the id of the table at the given index.
    pub(super) fn new(index: usize) -> Self {
        Self(u32::try_from(index).expect("fewer than 2^32 tables are registered"))
    }

    /// Returns the index of the table.
    pub(super) fn index(self) -> usize {
        self.0 as usize
    }
}

/// A table: its rows, held by subtables in ascending order of their blocks.
#[derive(Debug)]
pub(super) struct Table {
    /// The table's name, for reports and diagnostics.
    pub(super) name: String,
    /// The number of columns.
    pub(super) arity: usize,
    /// The subtables, whose blocks are disjoint and ascend.
    subtables: Vec<Subtable>,
}

impl Table {
    /// Creates a table without rows.
    pub(super) fn new(name: String, arity: usize) -> Self {
        Self {
            name,
            arity,
            subtables: Vec::new(),
        }
    }

    /// Stores a trie, its columns laid out in `order`, as a new subtable at the given revision,
    /// and returns the number of rows stored.
    ///
    /// An empty trie stores nothing.
    /// Panics if the order or the trie does not match the table's arity,
    /// or if a nonempty trie's revision does not follow the blocks of the table's subtables.
    pub(super) fn add(&mut self, revision: Revision, order: ColumnOrder, trie: Trie) -> usize {
        assert!(
            order.arity() == self.arity && trie.arity() == self.arity,
            "an added trie must match the table's arity"
        );
        if trie.is_empty() {
            return 0;
        }
        if let Some(last) = self.subtables.last() {
            assert!(
                revision >= last.range.end,
                "subtables must be added in ascending revision order"
            );
        }

        let rows = trie.row_count();
        self.subtables.push(Subtable::new(revision, order, trie));

        rows
    }

    /// Builds the arrangements in the given column order
    /// that the subtables within a range lack, and returns the work that took.
    ///
    /// Panics if the order does not match the table's arity,
    /// or if the range does not align with the table's blocks.
    pub(super) fn ensure_order(
        &mut self,
        range: Range<Revision>,
        order: &ColumnOrder,
    ) -> ArrangementWork {
        assert_eq!(
            order.arity(),
            self.arity,
            "a requested order must match the table's arity"
        );

        let within = self.within(&range);

        let mut work = ArrangementWork::default();
        for subtable in &mut self.subtables[within] {
            if let Some(rows) = subtable.ensure_order(order) {
                work.arrangements += 1;
                work.rows += rows;
            }
        }

        work
    }

    /// Returns the number of distinct column orders the subtables store.
    pub(super) fn stored_orders(&self) -> usize {
        let mut orders: Vec<&ColumnOrder> = Vec::new();
        for order in self.subtables.iter().flat_map(Subtable::orders) {
            if !orders.contains(&order) {
                orders.push(order);
            }
        }

        orders.len()
    }

    /// Appends the tries of the subtables within a range, arranged in the given order, to a list.
    ///
    /// Panics if the range does not align with the table's blocks,
    /// or if a subtable within it does not store the order.
    pub(super) fn tries_into<'s>(
        &'s self,
        range: Range<Revision>,
        order: &ColumnOrder,
        into: &mut Vec<&'s Trie>,
    ) {
        for subtable in &self.subtables[self.within(&range)] {
            let Some(trie) = subtable.trie(order) else {
                panic!("the order of a read must be ensured before the read");
            };
            into.push(trie);
        }
    }

    /// Returns the number of rows the subtables within a range hold.
    ///
    /// Panics if the range does not align with the table's blocks.
    pub(super) fn row_count(&self, range: Range<Revision>) -> usize {
        self.subtables[self.within(&range)]
            .iter()
            .map(Subtable::row_count)
            .sum()
    }

    /// Returns the blocks of the subtables, in ascending order.
    pub(super) fn blocks(&self) -> impl Iterator<Item = Range<Revision>> {
        self.subtables.iter().map(|subtable| subtable.range.clone())
    }

    /// Returns the start of the block the given revision lies strictly inside,
    /// or the revision itself if it lies inside none.
    pub(super) fn aligned_floor(&self, revision: Revision) -> Revision {
        self.subtables
            .iter()
            .find(|subtable| subtable.range.start < revision && revision < subtable.range.end)
            .map_or(revision, |subtable| subtable.range.start)
    }

    /// Returns the revision of the latest batch the subtables hold,
    /// which is the last revision of the last block.
    pub(super) fn last_added(&self) -> Option<Revision> {
        self.subtables
            .last()
            .map(|subtable| Revision(subtable.range.end.0 - 1))
    }

    /// Returns the next merge the policy proposes,
    /// within the compartments the walls divide the table into, the newest first.
    pub(super) fn plan_consolidation(
        &self,
        walls: &[Revision],
        policy: MergePolicy,
    ) -> Option<Range<Revision>> {
        self.subtables
            .chunk_by(|before, after| {
                !walls
                    .iter()
                    .any(|&wall| before.range.end <= wall && wall <= after.range.start)
            })
            .rev()
            .find_map(|compartment| policy.propose(compartment))
    }

    /// Merges the subtables within a range into one,
    /// and returns the number of rows it holds.
    ///
    /// Panics if the range does not align with the table's blocks,
    /// holds no subtable, or holds subtables that share no column order.
    pub(super) fn consolidate(&mut self, range: Range<Revision>) -> usize {
        let within = self.within(&range);
        let merged = Subtable::merge(&self.subtables[within.clone()]);

        let rows = merged.row_count();
        self.subtables.splice(within, [merged]);

        rows
    }

    /// Drops the subtables within a range,
    /// and returns the number of rows they held.
    ///
    /// Panics if the range does not align with the table's blocks.
    pub(super) fn drop_rows(&mut self, range: Range<Revision>) -> usize {
        let within = self.within(&range);
        self.subtables
            .drain(within)
            .map(|subtable| subtable.row_count())
            .sum()
    }

    /// Returns the allocated bytes of the subtables.
    pub(super) fn memory_bytes(&self) -> usize {
        self.subtables.iter().map(Subtable::memory_bytes).sum()
    }

    /// Returns the positions of the subtables whose blocks lie within a range.
    ///
    /// Panics if a block straddles one of the range's ends.
    fn within(&self, range: &Range<Revision>) -> Range<usize> {
        let start = self
            .subtables
            .partition_point(|subtable| subtable.range.end <= range.start);
        let end = self
            .subtables
            .partition_point(|subtable| subtable.range.start < range.end)
            .max(start);

        let inside = &self.subtables[start..end];
        let straddles = inside
            .first()
            .is_some_and(|first| first.range.start < range.start)
            || inside.last().is_some_and(|last| last.range.end > range.end);
        assert!(
            !straddles,
            "a revision range must align with the table's blocks"
        );

        start..end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        storage::PayloadEncoding,
        testing::{assert_panics, int_trie},
    };

    /// Returns a table of the given arity without rows.
    fn table(arity: usize) -> Table {
        Table::new("edge".to_string(), arity)
    }

    /// Returns the integer rows of the subtables within a range, in the given order.
    fn read(table: &Table, range: Range<Revision>, order: &ColumnOrder) -> Vec<Vec<i64>> {
        let mut tries = Vec::new();
        table.tries_into(range, order, &mut tries);

        let mut rows = Vec::new();
        for trie in tries {
            trie.for_each_row(|row| {
                rows.push(
                    row.iter()
                        .map(|value| i64::from_payload(value.payload))
                        .collect(),
                );
            });
        }
        rows
    }

    /// Returns the number of tries a read of every revision covers.
    fn width(table: &Table, order: &ColumnOrder) -> usize {
        let mut tries = Vec::new();
        table.tries_into(Revision(0)..Revision(100), order, &mut tries);
        tries.len()
    }

    /// Consolidates a table until the policy proposes nothing,
    /// and returns the merged ranges.
    fn consolidate_fully(
        table: &mut Table,
        walls: &[Revision],
        policy: MergePolicy,
    ) -> Vec<Range<Revision>> {
        let mut merged = Vec::new();
        while let Some(range) = table.plan_consolidation(walls, policy) {
            table.consolidate(range.clone());
            merged.push(range);
        }

        merged
    }

    #[test]
    fn orders_are_built_from_the_stored_copy() {
        let mut table = table(2);
        let swapped = ColumnOrder::new(&[1, 0]).expect("valid");
        let natural = ColumnOrder::identity(2);
        let range = Revision(0)..Revision(10);

        // The rows arrive with their columns swapped: the rows are (b, a).
        table.add(
            Revision(1),
            swapped.clone(),
            int_trie(2, &[&[2, 1], &[3, 1]]),
        );

        assert_panics("ensured", || {
            table.tries_into(range.clone(), &natural, &mut Vec::new())
        });

        let work = table.ensure_order(range.clone(), &natural);
        assert_eq!(
            work,
            ArrangementWork {
                arrangements: 1,
                rows: 2
            }
        );
        assert_eq!(
            read(&table, range.clone(), &natural),
            vec![vec![1, 2], vec![1, 3]]
        );
        assert_eq!(
            read(&table, range.clone(), &swapped),
            vec![vec![2, 1], vec![3, 1]]
        );

        // A stored order is not built again.
        let work = table.ensure_order(range, &natural);
        assert_eq!(work, ArrangementWork::default());
        assert_eq!(table.stored_orders(), 2);
    }

    #[test]
    fn an_order_sharing_a_prefix_sorts_within_its_groups() {
        let mut table = table(3);
        let stored = ColumnOrder::identity(3);
        let target = ColumnOrder::new(&[0, 2, 1]).expect("valid");
        let range = Revision(0)..Revision(10);
        table.add(
            Revision(1),
            stored,
            int_trie(3, &[&[1, 2, 9], &[1, 3, 1], &[2, 1, 5], &[2, 4, 0]]),
        );

        table.ensure_order(range.clone(), &target);
        assert_eq!(
            read(&table, range, &target),
            vec![vec![1, 1, 3], vec![1, 9, 2], vec![2, 0, 4], vec![2, 5, 1]]
        );
    }

    #[test]
    fn ranges_must_align_with_blocks() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        table.add(Revision(1), order.clone(), int_trie(1, &[&[1]]));
        table.add(Revision(2), order.clone(), int_trie(1, &[&[2]]));
        table.consolidate(Revision(1)..Revision(3));

        // The merged block spans revisions 1 and 2; a range may not split it.
        assert_panics("align", || table.row_count(Revision(0)..Revision(2)));
        assert_panics("align", || table.row_count(Revision(2)..Revision(2)));
        assert_eq!(table.row_count(Revision(3)..Revision(3)), 0);
        assert_eq!(
            read(&table, Revision(1)..Revision(3), &order),
            vec![vec![1], vec![2]]
        );

        // A revision inside the block reads from the block's start.
        assert_eq!(table.aligned_floor(Revision(2)), Revision(1));
        assert_eq!(table.aligned_floor(Revision(3)), Revision(3));
    }

    #[test]
    fn consolidation_merges_every_shared_order() {
        let mut table = table(2);
        let order = ColumnOrder::identity(2);
        let swapped = ColumnOrder::new(&[1, 0]).expect("valid");
        table.add(Revision(1), order.clone(), int_trie(2, &[&[1, 2], &[3, 4]]));
        table.add(Revision(2), order.clone(), int_trie(2, &[&[1, 2], &[5, 6]]));
        table.ensure_order(Revision(1)..Revision(3), &swapped);

        let rows = table.consolidate(Revision(1)..Revision(3));
        assert_eq!(rows, 3);
        assert_eq!(width(&table, &order), 1);
        assert_eq!(
            read(&table, Revision(1)..Revision(3), &order),
            vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        );
        assert_eq!(
            read(&table, Revision(1)..Revision(3), &swapped),
            vec![vec![2, 1], vec![4, 3], vec![6, 5]]
        );
    }

    #[test]
    fn the_last_batch_survives_merges_and_follows_drops() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        assert_eq!(table.last_added(), None);
        for (revision, value) in [(1, 1), (3, 3), (4, 4)] {
            table.add(Revision(revision), order.clone(), int_trie(1, &[&[value]]));
        }
        table.add(Revision(5), order.clone(), Trie::empty(1));
        assert_eq!(table.last_added(), Some(Revision(4)));

        table.consolidate(Revision(3)..Revision(5));
        assert_eq!(table.last_added(), Some(Revision(4)));

        assert_eq!(table.drop_rows(Revision(2)..Revision(5)), 2);
        assert_eq!(table.last_added(), Some(Revision(1)));
        assert_eq!(
            table.blocks().collect::<Vec<_>>(),
            vec![Revision(1)..Revision(2)]
        );
    }

    #[test]
    fn the_whole_merge_leaves_one_subtable_per_compartment() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        for (revision, value) in (1..=5).zip(1..) {
            table.add(Revision(revision), order.clone(), int_trie(1, &[&[value]]));
        }

        assert_eq!(
            consolidate_fully(&mut table, &[Revision(3)], MergePolicy::Whole),
            vec![Revision(3)..Revision(6), Revision(1)..Revision(3)]
        );
        assert_eq!(
            table.blocks().collect::<Vec<_>>(),
            vec![Revision(1)..Revision(3), Revision(3)..Revision(6)]
        );
    }

    #[test]
    fn the_doubling_schedule_cascades_within_walls() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        // Blocks of 4, 2, and 2 rows at revisions 1, 2, 3.
        let blocks: [&[&[i64]]; 3] = [&[&[1], &[2], &[3], &[4]], &[&[10], &[11]], &[&[20], &[21]]];
        for (index, rows) in blocks.iter().enumerate() {
            table.add(Revision(index as u64 + 1), order.clone(), int_trie(1, rows));
        }

        // Without walls the cascade swallows everything:
        // 2 <= 2, then 4 <= 4.
        assert_eq!(
            table.plan_consolidation(&[], MergePolicy::Doubling),
            Some(Revision(1)..Revision(4))
        );
        // A wall at revision 2 keeps the first block apart;
        // the compartment behind it merges.
        assert_eq!(
            table.plan_consolidation(&[Revision(2)], MergePolicy::Doubling),
            Some(Revision(2)..Revision(4))
        );
        // A wall at revision 3 leaves compartments of 4 + 2 and 2 rows:
        // neither merges.
        assert_eq!(
            table.plan_consolidation(&[Revision(3)], MergePolicy::Doubling),
            None
        );

        // Consolidating the proposal leaves nothing to merge.
        assert_eq!(
            consolidate_fully(&mut table, &[Revision(2)], MergePolicy::Doubling),
            vec![Revision(2)..Revision(4)]
        );
    }

    #[test]
    fn the_two_level_schedule_keeps_reads_two_blocks_wide() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        let mut revision = 0;
        let mut add = |table: &mut Table, rows: &[&[i64]]| {
            revision += 1;
            table.add(Revision(revision), order.clone(), int_trie(1, rows));
        };

        // A lone block proposes nothing.
        add(
            &mut table,
            &[&[1], &[2], &[3], &[4], &[5], &[6], &[7], &[8], &[9]],
        );
        assert_eq!(table.plan_consolidation(&[], MergePolicy::TwoLevel), None);

        // A single block behind the leader has nothing to merge with.
        add(&mut table, &[&[10]]);
        assert_eq!(table.plan_consolidation(&[], MergePolicy::TwoLevel), None);

        // Every further batch merges the suffix,
        // and each merge adds the rows it wrote to the suffix's account: 2 rows, then 3.
        // The leader stays out of it while the account is below its 9 rows.
        for batch in [&[11], &[12]] {
            add(&mut table, &[batch]);
            let merges = consolidate_fully(&mut table, &[], MergePolicy::TwoLevel);
            assert_eq!(merges.len(), 1, "the suffix merges, the leader waits");
            assert_ne!(merges[0].start, Revision(1), "the leader is untouched");
            assert_eq!(width(&table, &order), 2, "reads stay two blocks wide");
        }

        // The next batch takes the account of 5 rows plus the merge it owes, 4 more,
        // past the leader's 9 rows: the suffix folds into the leader.
        // One proposal does it, so the suffix's rows are written once.
        add(&mut table, &[&[13]]);
        assert_eq!(
            consolidate_fully(&mut table, &[], MergePolicy::TwoLevel),
            vec![Revision(1)..Revision(6)]
        );
        assert_eq!(table.row_count(Revision(0)..Revision(100)), 13);
        assert_eq!(width(&table, &order), 1);

        // Walls keep the schedule apart like any other.
        add(&mut table, &[&[20]]);
        add(&mut table, &[&[21]]);
        assert_eq!(
            table.plan_consolidation(&[Revision(6)], MergePolicy::TwoLevel),
            None
        );
    }

    #[test]
    fn a_fold_behind_a_wall_settles_its_compartments_account() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        let mut revision = 0;
        let mut add = |table: &mut Table, rows: &[&[i64]]| {
            revision += 1;
            table.add(Revision(revision), order.clone(), int_trie(1, rows));
        };
        // A block that a wall keeps apart,
        // so that the compartment behind it leads from the second block.
        let walls = [Revision(2)];
        add(&mut table, &[&[0]]);

        // The same cycle as without the wall: suffix merges, then the fold.
        add(
            &mut table,
            &[&[1], &[2], &[3], &[4], &[5], &[6], &[7], &[8], &[9]],
        );
        for batch in [&[10], &[11], &[12]] {
            add(&mut table, &[batch]);
            consolidate_fully(&mut table, &walls, MergePolicy::TwoLevel);
        }
        add(&mut table, &[&[13]]);
        assert_eq!(
            consolidate_fully(&mut table, &walls, MergePolicy::TwoLevel),
            vec![Revision(2)..Revision(7)]
        );

        // The fold settled the compartment's account:
        // the next batches merge the suffix alone, and the walled block stays apart.
        add(&mut table, &[&[14]]);
        assert_eq!(
            consolidate_fully(&mut table, &walls, MergePolicy::TwoLevel),
            vec![]
        );
        add(&mut table, &[&[15]]);
        assert_eq!(
            consolidate_fully(&mut table, &walls, MergePolicy::TwoLevel),
            vec![Revision(7)..Revision(9)]
        );
        assert_eq!(
            table
                .subtables
                .iter()
                .map(Subtable::row_count)
                .collect::<Vec<_>>(),
            vec![1, 13, 2]
        );
    }
}
