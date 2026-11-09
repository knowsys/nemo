//! Tables, which keep their rows in subtables.

use std::ops::Range;

use crate::{
    management::{ArrangementWork, ColumnOrder, MergePolicy, Revision, subtable::Subtable},
    storage::StorageValue,
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

/// A table, whose rows are stored in subtables.
#[derive(Debug)]
pub(super) struct Table {
    /// The table's name, for reports and diagnostics.
    pub(super) name: String,
    /// The number of columns.
    pub(super) arity: usize,
    /// The subtables, whose blocks are disjoint and in ascending order.
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

    /// Stores a trie as a new subtable at the given revision,
    /// and returns the number of rows stored.
    ///
    /// `order` is the column order of the trie.
    /// An empty trie stores nothing.
    /// Panics if the order or the trie does not match the table's arity.
    /// For a nonempty trie, it also panics if the revision is below the end of the last block,
    /// or if the order differs from the order of the earlier batches.
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
            assert!(
                last.arrival_order() == &order,
                "a table's batches arrive in one column order"
            );
        }

        let rows = trie.row_count();
        self.subtables.push(Subtable::new(revision, order, trie));

        rows
    }

    /// Builds the arrangements in the given column order
    /// that the subtables within a range lack, and returns the work done.
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

    /// Returns the number of distinct column orders that the subtables store.
    pub(super) fn stored_orders(&self) -> usize {
        let mut orders: Vec<&ColumnOrder> = Vec::new();
        for order in self.subtables.iter().flat_map(Subtable::orders) {
            if !orders.contains(&order) {
                orders.push(order);
            }
        }

        orders.len()
    }

    /// Appends to `into` the tries in the given order of the subtables within a range.
    ///
    /// Panics if the range does not align with the table's blocks,
    /// or if a subtable within it does not store the order.
    pub(super) fn tries_into<'s>(
        &'s self,
        range: Range<Revision>,
        order: &ColumnOrder,
        into: &mut impl Extend<&'s Trie>,
    ) {
        into.extend(self.subtables[self.within(&range)].iter().map(|subtable| {
            subtable
                .trie(order)
                .expect("the order of a read must be ensured before the read")
        }));
    }

    /// Returns the number of rows of the subtables within a range.
    ///
    /// Panics if the range does not align with the table's blocks.
    pub(super) fn row_count(&self, range: Range<Revision>) -> usize {
        self.subtables[self.within(&range)]
            .iter()
            .map(Subtable::row_count)
            .sum()
    }

    /// Returns the earliest revision at which the row was added,
    /// or `None` if the table does not contain it.
    ///
    /// The row's columns must be in the order in which the table's batches arrive.
    /// Panics if the row does not match the table's arity.
    pub(super) fn revision_of(&self, row: &[StorageValue]) -> Option<Revision> {
        assert_eq!(
            row.len(),
            self.arity,
            "a looked-up row must match the table's arity"
        );
        self.subtables
            .iter()
            .find_map(|subtable| subtable.revision_of(row))
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

    /// Returns the revision of the latest batch in the subtables,
    /// which is the last revision of the last block,
    /// or `None` if there are no subtables.
    pub(super) fn last_added(&self) -> Option<Revision> {
        self.subtables
            .last()
            .map(|subtable| Revision(subtable.range.end.0 - 1))
    }

    /// Returns the next merge that the policy proposes, or `None` if it proposes none.
    ///
    /// The policy plans each compartment between the walls on its own,
    /// the newest compartment first.
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
    /// Panics if the range does not align with the table's blocks, or contains no subtable.
    pub(super) fn consolidate(&mut self, range: Range<Revision>) -> usize {
        let within = self.within(&range);
        let merged = Subtable::merge(&self.subtables[within.clone()]);

        let rows = merged.row_count();
        self.subtables.splice(within, [merged]);

        rows
    }

    /// Drops the subtables within a range,
    /// and returns the number of rows that they contained.
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

    /// An order is built once, from the stored arrangement,
    /// and a read in an order that was not built panics.
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

    /// An order that shares a prefix with the stored one is built sorted,
    /// although only the groups of that prefix are sorted.
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

    /// A range that splits a block panics,
    /// and the aligned floor of a revision inside a block is the block's start.
    #[test]
    fn ranges_must_align_with_blocks() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        table.add(Revision(1), order.clone(), int_trie(1, &[&[1]]));
        table.add(Revision(2), order.clone(), int_trie(1, &[&[2]]));
        table.consolidate(Revision(1)..Revision(3));

        // The merged block spans revisions 1 and 2, and a range may not split it.
        assert_panics("align", || table.row_count(Revision(0)..Revision(2)));
        assert_panics("align", || table.row_count(Revision(2)..Revision(2)));
        assert_eq!(table.row_count(Revision(3)..Revision(3)), 0);
        assert_eq!(
            read(&table, Revision(1)..Revision(3), &order),
            vec![vec![1], vec![2]]
        );

        // The aligned floor of a revision inside the block is the block's start.
        assert_eq!(table.aligned_floor(Revision(2)), Revision(1));
        assert_eq!(table.aligned_floor(Revision(3)), Revision(3));
    }

    /// A merge keeps every order that all merged subtables store,
    /// and keeps a row of several subtables once.
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

    /// The revision of the last batch ignores an empty batch,
    /// stays the same through a merge, and changes with a drop.
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

    /// [Whole](MergePolicy::Whole) merges each compartment into one subtable,
    /// the newest compartment first.
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

    /// [Doubling](MergePolicy::Doubling) takes older subtables into the merge
    /// while they have no more rows than the merge has gathered, within the walls.
    #[test]
    fn the_doubling_schedule_cascades_within_walls() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        // Blocks of 4, 2, and 2 rows at revisions 1, 2, 3.
        let blocks: [&[&[i64]]; 3] = [&[&[1], &[2], &[3], &[4]], &[&[10], &[11]], &[&[20], &[21]]];
        for (index, rows) in blocks.iter().enumerate() {
            table.add(Revision(index as u64 + 1), order.clone(), int_trie(1, rows));
        }

        // Without walls, every block is taken: 2 rows against 2, then 4 rows against 4.
        assert_eq!(
            table.plan_consolidation(&[], MergePolicy::Doubling),
            Some(Revision(1)..Revision(4))
        );
        // A wall at revision 2 keeps the first block apart,
        // and the compartment after the wall is merged.
        assert_eq!(
            table.plan_consolidation(&[Revision(2)], MergePolicy::Doubling),
            Some(Revision(2)..Revision(4))
        );
        // A wall at revision 3 leaves a compartment of 4 + 2 rows and one of 2 rows.
        // Neither is merged, since the 4 rows are more than 2.
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

    /// [TwoLevel](MergePolicy::TwoLevel) merges new batches into the suffix
    /// until the rewritten rows reach the leader's rows, and then merges the suffix into the
    /// leader, so that reads stay at most two subtables wide.
    #[test]
    fn the_two_level_schedule_keeps_reads_two_blocks_wide() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        let mut revision = 0;
        let mut add = |table: &mut Table, rows: &[&[i64]]| {
            revision += 1;
            table.add(Revision(revision), order.clone(), int_trie(1, rows));
        };

        // A single block gives no proposal.
        add(
            &mut table,
            &[&[1], &[2], &[3], &[4], &[5], &[6], &[7], &[8], &[9]],
        );
        assert_eq!(table.plan_consolidation(&[], MergePolicy::TwoLevel), None);

        // A single block after the leader has nothing to merge with.
        add(&mut table, &[&[10]]);
        assert_eq!(table.plan_consolidation(&[], MergePolicy::TwoLevel), None);

        // Every further batch is merged into the suffix,
        // and each merge adds the rows it wrote to the suffix's rewritten rows: 2, then 3.
        // The leader is left alone while these are fewer than its 9 rows.
        for batch in [&[11], &[12]] {
            add(&mut table, &[batch]);
            let merges = consolidate_fully(&mut table, &[], MergePolicy::TwoLevel);
            assert_eq!(merges.len(), 1, "the suffix merges, the leader waits");
            assert_ne!(merges[0].start, Revision(1), "the leader is untouched");
            assert_eq!(width(&table, &order), 2, "reads stay two blocks wide");
        }

        // After the next batch, the 5 rewritten rows and the 4 rows of the suffix's pending merge
        // reach the leader's 9 rows, so the suffix is merged into the leader.
        // One proposal does it, so the suffix's rows are written once.
        add(&mut table, &[&[13]]);
        assert_eq!(
            consolidate_fully(&mut table, &[], MergePolicy::TwoLevel),
            vec![Revision(1)..Revision(6)]
        );
        assert_eq!(table.row_count(Revision(0)..Revision(100)), 13);
        assert_eq!(width(&table, &order), 1);

        // With a wall before the two new batches, they form a compartment of their own,
        // whose suffix of one subtable has nothing to merge with.
        add(&mut table, &[&[20]]);
        add(&mut table, &[&[21]]);
        assert_eq!(
            table.plan_consolidation(&[Revision(6)], MergePolicy::TwoLevel),
            None
        );
    }

    /// After [TwoLevel](MergePolicy::TwoLevel) merges a suffix into its leader behind a wall,
    /// the compartment counts rewritten rows anew.
    #[test]
    fn a_fold_behind_a_wall_settles_its_compartments_account() {
        let mut table = table(1);
        let order = ColumnOrder::identity(1);
        let mut revision = 0;
        let mut add = |table: &mut Table, rows: &[&[i64]]| {
            revision += 1;
            table.add(Revision(revision), order.clone(), int_trie(1, rows));
        };
        // A wall keeps the first block apart,
        // so the compartment after the wall has the second block as its leader.
        let walls = [Revision(2)];
        add(&mut table, &[&[0]]);

        // The same cycle as without the wall:
        // merges into the suffix, and then the merge into the leader.
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

        // After that merge, the new suffix starts without rewritten rows:
        // the next batches only merge the suffix, and the first block stays apart.
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
