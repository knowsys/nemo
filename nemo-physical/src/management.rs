//! The table manager: the tables of one database instance.
//!
//! A table holds rows of one arity, added in batches.
//! Each batch occupies a [Revision] of the table.
//! A table keeps its rows in subtables,
//! each holding the rows of one range of revisions, its block.
//!
//! A subtable stores its rows in arrangements: one [trie](Trie) per column order,
//! a [ColumnOrder] of the table's columns.
//! It starts with the order its rows arrived in,
//! and builds arrangements in further orders on request, each from the closest one it stores.
//! The arrangements are the indexes of the table.
//!
//! A read addresses a range of revisions and a column order,
//! and takes the arrangements in that order of the subtables whose blocks lie within it,
//! as a union.
//! A range must align with the blocks: no block may straddle one of its ends.
//! Consolidation merges adjacent subtables into one, so that reads cover fewer arrangements.
//! A wall is a revision no merge crosses:
//! the subtables before it and those from it on stay apart, so a read may still start there.
//! The walls divide a table into compartments,
//! and within each a [MergePolicy] chooses the subtables to merge.

mod column_order;
mod policy;
mod revision;
mod subtable;
mod table;

pub use column_order::ColumnOrder;
pub use policy::MergePolicy;
pub use revision::Revision;
pub use table::TableId;

use std::ops::{ControlFlow, Range};

use crate::{
    management::table::Table,
    storage::StorageValue,
    trie::{RowBuffer, Trie},
};

/// The tables of one database instance.
///
/// A table is addressed by the [TableId] its registration returns,
/// and the id of a dropped table is not reused.
/// Every method that takes a table id panics if the id is unknown or dropped:
/// if this manager did not register it, or has dropped the table since.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     management::{MergePolicy, ColumnOrder, Revision, TableManager},
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// let mut manager = TableManager::new();
/// let edge = manager.register("edge", 2);
///
/// // Two batches of rows, stored at revisions 1 and 2.
/// for (revision, batch) in [(1, [(1, 2), (2, 3)]), (2, [(3, 1), (2, 3)])] {
///     let mut rows = RowBuffer::new(2);
///     for (from, to) in batch {
///         rows.push_row(&[StorageValue::integer(from), StorageValue::integer(to)]);
///     }
///     manager.add_rows(edge, Revision(revision), ColumnOrder::identity(2), rows);
/// }
///
/// // A read by the second column first needs an arrangement in that order,
/// // which each of the two subtables builds.
/// let swapped = ColumnOrder::new(&[1, 0]).expect("a permutation");
/// let all = Revision(0)..Revision(3);
/// assert_eq!(manager.ensure_order(edge, all.clone(), &swapped).arrangements, 2);
/// assert_eq!(manager.tries(edge, all.clone(), &swapped).len(), 2);
///
/// // Consolidation merges the subtables into one, which keeps both orders.
/// manager.consolidate_with(edge, &[], MergePolicy::Whole);
/// assert_eq!(manager.blocks(edge), vec![Revision(1)..Revision(3)]);
/// assert_eq!(manager.row_count(edge, all.clone()), 3);
/// assert_eq!(manager.tries(edge, all, &swapped).len(), 1);
/// ```
#[derive(Debug, Default)]
pub struct TableManager {
    /// The tables, indexed by their ids; a dropped table leaves `None`.
    tables: Vec<Option<Table>>,
}

/// The work of building arrangements:
/// the number of arrangements built, and the rows written to build them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArrangementWork {
    /// The number of arrangements built.
    pub arrangements: usize,
    /// The number of rows written to build them.
    pub rows: usize,
}

impl TableManager {
    /// Creates a manager without tables.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a table without rows, and returns its id.
    ///
    /// The name identifies the table in reports and diagnostics.
    pub fn register(&mut self, name: impl Into<String>, arity: usize) -> TableId {
        let id = TableId::new(self.tables.len());
        self.tables.push(Some(Table::new(name.into(), arity)));

        id
    }

    /// Returns a manager with the same tables under the same ids, without their rows.
    ///
    /// The tables registered with the copy get ids this manager has not given out,
    /// so a dry run can register tables beside the existing ones.
    pub fn without_rows(&self) -> Self {
        let tables = self
            .tables
            .iter()
            .map(|table| {
                table
                    .as_ref()
                    .map(|table| Table::new(table.name.clone(), table.arity))
            })
            .collect();
        Self { tables }
    }

    /// Returns the table's name.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn name(&self, table: TableId) -> &str {
        &self.table(table).name
    }

    /// Returns the table's arity, its number of columns.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn arity(&self, table: TableId) -> usize {
        self.table(table).arity
    }

    /// Stores a batch of rows, its columns laid out in `order`,
    /// as a new subtable at the given revision,
    /// and returns the number of distinct rows stored.
    ///
    /// The rows need not be sorted or distinct.
    ///
    /// # Panics
    ///
    /// Panics as [add_trie](Self::add_trie) does.
    pub fn add_rows(
        &mut self,
        table: TableId,
        revision: Revision,
        order: ColumnOrder,
        rows: RowBuffer,
    ) -> usize {
        self.add_trie(table, revision, order, Trie::from_buffer(rows))
    }

    /// Stores a trie, its columns laid out in `order`,
    /// as a new subtable at the given revision,
    /// and returns the number of rows stored.
    ///
    /// An empty trie stores nothing.
    /// A revision holds one subtable,
    /// so the tries of one batch are [merged](Trie::merge) before they are added.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// if the order or the trie does not match the table's arity,
    /// or if the trie is nonempty and its revision does not follow the table's blocks.
    pub fn add_trie(
        &mut self,
        table: TableId,
        revision: Revision,
        order: ColumnOrder,
        trie: Trie,
    ) -> usize {
        self.table_mut(table).add(revision, order, trie)
    }

    /// Builds the arrangements in the given column order
    /// that the subtables within a range lack,
    /// and returns the work that took.
    ///
    /// Each is built from the closest arrangement its subtable stores.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// if the order does not match the table's arity,
    /// or if the range does not align with the table's blocks.
    pub fn ensure_order(
        &mut self,
        table: TableId,
        range: Range<Revision>,
        order: &ColumnOrder,
    ) -> ArrangementWork {
        let work = self.table_mut(table).ensure_order(range, order);
        if work.arrangements > 0 {
            tracing::debug!(
                table = table.index(),
                arrangements = work.arrangements,
                rows = work.rows,
                "built a column order"
            );
        }

        work
    }

    /// Returns the number of distinct column orders the table's subtables store.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn stored_orders(&self, table: TableId) -> usize {
        self.table(table).stored_orders()
    }

    /// Returns the tries of the subtables within a range, arranged in the given column order,
    /// in ascending revision order, which the caller reads as a union.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// if the range does not align with the table's blocks,
    /// or if the order was not [ensured](Self::ensure_order) over it.
    pub fn tries(&self, table: TableId, range: Range<Revision>, order: &ColumnOrder) -> Vec<&Trie> {
        let mut tries = Vec::new();
        self.tries_into(table, range, order, &mut tries);

        tries
    }

    /// Appends the tries of [tries](Self::tries) to a list, panicking as it does.
    ///
    /// A reader that gathers the tries of several tables thus allocates one list.
    pub(crate) fn tries_into<'s>(
        &'s self,
        table: TableId,
        range: Range<Revision>,
        order: &ColumnOrder,
        into: &mut Vec<&'s Trie>,
    ) {
        self.table(table).tries_into(range, order, into);
    }

    /// Visits the rows of the tries of [tries](Self::tries) from the given position on,
    /// until the visitor breaks or the rows end.
    ///
    /// The position counts the rows of the tries one after the other.
    /// A paged reader thus resumes at the position where it stopped.
    ///
    /// # Panics
    ///
    /// Panics as [tries](Self::tries) does.
    pub fn for_each_row_from(
        &self,
        table: TableId,
        range: Range<Revision>,
        order: &ColumnOrder,
        start: usize,
        mut visit: impl FnMut(&[StorageValue]) -> ControlFlow<()>,
    ) {
        let mut skip = start;

        for trie in self.tries(table, range, order) {
            if skip >= trie.row_count() {
                skip -= trie.row_count();
                continue;
            }

            let mut flow = ControlFlow::Continue(());
            trie.for_each_row_from(skip, |row| {
                flow = visit(row);
                flow
            });

            if flow.is_break() {
                break;
            }

            skip = 0;
        }
    }

    /// Returns the number of rows the subtables within a range hold.
    ///
    /// A row held by several subtables counts once for each.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// or if the range does not align with the table's blocks.
    pub fn row_count(&self, table: TableId, range: Range<Revision>) -> usize {
        self.table(table).row_count(range)
    }

    /// Returns the blocks of the table's subtables, in ascending order.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn blocks(&self, table: TableId) -> Vec<Range<Revision>> {
        self.table(table).blocks().collect()
    }

    /// Returns the nearest revision at or below the given one that aligns with the table's blocks:
    /// the revision itself, unless it lies strictly inside a block,
    /// and the start of that block otherwise.
    ///
    /// A reader that tolerates reading some rows twice starts its ranges there,
    /// so that consolidation need not keep its exact split points.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn aligned_floor(&self, table: TableId, revision: Revision) -> Revision {
        self.table(table).aligned_floor(revision)
    }

    /// Returns the revision of the latest batch the table holds,
    /// or `None` if it holds no rows.
    ///
    /// Consolidation leaves it unchanged.
    /// A reader that remembers the revision it read up to
    /// thus tells whether the table holds rows it has not read.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn last_added(&self, table: TableId) -> Option<Revision> {
        self.table(table).last_added()
    }

    /// Returns the next merge the policy proposes,
    /// as a range of revisions for [consolidate](Self::consolidate),
    /// or `None` if it proposes none.
    ///
    /// No proposal crosses one of the walls.
    /// If the policy would merge in several places, the newest comes first.
    ///
    /// A merge changes what the policy proposes next,
    /// so the caller consolidates each proposal and asks again until none is left,
    /// as [consolidate_with](Self::consolidate_with) does.
    /// That ends, since a proposal covers at least two subtables and merges them into one.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped. See [TableManager].
    pub fn plan_consolidation(
        &self,
        table: TableId,
        walls: &[Revision],
        policy: MergePolicy,
    ) -> Option<Range<Revision>> {
        self.table(table).plan_consolidation(walls, policy)
    }

    /// Merges the subtables within a range into one,
    /// and returns the number of rows it holds.
    ///
    /// The merged subtable stores every column order that all the merged subtables store,
    /// so a read finds the orders it needed before the merge,
    /// and the merge writes its rows once per such order.
    /// The merged subtables are dropped.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// if the range does not align with the table's blocks or holds none of them,
    /// or if the subtables within it share no column order.
    pub fn consolidate(&mut self, table: TableId, range: Range<Revision>) -> usize {
        tracing::trace!(
            table = table.index(),
            from = range.start.0,
            to = range.end.0,
            "consolidating"
        );

        self.table_mut(table).consolidate(range)
    }

    /// Merges the subtables of a table as the policy proposes, until it proposes nothing,
    /// and returns the number of rows the merges wrote.
    ///
    /// No merge crosses one of the walls.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// or if subtables the policy merges share no column order.
    pub fn consolidate_with(
        &mut self,
        table: TableId,
        walls: &[Revision],
        policy: MergePolicy,
    ) -> usize {
        let mut rewritten = 0;
        while let Some(range) = self.plan_consolidation(table, walls, policy) {
            rewritten += self.consolidate(table, range);
        }

        rewritten
    }

    /// Drops the subtables within a range,
    /// and returns the number of rows they held.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or dropped (see [TableManager]),
    /// or if the range does not align with the table's blocks.
    pub fn drop_rows(&mut self, table: TableId, range: Range<Revision>) -> usize {
        self.table_mut(table).drop_rows(range)
    }

    /// Drops a table with its rows.
    ///
    /// # Panics
    ///
    /// Panics if the table is unknown or already dropped. See [TableManager].
    pub fn drop_table(&mut self, table: TableId) {
        let slot = self.tables.get_mut(table.index()).and_then(Option::take);

        assert!(slot.is_some(), "unknown or dropped table id");
    }

    /// Returns the allocated bytes of every table's arrangements.
    pub fn memory_bytes(&self) -> usize {
        self.tables.iter().flatten().map(Table::memory_bytes).sum()
    }

    /// Returns the table behind an id; panics if it is unknown or dropped.
    fn table(&self, table: TableId) -> &Table {
        self.tables
            .get(table.index())
            .and_then(Option::as_ref)
            .expect("unknown or dropped table id")
    }

    /// Returns the table behind an id for changing it; panics if it is unknown or dropped.
    fn table_mut(&mut self, table: TableId) -> &mut Table {
        self.tables
            .get_mut(table.index())
            .and_then(Option::as_mut)
            .expect("unknown or dropped table id")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        storage::PayloadEncoding,
        testing::{assert_panics, int_buffer},
    };

    /// Returns the integer rows a table holds within a range, in the given order.
    fn read(
        manager: &TableManager,
        table: TableId,
        range: Range<Revision>,
        order: &ColumnOrder,
    ) -> Vec<Vec<i64>> {
        let mut rows = Vec::new();
        manager.for_each_row_from(table, range, order, 0, |row| {
            rows.push(
                row.iter()
                    .map(|value| i64::from_payload(value.payload))
                    .collect(),
            );
            ControlFlow::Continue(())
        });
        rows
    }

    #[test]
    fn added_rows_deduplicate() {
        let mut manager = TableManager::new();
        let table = manager.register("edge", 2);
        let order = ColumnOrder::identity(2);
        let range = Revision(0)..Revision(10);

        let stored = manager.add_rows(
            table,
            Revision(1),
            order.clone(),
            int_buffer(2, &[&[2, 1], &[1, 2], &[2, 1]]),
        );
        assert_eq!(stored, 2);
        assert_eq!(
            read(&manager, table, range.clone(), &order),
            vec![vec![1, 2], vec![2, 1]]
        );

        // An empty batch stores nothing.
        let stored = manager.add_rows(table, Revision(2), order, RowBuffer::new(2));
        assert_eq!(stored, 0);
        assert_eq!(manager.row_count(table, range), 2);
    }

    #[test]
    fn added_rows_need_the_arity_and_a_later_revision() {
        let mut manager = TableManager::new();
        let table = manager.register("edge", 2);
        let order = ColumnOrder::identity(2);
        manager.add_rows(table, Revision(1), order.clone(), int_buffer(2, &[&[1, 2]]));

        assert_panics("arity", || {
            manager.add_rows(
                table,
                Revision(3),
                ColumnOrder::identity(1),
                int_buffer(1, &[&[9]]),
            )
        });
        assert_panics("ascending revision order", || {
            manager.add_rows(table, Revision(0), order, int_buffer(2, &[&[9, 9]]))
        });
    }

    #[test]
    fn paged_reads_resume_across_subtables() {
        let mut manager = TableManager::new();
        let table = manager.register("edge", 1);
        let order = ColumnOrder::identity(1);
        let blocks: [&[&[i64]]; 2] = [&[&[1], &[2]], &[&[3], &[4]]];
        for (index, rows) in blocks.iter().enumerate() {
            manager.add_rows(
                table,
                Revision(index as u64 + 1),
                order.clone(),
                int_buffer(1, rows),
            );
        }

        let mut page = Vec::new();
        manager.for_each_row_from(table, Revision(0)..Revision(3), &order, 1, |row| {
            page.push(i64::from_payload(row[0].payload));
            if page.len() == 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        assert_eq!(page, vec![2, 3]);
    }

    #[test]
    fn consolidation_runs_until_the_policy_proposes_nothing() {
        let mut manager = TableManager::new();
        let table = manager.register("edge", 1);
        let order = ColumnOrder::identity(1);
        for (revision, value) in (1..=4).zip(1..) {
            manager.add_rows(
                table,
                Revision(revision),
                order.clone(),
                int_buffer(1, &[&[value]]),
            );
        }

        // The wall at revision 3 keeps two compartments of two rows each,
        // each merged once.
        let rewritten = manager.consolidate_with(table, &[Revision(3)], MergePolicy::Whole);
        assert_eq!(rewritten, 4);
        assert_eq!(
            manager.blocks(table),
            vec![Revision(1)..Revision(3), Revision(3)..Revision(5)]
        );

        // Nothing is left to merge.
        let rewritten = manager.consolidate_with(table, &[Revision(3)], MergePolicy::Whole);
        assert_eq!(rewritten, 0);
    }

    #[test]
    fn dropped_tables_keep_their_ids() {
        let mut manager = TableManager::new();
        let first = manager.register("a", 1);
        let second = manager.register("b", 1);
        manager.add_rows(
            first,
            Revision(1),
            ColumnOrder::identity(1),
            int_buffer(1, &[&[1]]),
        );
        assert!(manager.memory_bytes() > 0);

        manager.drop_table(first);
        assert_panics("dropped", || manager.name(first).len());
        assert_panics("dropped", || manager.drop_table(first));
        assert_eq!(manager.name(second), "b");
        assert_eq!(manager.memory_bytes(), 0);
    }

    #[test]
    fn a_copy_without_rows_keeps_the_ids() {
        let mut manager = TableManager::new();
        let first = manager.register("a", 1);
        let second = manager.register("b", 2);
        manager.add_rows(
            second,
            Revision(1),
            ColumnOrder::identity(2),
            int_buffer(2, &[&[1, 2]]),
        );
        manager.drop_table(first);

        let mut copy = manager.without_rows();
        assert_panics("dropped", || copy.name(first).len());
        assert_eq!((copy.name(second), copy.arity(second)), ("b", 2));
        assert_eq!(copy.row_count(second, Revision(0)..Revision(2)), 0);
        // A table registered with the copy takes an id beside the copied ones.
        assert_eq!(copy.register("c", 1), manager.register("c", 1));
    }
}
