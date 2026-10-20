//! The trie: the immutable table structure of the database layer.
//!
//! A trie holds a set of rows of one arity, which is the number of values per row.
//! The rows are stored in the "storage order":
//! lexicographic by column, each value by its tag and then its payload.
//! Column `c` holds one node for every distinct prefix of `c + 1` values among the rows,
//! in that order, so the nodes of the last column are the rows.
//! The nodes that extend one node of the column above are stored contiguously, in "blocks",
//! and the intervals of a column record where each block starts,
//! indexed by the node of the column above.
//! Column 0 is a single block.
//!
//! Tries are built from runs, sequences of rows in storage order:
//! a sorted row buffer as a whole, or the segments of a segmented one.
//! Rows may repeat within and across runs, and the trie keeps each once.

mod buffer;
mod column;
mod construct;
mod cursor;

pub use buffer::RowBuffer;
pub(crate) use buffer::{Run, SegmentedRowBuffer, SortedRowBuffer};
pub(crate) use column::{ColumnView, Intervals};

use std::ops::ControlFlow;

use crate::{
    storage::StorageValue,
    trie::{column::Column, cursor::RowCursor},
};

/// An immutable trie holding a set of rows.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     storage::StorageValue,
///     trie::{RowBuffer, Trie},
/// };
///
/// let int = StorageValue::integer;
/// let mut rows = RowBuffer::new(2);
/// for (a, b) in [(2, 1), (1, 3), (1, 2), (1, 3)] {
///     rows.push_row(&[int(a), int(b)]);
/// }
///
/// // The trie holds each row once, and visits the rows in order.
/// let trie = Trie::from_buffer(rows);
/// let mut visited = Vec::new();
/// trie.for_each_row(|row| visited.push(row.to_vec()));
/// assert_eq!(visited, [[int(1), int(2)], [int(1), int(3)], [int(2), int(1)]]);
///
/// // A merge holds each row of its tries once.
/// let mut more = RowBuffer::new(2);
/// more.push_row(&[int(1), int(2)]);
/// more.push_row(&[int(3), int(3)]);
/// let merged = Trie::merge(&[&trie, &Trie::from_buffer(more)]);
/// assert_eq!(merged.row_count(), 4);
/// ```
#[derive(Debug, Default)]
pub struct Trie {
    /// The number of values per row.
    arity: usize,
    /// The number of rows.
    rows: usize,
    /// The columns, one per row position.
    columns: Vec<Column>,
}

impl Trie {
    /// Returns the number of values per row.
    pub fn arity(&self) -> usize {
        self.arity
    }

    /// Returns the number of rows.
    pub fn row_count(&self) -> usize {
        self.rows
    }

    /// Returns whether the trie holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Returns a view of the values of the given column.
    pub(crate) fn column(&self, index: usize) -> ColumnView<'_> {
        self.columns[index].view()
    }

    /// Returns the intervals of the given column.
    ///
    /// The children of node `i` of column `c` are `self.intervals(c + 1).children(i)`,
    /// and column 0 is the single block 0.
    pub(crate) fn intervals(&self, index: usize) -> &Intervals {
        self.columns[index].intervals()
    }

    /// Creates the empty trie of the given arity.
    pub fn empty(arity: usize) -> Self {
        Self {
            arity,
            rows: 0,
            columns: (0..arity).map(|_| Column::empty()).collect(),
        }
    }

    /// Creates the nullary trie holding the empty row, or nothing.
    pub fn nullary(present: bool) -> Self {
        Self {
            arity: 0,
            rows: usize::from(present),
            columns: Vec::new(),
        }
    }

    /// Builds a trie from a buffer, sorting it unless it is.
    pub fn from_buffer(buffer: RowBuffer) -> Self {
        Self::from_sorted_buffer(&buffer.into_sorted())
    }

    /// Builds a trie from a sorted buffer.
    pub(crate) fn from_sorted_buffer(buffer: &SortedRowBuffer) -> Self {
        if buffer.arity() == 0 {
            return Self::nullary(!buffer.is_empty());
        }

        Self::from_runs(buffer.arity(), &[buffer.run()])
    }

    /// Builds a trie of the given positive arity from runs of that arity.
    ///
    /// Rows may repeat within a run and across runs; the trie holds each once.
    pub(crate) fn from_runs(arity: usize, runs: &[Run<'_>]) -> Self {
        assert!(arity > 0, "nullary rows hold no values; see Trie::nullary");

        Self::from_columns(arity, construct::from_runs(arity, runs))
    }

    /// Creates a trie of positive arity from its built columns.
    fn from_columns(arity: usize, columns: Vec<Column>) -> Self {
        debug_assert_eq!(columns.len(), arity, "one column per row position");

        Self {
            arity,
            rows: columns.last().map_or(0, Column::len),
            columns,
        }
    }

    /// Merges tries of one arity into one that holds each of their rows once.
    ///
    /// # Panics
    ///
    /// Panics if there are no tries, which leaves no arity,
    /// or if the tries differ in arity.
    pub fn merge(sources: &[&Trie]) -> Self {
        let arity = Self::merged_arity(sources);
        if arity == 0 {
            return Self::nullary(sources.iter().any(|source| !source.is_empty()));
        }

        Self::from_columns(arity, construct::from_tries(sources))
    }

    /// Returns the arity shared by the tries of a merge;
    /// panics on an empty or mixed-arity source list.
    fn merged_arity(sources: &[&Trie]) -> usize {
        let arity = sources
            .first()
            .expect("a merge covers at least one trie")
            .arity();

        assert!(
            sources.iter().all(|source| source.arity() == arity),
            "merged tries share one arity"
        );

        arity
    }

    /// Returns the number of distinct rows across tries of one arity:
    /// the [merge](Self::merge) without the build, for a consumer that needs the count alone.
    pub(crate) fn count_distinct(sources: &[&Trie]) -> usize {
        let arity = Self::merged_arity(sources);
        if arity == 0 {
            return usize::from(sources.iter().any(|source| !source.is_empty()));
        }

        construct::count_distinct(sources)
    }

    /// Returns the allocated bytes of the trie.
    pub fn memory_bytes(&self) -> usize {
        self.columns.iter().map(Column::memory_bytes).sum::<usize>()
            + self.columns.capacity() * size_of::<Column>()
    }

    /// Visits every row in storage order, reusing one row buffer.
    ///
    /// A nullary trie visits the empty row once if present.
    pub fn for_each_row(&self, mut visit: impl FnMut(&[StorageValue])) {
        self.for_each_row_from(0, |row| {
            visit(row);
            ControlFlow::Continue(())
        });
    }

    /// Visits the rows from the given index in storage order onward,
    /// reusing one row buffer, until the visitor breaks or the rows end.
    ///
    /// The entry seeks directly, so earlier rows are never visited,
    /// which makes this the paged read path:
    /// a consumer fetching a bounded chunk at a time resumes where it stopped
    /// at the cost of one seek.
    /// A nullary trie visits the empty row once if present and `start` is 0.
    pub fn for_each_row_from(
        &self,
        start: usize,
        mut visit: impl FnMut(&[StorageValue]) -> ControlFlow<()>,
    ) {
        if self.arity == 0 {
            if !self.is_empty() && start == 0 {
                // The one row is also the last, so whether the visitor breaks makes no difference.
                let _ = visit(&[]);
            }
            return;
        }

        let mut cursor = RowCursor::new(self, start);
        while !cursor.is_done() {
            if visit(cursor.row()).is_break() {
                return;
            }
            cursor.advance();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::{
        parallel::Tasks,
        storage::{StorageValue, Tag},
        testing::{assert_panics, rows},
    };

    /// A buffer of term rows, for tests.
    fn term_buffer(arity: usize, flat: &[u64]) -> RowBuffer {
        let mut buffer = RowBuffer::new(arity);
        for row in flat.chunks(arity) {
            let row: Vec<StorageValue> = row.iter().map(|&id| StorageValue::term(id)).collect();
            buffer.push_row(&row);
        }
        buffer
    }

    /// The payloads of a column's values.
    fn payloads(trie: &Trie, column: usize) -> Vec<u64> {
        let view = trie.column(column);
        (0..view.len())
            .map(|index| view.get(index).payload)
            .collect()
    }

    /// The offsets of a column's intervals, sentinel included.
    fn offsets(trie: &Trie, column: usize) -> Vec<usize> {
        let intervals = trie.intervals(column);
        (0..=intervals.blocks())
            .map(|index| intervals.get(index))
            .collect()
    }

    #[test]
    fn empty_tries_have_empty_columns() {
        for trie in [Trie::empty(2), Trie::from_buffer(RowBuffer::new(2))] {
            assert_eq!(trie.arity(), 2);
            assert!(trie.is_empty());
            assert_eq!(trie.row_count(), 0);
            assert_eq!(trie.column(0).len(), 0);
            assert_eq!(trie.column(1).len(), 0);
            assert!(rows(&trie).is_empty());
        }
    }

    #[test]
    fn shared_prefixes_become_shared_nodes() {
        // Rows (1,2), (1,3), (2,4): the two 1-prefixed rows share a node.
        let trie = Trie::from_buffer(term_buffer(2, &[1, 2, 1, 3, 2, 4]));

        assert_eq!(payloads(&trie, 0), &[1, 2]);
        assert_eq!(offsets(&trie, 0), &[0, 2]);
        assert_eq!(payloads(&trie, 1), &[2, 3, 4]);
        assert_eq!(offsets(&trie, 1), &[0, 2, 3]);
        assert_eq!(trie.row_count(), 3);
        assert!(matches!(trie.column(0), ColumnView::U32 { .. }));
    }

    #[test]
    fn deeper_tries_nest_intervals() {
        let trie = Trie::from_buffer(term_buffer(3, &[1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 2, 4]));

        assert_eq!(payloads(&trie, 0), &[1, 2]);
        assert_eq!(offsets(&trie, 0), &[0, 2]);
        assert_eq!(payloads(&trie, 1), &[1, 2, 2]);
        assert_eq!(offsets(&trie, 1), &[0, 2, 3]);
        assert_eq!(payloads(&trie, 2), &[1, 2, 3, 4]);
        assert_eq!(offsets(&trie, 2), &[0, 2, 3, 4]);
    }

    #[test]
    fn repeated_rows_are_kept_once() {
        let trie = Trie::from_buffer(term_buffer(2, &[1, 2, 1, 2, 1, 2, 3, 4]));

        assert_eq!(trie.row_count(), 2);
        assert_eq!(payloads(&trie, 1), &[2, 4]);
    }

    #[test]
    fn mixed_columns_interleave_tags_per_parent() {
        // Rows: (term 0, int 5), (term 0, term 3), (int 1, term 3).
        let mut buffer = RowBuffer::new(2);
        buffer.push_row(&[StorageValue::term(0), StorageValue::integer(5)]);
        buffer.push_row(&[StorageValue::term(0), StorageValue::term(3)]);
        buffer.push_row(&[StorageValue::integer(1), StorageValue::term(3)]);
        let trie = Trie::from_buffer(buffer);

        // Column 0 mixes term 0 and int 1; terms sort first.
        let column0 = trie.column(0);
        assert_eq!(column0.uniform_tag(), None);
        assert_eq!(column0.get(0), StorageValue::term(0));
        assert_eq!(column0.get(1), StorageValue::integer(1));

        // The children of (term 0) are [term 3, int 5]; the intervals are
        // indexed by the parent node even across parent tags.
        assert_eq!(offsets(&trie, 1), &[0, 2, 3]);
        assert_eq!(trie.column(1).get(0), StorageValue::term(3));
        assert_eq!(trie.column(1).get(1), StorageValue::integer(5));
        assert_eq!(trie.column(1).get(2), StorageValue::term(3));
    }

    #[test]
    fn payloads_beyond_32_bits_widen_the_column() {
        let big = u64::from(u32::MAX) + 1;
        let trie = Trie::from_buffer(term_buffer(1, &[1, big]));

        assert!(matches!(
            trie.column(0),
            ColumnView::U64 { tag: Tag::Term, .. }
        ));
        assert_eq!(payloads(&trie, 0), &[1, big]);
    }

    #[test]
    fn nullary_tries_distinguish_empty_from_present() {
        let empty = Trie::from_buffer(RowBuffer::new(0));
        assert!(empty.is_empty());
        assert!(rows(&empty).is_empty());

        let mut buffer = RowBuffer::new(0);
        buffer.push_row(&[]);
        buffer.push_row(&[]);
        let present = Trie::from_buffer(buffer);
        assert_eq!(present.row_count(), 1);
        assert!(!present.is_empty());
        assert_eq!(rows(&present), vec![Vec::new()]);
    }

    #[test]
    fn merges_need_tries_of_one_arity() {
        let unary = Trie::from_buffer(term_buffer(1, &[1]));
        let binary = Trie::from_buffer(term_buffer(2, &[1, 2]));
        assert_panics("at least one trie", || Trie::merge(&[]));
        assert_panics("share one arity", || Trie::merge(&[&unary, &binary]));
    }

    #[test]
    fn paged_reads_resume_where_they_stopped() {
        let trie = Trie::from_buffer(term_buffer(2, &[1, 2, 1, 3, 2, 4, 3, 5]));
        let all = rows(&trie);

        let mut paged = Vec::new();
        let mut next = 0;
        loop {
            let mut taken = 0;
            trie.for_each_row_from(next, |row| {
                paged.push(row.to_vec());
                taken += 1;
                if taken == 3 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
            if taken < 3 {
                break;
            }
            next += taken;
        }
        assert_eq!(paged, all);
    }

    #[test]
    fn merged_tries_match_concatenate_sort_dedup() {
        // The consolidation property: merging tries equals rebuilding from
        // all their rows, across overlapping sources.
        let mut rng = StdRng::seed_from_u64(31);
        for _ in 0..60 {
            let arity = rng.gen_range(1..=3);
            let sources: Vec<Trie> = (0..rng.gen_range(1..=4))
                .map(|_| {
                    let mut buffer = RowBuffer::new(arity);
                    for _ in 0..rng.gen_range(0..40) {
                        let row: Vec<StorageValue> = (0..arity)
                            .map(|_| match rng.gen_range(0..2) {
                                0 => StorageValue::term(rng.gen_range(0..4)),
                                _ => StorageValue::integer(rng.gen_range(-2..2)),
                            })
                            .collect();
                        buffer.push_row(&row);
                    }
                    Trie::from_buffer(buffer)
                })
                .collect();

            let mut all = RowBuffer::new(arity);
            for source in &sources {
                source.for_each_row(|row| all.push_row(row));
            }
            let expected = Trie::from_buffer(all);

            let refs: Vec<&Trie> = sources.iter().collect();
            let merged = Trie::merge(&refs);
            assert_eq!(merged.row_count(), expected.row_count());
            assert_eq!(rows(&merged), rows(&expected));
            assert_eq!(Trie::count_distinct(&refs), expected.row_count());

            // Cut into shards, the pieces concatenate to the same trie.
            for shards in [2, 3, 7] {
                let sharded =
                    Trie::from_columns(arity, construct::from_tries_in(&refs, Tasks::of(shards)));
                assert_eq!(rows(&sharded), rows(&expected));
                for column in 0..arity {
                    assert_eq!(offsets(&sharded, column), offsets(&merged, column));
                }
            }
        }
    }

    #[test]
    fn random_tries_enumerate_back_to_their_input() {
        // The round-trip property: build from random rows with many repeats,
        // walk the trie, and compare against the sorted distinct input rows.
        let mut rng = StdRng::seed_from_u64(9);
        for _ in 0..50 {
            let arity = rng.gen_range(1..=4);
            let count = rng.gen_range(0..200);

            let mut buffer = RowBuffer::new(arity);
            let mut expected: BTreeSet<Vec<StorageValue>> = BTreeSet::new();
            for _ in 0..count {
                let row: Vec<StorageValue> = (0..arity)
                    .map(|_| match rng.gen_range(0..3) {
                        0 => StorageValue::term(rng.gen_range(0..5)),
                        1 => StorageValue::integer(rng.gen_range(-3..3)),
                        _ => StorageValue::double(f64::from(rng.gen_range(-2..2))),
                    })
                    .collect();
                buffer.push_row(&row);
                expected.insert(row);
            }

            let trie = Trie::from_buffer(buffer);
            assert_eq!(trie.row_count(), expected.len());
            let enumerated = rows(&trie);
            assert_eq!(enumerated, expected.into_iter().collect::<Vec<_>>());
        }
    }
}
