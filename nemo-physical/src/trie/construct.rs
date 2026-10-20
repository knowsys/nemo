//! The construction of a trie's columns: from runs, and from tries.
//!
//! Both constructions follow one recipe.
//! The rows are cut into shards at bounds on the first-column value,
//! each shard's column pieces build on a worker straight into column builders,
//! and the pieces concatenate: no row is copied into an intermediate buffer.
//! A cut between distinct first-column values keeps every first-column subtree
//! inside one shard, so the pieces concatenate without a block spanning two.
//! The constructions differ in how they sample the bounds and in how they build a shard:
//! they are the two [shard sources](ShardSource), in [runs] and [tries].
//! Both read rows through a [distinct] filter or a [merge] into the [builder]s;
//! a trie enters a merge as a [row range](RowRange).

mod builder;
mod distinct;
mod merge;
mod range;
mod runs;
mod tries;

use crate::{
    parallel::{self, Tasks},
    storage::StorageValue,
    trie::{
        Run, Trie,
        column::Column,
        construct::{range::RowRange, runs::Runs, tries::Tries},
    },
};

/// A source the columns of a trie are built from, shard by shard: runs, or tries to merge.
///
/// The sources differ in how they sample the bounds and in how they build a shard.
/// [build](Self::build) is the recipe they share.
trait ShardSource: Sync {
    /// Returns the number of columns.
    fn arity(&self) -> usize;

    /// Returns bounds cutting the rows into about the given number of shards:
    /// distinct first-column values, in storage order.
    fn bounds(&self, shards: usize) -> Vec<StorageValue>;

    /// Builds the column pieces of the rows whose first-column value lies in `low..high`,
    /// a side unbounded if `None`.
    fn build_shard(&self, low: Option<StorageValue>, high: Option<StorageValue>) -> Vec<Column>;

    /// Builds the columns, cut into about as many shards as the given tasks:
    /// each shard builds its column pieces on a task, and the pieces concatenate.
    ///
    /// Without bounds there is one shard, unbounded on both sides.
    fn build(&self, tasks: Tasks) -> Vec<Column> {
        let bounds = self.bounds(tasks.count());
        debug_assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "bounds are distinct and in order"
        );
        if bounds.is_empty() {
            return self.build_shard(None, None);
        }

        let pieces = parallel::map_tasks(tasks.over(bounds.len() + 1), |shard| {
            let low = shard.checked_sub(1).map(|before| bounds[before]);
            let high = bounds.get(shard).copied();
            self.build_shard(low, high)
        });
        Column::concat_shards(self.arity(), pieces)
    }
}

/// Builds the columns of a trie of the given positive arity from runs.
pub(super) fn from_runs(arity: usize, runs: &[Run<'_>]) -> Vec<Column> {
    debug_assert!(arity > 0, "nullary tries hold no values to build from");
    debug_assert!(
        runs.iter().all(|run| run.arity() == arity),
        "runs hold rows of the arity"
    );

    let nonempty: Vec<Run<'_>>;
    let runs: &[Run<'_>] = if runs.iter().any(|run| run.is_empty()) {
        nonempty = runs.iter().copied().filter(|run| !run.is_empty()).collect();
        &nonempty
    } else {
        runs
    };
    let rows: usize = runs.iter().map(Run::len).sum();
    if rows == 0 {
        return (0..arity).map(|_| Column::empty()).collect();
    }

    Runs::new(arity, runs).build(Tasks::cut(rows * arity, rows))
}

/// Builds the columns of a trie merging the given tries,
/// at least one and of one positive arity, so that each of their rows is held once.
///
/// The sources stream through cursors straight into the column builders,
/// so the peak memory is the sources plus the builders, never a row buffer.
pub(super) fn from_tries(sources: &[&Trie]) -> Vec<Column> {
    let arity = sources[0].arity();
    debug_assert!(arity > 0, "nullary tries hold no columns to build");
    debug_assert!(
        sources.iter().all(|source| source.arity() == arity),
        "merged tries share one arity"
    );

    let total: usize = sources.iter().map(|source| source.row_count()).sum();
    let distinct = sources
        .iter()
        .map(|source| source.column(0).len())
        .max()
        .unwrap_or(0);
    from_tries_in(sources, Tasks::cut(total * arity, distinct))
}

/// Builds the columns as [from_tries] does,
/// cut into about as many shards as the given tasks.
pub(super) fn from_tries_in(sources: &[&Trie], tasks: Tasks) -> Vec<Column> {
    Tries::new(sources).build(tasks)
}

/// Returns the number of distinct rows across the given tries,
/// at least one and of one positive arity:
/// the merge of [from_tries] without the build.
pub(super) fn count_distinct(sources: &[&Trie]) -> usize {
    let arity = sources[0].arity();
    debug_assert!(arity > 0, "nullary tries hold no rows to merge");

    let sources = sources.iter().map(|source| RowRange::all(source)).collect();

    let mut count = 0;
    merge::merge(sources, arity, |_, _| count += 1);

    count
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::{
        testing::rows,
        trie::{ColumnView, RowBuffer, SortedRowBuffer, Trie},
        tuning::Tuning,
    };

    /// A random row of the arity, from a small value space so that rows repeat.
    fn random_row(rng: &mut StdRng, arity: usize, span: i64) -> Vec<StorageValue> {
        (0..arity)
            .map(|_| StorageValue::integer(rng.gen_range(0..span)))
            .collect()
    }

    /// Sorted buffers of random rows and the distinct rows they hold.
    fn random_runs(
        rng: &mut StdRng,
        arity: usize,
        runs: usize,
        rows: usize,
        span: i64,
    ) -> (Vec<SortedRowBuffer>, Vec<Vec<StorageValue>>) {
        let mut expected = BTreeSet::new();
        let buffers = (0..runs)
            .map(|_| {
                let mut buffer = RowBuffer::new(arity);
                for _ in 0..rng.gen_range(0..=rows) {
                    let row = random_row(rng, arity, span);
                    expected.insert(row.clone());
                    buffer.push_row(&row);
                }
                buffer.into_sorted()
            })
            .collect();
        (buffers, expected.into_iter().collect())
    }

    /// Builds from the buffers as runs and enumerates the trie.
    fn built(arity: usize, buffers: &[SortedRowBuffer]) -> Vec<Vec<StorageValue>> {
        let runs: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        rows(&Trie::from_runs(arity, &runs))
    }

    /// Builds from the buffers as runs cut into the given number of shards,
    /// whatever the machine, and enumerates the trie.
    fn built_in(
        arity: usize,
        buffers: &[SortedRowBuffer],
        shards: usize,
    ) -> Vec<Vec<StorageValue>> {
        let runs: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        let columns = Runs::new(arity, &runs).build(Tasks::of(shards));
        rows(&Trie::from_columns(arity, columns))
    }

    #[test]
    fn overlapping_runs_with_repeats_build_the_distinct_rows() {
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..40 {
            let arity = rng.gen_range(1..=4);
            let runs = rng.gen_range(0..6);
            let (buffers, expected) = random_runs(&mut rng, arity, runs, 40, 4);
            assert_eq!(built(arity, &buffers), expected);
        }
    }

    #[test]
    fn chained_runs_build_without_a_merge() {
        // Disjoint runs in order, with a first-column value spanning
        // two of them and a repeat at their seam.
        let mut a = RowBuffer::new(2);
        let mut b = RowBuffer::new(2);
        for row in [[1, 1], [1, 2], [2, 5]] {
            a.push_row(&row.map(StorageValue::integer));
        }
        for row in [[2, 5], [2, 6], [3, 0]] {
            b.push_row(&row.map(StorageValue::integer));
        }
        let expected: Vec<Vec<StorageValue>> = [[1, 1], [1, 2], [2, 5], [2, 6], [3, 0]]
            .iter()
            .map(|row| row.map(StorageValue::integer).to_vec())
            .collect();
        assert_eq!(built(2, &[a.into_sorted(), b.into_sorted()]), expected);
    }

    #[test]
    fn large_builds_cross_into_shards() {
        // Enough rows to cut into shards on a multi-threaded build, and
        // cut into a fixed number of them on any machine: chained runs, then
        // overlapping ones, each against the distinct rows they hold; the
        // one-run build of the chained runs' concatenation agrees.
        let parallel_work = Tuning::get().parallel_work;
        let mut rng = StdRng::seed_from_u64(5);
        let mut chained = Vec::new();
        let mut all = RowBuffer::new(2);
        let mut expected = BTreeSet::new();
        for run in 0..7i64 {
            let mut buffer = RowBuffer::new(2);
            for _ in 0..10_000 {
                let row = [
                    StorageValue::integer(run * 1000 + rng.gen_range(0..1000)),
                    StorageValue::integer(rng.gen_range(0..50)),
                ];
                buffer.push_row(&row);
                all.push_row(&row);
                expected.insert(row.to_vec());
            }
            chained.push(buffer.into_sorted());
        }
        assert!(all.len() * 2 >= parallel_work, "the build is worth cutting");
        let expected: Vec<Vec<StorageValue>> = expected.into_iter().collect();
        assert_eq!(built(2, &chained), expected);
        for shards in [2, 5, 64] {
            assert_eq!(built_in(2, &chained, shards), expected);
        }
        assert_eq!(rows(&Trie::from_buffer(all)), expected);

        let (overlapping, expected) = random_runs(&mut rng, 3, 5, 30_000, 200);
        let total: usize = overlapping.iter().map(SortedRowBuffer::len).sum();
        assert!(total * 3 >= parallel_work, "the build is worth cutting");
        assert_eq!(built(3, &overlapping), expected);
        for shards in [2, 5, 64] {
            assert_eq!(built_in(3, &overlapping, shards), expected);
        }
    }

    #[test]
    fn shards_agree_on_a_column_layout() {
        // A 32-bit first column in every shard but the last, whose 64-bit
        // payload widens the whole column across the pieces.
        let mut buffer = RowBuffer::new(1);
        for value in 0..100_000u64 {
            buffer.push_row(&[StorageValue::term(value)]);
        }
        buffer.push_row(&[StorageValue::term(u64::from(u32::MAX) + 1)]);
        let buffer = buffer.into_sorted();
        let columns = Runs::new(1, &[buffer.run()]).build(Tasks::of(4));
        let column = columns[0].view();
        assert!(matches!(column, ColumnView::U64 { .. }));
        assert_eq!(column.len(), 100_001);
        assert_eq!(
            column.get(100_000),
            StorageValue::term(u64::from(u32::MAX) + 1)
        );
    }
}
