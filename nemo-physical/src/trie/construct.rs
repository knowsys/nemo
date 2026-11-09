//! The construction of a trie's columns, from runs or from tries.
//!
//! Both constructions follow one recipe:
//! 1. The rows are cut into *shards* at *bounds*, which are first-column values.
//! 2. Each shard builds its *pieces* of the columns on a worker,
//!    directly in column builders, without copying rows into an intermediate buffer.
//! 3. The pieces are concatenated into the columns.
//!
//! A cut between distinct first-column values keeps the subtree of each first-column node
//! inside one shard, so no block below the first column spans two pieces.
//! The two constructions differ in how they sample the bounds and how they build a shard.
//! They are the two [shard sources](ShardSource), in [runs] and [tries].
//! Both pass the rows through a [distinct] filter or a [merge] into the [builder]s,
//! and a trie enters a merge as a [row range](RowRange).

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

/// A source of a trie's columns, built shard by shard: runs, or tries to merge.
///
/// The sources differ in how they sample the bounds and how they build a shard.
/// [build](Self::build) is the recipe that they share.
trait ShardSource: Sync {
    /// Returns the number of columns.
    fn arity(&self) -> usize;

    /// Returns the bounds that cut the rows into about the given number of shards.
    ///
    /// The bounds are distinct first-column values, in storage order.
    fn bounds(&self, shards: usize) -> Vec<StorageValue>;

    /// Builds the column pieces of the rows whose first-column value lies in `low..high`.
    ///
    /// A bound that is `None` leaves its side unbounded.
    fn build_shard(&self, low: Option<StorageValue>, high: Option<StorageValue>) -> Vec<Column>;

    /// Builds the columns in about as many shards as the given tasks.
    ///
    /// Each shard builds its column pieces on a task, and the pieces are concatenated.
    fn build(&self, tasks: Tasks) -> Vec<Column> {
        let pieces = self.map_shards(tasks, |low, high| self.build_shard(low, high));
        Column::concat_shards(self.arity(), pieces)
    }

    /// Runs `build` for each shard, each on a task, and returns the results in shard order.
    ///
    /// The rows are cut into about as many shards as the given tasks.
    /// Without bounds, there is one shard, unbounded on both sides.
    fn map_shards<P: Send>(
        &self,
        tasks: Tasks,
        build: impl Fn(Option<StorageValue>, Option<StorageValue>) -> P + Sync,
    ) -> Vec<P> {
        let bounds = self.bounds(tasks.count());
        debug_assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "bounds are distinct and in order"
        );
        if bounds.is_empty() {
            return vec![build(None, None)];
        }

        parallel::map_tasks(tasks.over(bounds.len() + 1), |shard| {
            let low = shard.checked_sub(1).map(|before| bounds[before]);
            let high = bounds.get(shard).copied();
            build(low, high)
        })
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

/// Builds the columns of the merge of the given tries, which contains each of their rows once.
///
/// There must be at least one trie, and the tries must have one positive arity.
/// The rows of the tries are read through cursors directly into the column builders,
/// so the peak memory is the tries plus the builders, and never a row buffer.
pub(super) fn from_tries(sources: &[&Trie]) -> Vec<Column> {
    let arity = sources[0].arity();
    debug_assert!(arity > 0, "nullary tries hold no columns to build");
    debug_assert!(
        sources.iter().all(|source| source.arity() == arity),
        "merged tries share one arity"
    );

    from_tries_in(sources, tasks_for(sources))
}

/// Returns the tasks for merging the tries.
///
/// The work is the number of values of the tries,
/// and there are at most as many tasks as the longest first column has values.
fn tasks_for(sources: &[&Trie]) -> Tasks {
    let total: usize = sources.iter().map(|source| source.row_count()).sum();
    let distinct = sources
        .iter()
        .map(|source| source.column(0).len())
        .max()
        .unwrap_or(0);
    Tasks::cut(total * sources[0].arity(), distinct)
}

/// Builds the columns as [from_tries] does, in about as many shards as the given tasks.
pub(super) fn from_tries_in(sources: &[&Trie], tasks: Tasks) -> Vec<Column> {
    Tries::new(sources).build(tasks)
}

/// Builds the columns as [from_tries] does, and a value for each row of the merge.
///
/// The value of a row is `value(source, row)`,
/// where `source` is the first source in the list that contains the row,
/// and `row` is the row's index in that source.
/// The values are returned in the merge's row order.
pub(super) fn from_tries_carrying<T: Send>(
    sources: &[&Trie],
    value: impl Fn(usize, usize) -> T + Sync,
) -> (Vec<Column>, Vec<T>) {
    let arity = sources[0].arity();
    debug_assert!(arity > 0, "nullary tries hold no columns to build");

    let tries = Tries::new(sources);
    let pieces = tries.map_shards(tasks_for(sources), |low, high| {
        let window = tries.window(low, high);
        let mut values = Vec::with_capacity(window.rows());
        let columns = tries.merge_window(window, |range| {
            values.push(value(range.source_index(), range.index()))
        });
        (columns, values)
    });
    let (columns, mut values): (Vec<Vec<Column>>, Vec<Vec<T>>) = pieces.into_iter().unzip();
    let carried = if values.len() == 1 {
        values.pop().expect("one shard")
    } else {
        let mut carried = Vec::with_capacity(values.iter().map(Vec::len).sum());
        for piece in values {
            carried.extend(piece);
        }
        carried
    };
    (Column::concat_shards(arity, columns), carried)
}

/// Returns the number of distinct rows across the given tries, without building their merge.
///
/// There must be at least one trie, and the tries must have one positive arity.
pub(super) fn count_distinct(sources: &[&Trie]) -> usize {
    let arity = sources[0].arity();
    debug_assert!(arity > 0, "nullary tries hold no rows to merge");

    let sources = sources
        .iter()
        .enumerate()
        .map(|(index, source)| RowRange::all(source, index))
        .collect();

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

    /// Returns a random row of the arity, with values below `span` so that rows repeat.
    fn random_row(rng: &mut StdRng, arity: usize, span: i64) -> Vec<StorageValue> {
        (0..arity)
            .map(|_| StorageValue::integer(rng.gen_range(0..span)))
            .collect()
    }

    /// Returns sorted buffers of random rows, and the distinct rows that they contain.
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

    /// Builds a trie from the buffers as runs, and returns its rows.
    fn built(arity: usize, buffers: &[SortedRowBuffer]) -> Vec<Vec<StorageValue>> {
        let runs: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        rows(&Trie::from_runs(arity, &runs))
    }

    /// Builds a trie from the buffers as runs, in the given number of shards on any machine,
    /// and returns its rows.
    fn built_in(
        arity: usize,
        buffers: &[SortedRowBuffer],
        shards: usize,
    ) -> Vec<Vec<StorageValue>> {
        let runs: Vec<Run<'_>> = buffers.iter().map(SortedRowBuffer::run).collect();
        let columns = Runs::new(arity, &runs).build(Tasks::of(shards));
        rows(&Trie::from_columns(arity, columns))
    }

    /// Overlapping runs with repeated rows build the trie of their distinct rows.
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

    /// Runs that follow one another build the trie of their distinct rows,
    /// although a first-column value is in both runs and a row repeats where they meet.
    #[test]
    fn chained_runs_build_without_a_merge() {
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

    /// Builds large enough to be cut into shards give the distinct rows,
    /// for runs that follow one another and for overlapping runs, in any number of shards.
    #[test]
    fn large_builds_cross_into_shards() {
        // The builds are large enough to be cut into shards on a machine with several threads,
        // and they are also cut into fixed numbers of shards on any machine.
        // The build of all rows of the runs that follow one another, as one run, agrees.
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

    /// A 64-bit payload in one shard gives the whole column the 64-bit layout.
    #[test]
    fn shards_agree_on_a_column_layout() {
        // Every shard but the last has a 32-bit first column,
        // and the last shard has a payload beyond 32 bits.
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
