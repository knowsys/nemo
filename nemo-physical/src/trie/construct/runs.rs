//! The construction from runs: the bounds sampled across them,
//! the window of their rows between two bounds, and the build of a window.

mod chain;

use crate::{
    storage::StorageValue,
    trie::{
        Run,
        column::Column,
        construct::{
            ShardSource,
            builder::{ColumnBuilder, ColumnScan, KindScan, TrieBuilder},
            distinct::Distinct,
            merge,
            runs::chain::RunChain,
        },
    },
};

/// Nonempty runs of one arity, as the source of a trie's columns.
#[derive(Debug)]
pub(super) struct Runs<'a, 'r> {
    /// The number of columns.
    arity: usize,
    /// The runs, none of them empty.
    runs: &'a [Run<'r>],
}

impl<'a, 'r> Runs<'a, 'r> {
    /// Creates the source of nonempty runs of the given arity.
    pub(super) fn new(arity: usize, runs: &'a [Run<'r>]) -> Self {
        debug_assert!(
            !runs.is_empty() && runs.iter().all(|run| !run.is_empty()),
            "shards are cut over nonempty runs"
        );

        Self { arity, runs }
    }

    /// Returns the nonempty pieces of the runs whose first-column value lies in `low..high`,
    /// a side unbounded if `None`.
    fn window(&self, low: Option<StorageValue>, high: Option<StorageValue>) -> Vec<Run<'r>> {
        self.runs
            .iter()
            .filter_map(|run| {
                let start = low.map_or(0, |low| run.lower_bound(low));
                let end = high.map_or(run.len(), |high| run.lower_bound(high));
                (start < end).then(|| run.rows(start..end))
            })
            .collect()
    }
}

impl ShardSource for Runs<'_, '_> {
    fn arity(&self) -> usize {
        self.arity
    }

    /// Samples the first-column values at equal row positions across the runs laid end to end:
    /// balanced for disjoint runs, a sample of the value distribution for overlapping ones.
    fn bounds(&self, shards: usize) -> Vec<StorageValue> {
        let rows: usize = self.runs.iter().map(Run::len).sum();
        let mut bounds: Vec<StorageValue> = (1..shards)
            .map(|shard| {
                let mut position = shard * rows / shards;
                for run in self.runs {
                    if position < run.len() {
                        return run.row(position)[0];
                    }
                    position -= run.len();
                }
                unreachable!("positions stay below the row total")
            })
            .collect();
        bounds.sort_unstable();
        bounds.dedup();
        bounds
    }

    /// Builds the shard from its pieces of the runs.
    ///
    /// Pieces that follow one another are one sorted sequence,
    /// built in a linear pass over exactly sized storage;
    /// otherwise the pieces group into [chains](RunChain) that merge.
    fn build_shard(&self, low: Option<StorageValue>, high: Option<StorageValue>) -> Vec<Column> {
        let mut chains = RunChain::partition(&self.window(low, high));
        if chains.len() == 1 {
            build_sequence(self.arity, &chains.remove(0))
        } else {
            merge_chains(self.arity, chains)
        }
    }
}

/// Builds the column pieces of one sorted sequence:
/// a scan pass sizes each column exactly and finds its layout,
/// so the storage is sized once, and a second pass emits the distinct rows into it.
fn build_sequence(arity: usize, sequence: &RunChain<'_>) -> Vec<Column> {
    let mut scans = vec![ColumnScan::default(); arity];
    let mut distinct = Distinct::borrowed();
    for row in sequence.rows() {
        for (scan, &value) in scans.iter_mut().zip(row) {
            scan.add(value);
        }
        if let Some(change) = distinct.pass(row) {
            for (column, scan) in scans.iter_mut().enumerate() {
                scan.count(change, column);
            }
        }
    }

    let mut builder = TrieBuilder::new(scans.iter().map(ColumnScan::builder));
    let mut distinct = Distinct::borrowed();
    for row in sequence.rows() {
        if let Some(change) = distinct.pass(row) {
            builder.push_row(row, change);
        }
    }
    builder.finish()
}

/// Builds the column pieces of chains that overlap:
/// a scan of the values finds each column's layout, as a repeat changes none,
/// and the chains merge into storage bounded by their row total, trimmed at finish.
fn merge_chains(arity: usize, chains: Vec<RunChain<'_>>) -> Vec<Column> {
    let rows: usize = chains.iter().map(RunChain::len).sum();
    let mut scans = vec![KindScan::default(); arity];
    for row in chains.iter().flat_map(RunChain::rows) {
        for (scan, &value) in scans.iter_mut().zip(row) {
            scan.add(value);
        }
    }

    let mut builder = TrieBuilder::new(scans.iter().enumerate().map(|(column, scan)| {
        let blocks = if column == 0 { 1 } else { rows };
        ColumnBuilder::new(scan.kind(), rows, blocks)
    }));
    merge::merge(chains, arity, |row, change| builder.push_row(row, change));
    builder.finish()
}
