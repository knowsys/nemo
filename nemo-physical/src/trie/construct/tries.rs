//! The construction from tries: the bounds sampled from their first columns,
//! and the build of the rows between two bounds.

use crate::{
    storage::StorageValue,
    trie::{
        ColumnView, Trie,
        column::Column,
        construct::{
            ShardSource,
            builder::{ColumnBuilder, ColumnKind, TrieBuilder},
            merge::{self, RowSource},
            range::RowRange,
        },
    },
};

/// Tries of one positive arity to merge, as the source of a trie's columns.
#[derive(Debug)]
pub(super) struct Tries<'s, 't> {
    /// The tries, at least one.
    sources: &'s [&'t Trie],
    /// The kind of each column of the merge, decided from the tries' columns.
    kinds: Vec<ColumnKind>,
}

impl<'s, 't> Tries<'s, 't> {
    /// Creates the source merging the given tries, at least one and of one positive arity.
    pub(super) fn new(sources: &'s [&'t Trie]) -> Self {
        let kinds = (0..sources[0].arity())
            .map(|column| ColumnKind::merged(sources.iter().map(|source| source.column(column))))
            .collect();

        Self { sources, kinds }
    }

    /// Returns the window of the sources' rows whose first-column value lies in `low..high`.
    pub(super) fn window(
        &self,
        low: Option<StorageValue>,
        high: Option<StorageValue>,
    ) -> Window<'t> {
        Window::of(self.sources, self.kinds.len(), low, high)
    }

    /// Merges the rows of a window into one column piece per position,
    /// and reports each merged row to `carry` by the range it was taken from,
    /// that of the earliest source holding it.
    pub(super) fn merge_window(
        &self,
        window: Window<'_>,
        mut carry: impl FnMut(&RowRange<'_>),
    ) -> Vec<Column> {
        let arity = self.kinds.len();
        let mut builder = TrieBuilder::new((0..arity).map(|column| {
            let blocks = if column == 0 {
                1
            } else {
                window.values[column - 1]
            };
            ColumnBuilder::new(self.kinds[column], window.values[column], blocks)
        }));
        merge::merge(window.ranges, arity, |range, change| {
            builder.push_row(range.row(), change);
            carry(range);
        });
        builder.finish()
    }
}

impl ShardSource for Tries<'_, '_> {
    fn arity(&self) -> usize {
        self.kinds.len()
    }

    /// Samples the values of the longest first column at equal position splits,
    /// which is sorted, so the sample is.
    fn bounds(&self, shards: usize) -> Vec<StorageValue> {
        let longest = self
            .sources
            .iter()
            .map(|source| source.column(0))
            .max_by_key(ColumnView::len)
            .expect("a merge covers at least one trie");
        let mut bounds: Vec<StorageValue> = (1..shards)
            .filter(|_| !longest.is_empty())
            .map(|shard| longest.get(shard * longest.len() / shards))
            .collect();
        bounds.dedup();
        bounds
    }

    /// Merges the sources' rows of the shard into one column piece per position.
    fn build_shard(&self, low: Option<StorageValue>, high: Option<StorageValue>) -> Vec<Column> {
        self.merge_window(self.window(low, high), |_| {})
    }
}

/// The rows of the sources between two bounds: one row range per source that has any,
/// and per column the number of values those ranges hold, which bounds the merged pieces.
///
/// A trie lays subtrees out in order, so in each source the window is a range
/// of first-column positions and, beneath it, one range of positions per column.
#[derive(Debug)]
pub(super) struct Window<'a> {
    /// The rows of each source in the window, sources without any left out.
    ranges: Vec<RowRange<'a>>,
    /// The number of values across the sources, per column.
    values: Vec<usize>,
}

impl<'a> Window<'a> {
    /// Returns the number of rows across the ranges, which bounds the rows they merge into.
    pub(super) fn rows(&self) -> usize {
        self.values.last().copied().unwrap_or(0)
    }

    /// Finds the window of the sources' rows whose first-column value lies in `low..high`.
    fn of(
        sources: &[&'a Trie],
        arity: usize,
        low: Option<StorageValue>,
        high: Option<StorageValue>,
    ) -> Self {
        let mut ranges = Vec::with_capacity(sources.len());
        let mut values = vec![0usize; arity];
        for (index, source) in sources.iter().enumerate() {
            let first = source.column(0);
            let start = low.map_or(0, |low| first.lower_bound(low));
            let end = high.map_or(first.len(), |high| first.lower_bound(high));
            if start >= end {
                continue;
            }

            let mut range = start..end;
            values[0] += range.len();
            for (column, count) in values.iter_mut().enumerate().skip(1) {
                let intervals = source.intervals(column);
                range = intervals.get(range.start)..intervals.get(range.end);
                *count += range.len();
            }
            ranges.push(RowRange::between(source, range, index));
        }
        Self { ranges, values }
    }
}
