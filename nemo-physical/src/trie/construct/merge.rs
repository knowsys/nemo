//! The merge, which produces the distinct rows of several sorted row streams,
//! each with its change.
//!
//! The sources are kept in a heap ordered by their front rows,
//! so the least unread row is always at the top.
//! Each round takes that row, drops it if a [Distinct] filter finds that it repeats the last
//! row emitted, and advances its source to the next row.
//! Equal rows from different sources come out one after another,
//! led by the row of the source that comes first in the list.
//! So the filter also catches repeats across sources, and keeps the first source's row.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, binary_heap::PeekMut},
};

use crate::{
    storage::StorageValue,
    trie::construct::{builder::RowChange, distinct::Distinct},
};

/// A sorted stream of rows, read one at a time by a [merge].
pub(super) trait RowSource {
    /// Returns the current row.
    ///
    /// It is valid while the source [has one](Self::has_row).
    fn row(&self) -> &[StorageValue];

    /// Returns whether the source still has a row.
    fn has_row(&self) -> bool;

    /// Moves past the current row.
    fn advance(&mut self);
}

/// Visits the distinct rows of the sources in storage order.
///
/// `visit` receives the source that the row was taken from, positioned at the row,
/// and the row's [RowChange].
/// A row that several sources contain is taken from the first of them in the list.
/// The least source advances in place, through the heap's handle to its top.
/// When the handle is released, the source sinks to where its next row belongs,
/// or it is removed if it has no next row.
pub(super) fn merge<S: RowSource>(
    sources: Vec<S>,
    arity: usize,
    mut visit: impl FnMut(&S, RowChange),
) {
    let mut heap: BinaryHeap<Reverse<ByFront<S>>> = sources
        .into_iter()
        .enumerate()
        .filter(|(_, source)| source.has_row())
        .map(|(index, source)| Reverse(ByFront { index, source }))
        .collect();

    let mut distinct = Distinct::copied(arity);
    while let Some(mut top) = heap.peek_mut() {
        let source = &mut top.0.source;
        if let Some(change) = distinct.pass(source.row()) {
            visit(source, change);
        }

        source.advance();
        if !source.has_row() {
            PeekMut::pop(top);
        }
    }
}

/// A source with its index among the sources, for the heap of a [merge].
///
/// It is ordered by its front row and then by the index.
/// Only sources that have a row are compared.
struct ByFront<S> {
    /// The source's index among the sources.
    index: usize,
    /// The source.
    source: S,
}

impl<S: RowSource> PartialEq for ByFront<S> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S: RowSource> Eq for ByFront<S> {}

impl<S: RowSource> PartialOrd for ByFront<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: RowSource> Ord for ByFront<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .row()
            .cmp(other.source.row())
            .then_with(|| self.index.cmp(&other.index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Integer rows of arity 2 as a source, with its index among the sources.
    struct Rows {
        /// The rows.
        rows: Vec<[StorageValue; 2]>,
        /// The index of the current row.
        next: usize,
        /// The source's index among the sources.
        index: usize,
    }

    impl RowSource for Rows {
        fn row(&self) -> &[StorageValue] {
            &self.rows[self.next]
        }

        fn has_row(&self) -> bool {
            self.next < self.rows.len()
        }

        fn advance(&mut self) {
            self.next += 1;
        }
    }

    /// Returns a source of the given rows, with the given index among the sources.
    fn rows(index: usize, rows: &[[i64; 2]]) -> Rows {
        Rows {
            rows: rows
                .iter()
                .map(|row| row.map(StorageValue::integer))
                .collect(),
            next: 0,
            index,
        }
    }

    /// The merge interleaves the sources in storage order and drops repeats,
    /// keeping the row of the first source in the list.
    #[test]
    fn merged_sources_interleave_and_drop_repeats() {
        let sources = vec![
            rows(0, &[[1, 1], [2, 2], [3, 3]]),
            rows(1, &[]),
            rows(2, &[[1, 1], [1, 5], [3, 3]]),
            rows(3, &[[0, 9], [2, 2]]),
        ];
        let mut merged = Vec::new();
        merge(sources, 2, |source, change| {
            merged.push((source.index, source.row().to_vec(), change))
        });

        // A repeated row is taken from the first source that contains it.
        let expected = [
            (3, [0, 9], RowChange::First),
            (0, [1, 1], RowChange::At(0)),
            (2, [1, 5], RowChange::At(1)),
            (0, [2, 2], RowChange::At(0)),
            (0, [3, 3], RowChange::At(0)),
        ];
        assert_eq!(merged.len(), expected.len());
        for ((index, row, change), (expected_index, expected_row, expected_change)) in
            merged.iter().zip(&expected)
        {
            assert_eq!(index, expected_index);
            assert_eq!(row, &expected_row.map(StorageValue::integer));
            assert_eq!(change, expected_change);
        }
    }
}
