//! The merge: the distinct rows of several sorted row streams, each with its change.
//!
//! The sources are kept in a heap ordered by their front rows,
//! so the least unread row is always at the top.
//! Each round takes that row, drops it if a [Distinct] filter finds
//! that it is a repeat of the last row emitted,
//! and steps the source it came from to its next row.
//! Equal rows from different sources
//! surface back to back, so the filter catches repeats across sources too.

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
    /// Returns the current row; valid while the source [has one](Self::has_row).
    fn row(&self) -> &[StorageValue];

    /// Returns whether the source still has a row.
    fn has_row(&self) -> bool;

    /// Steps past the current row.
    fn advance(&mut self);
}

/// Visits the distinct rows of the sources in storage order, each with its [RowChange].
///
/// The least source steps in place through the heap's top handle,
/// which sinks it to where its next row belongs when released,
/// or removes it when it has none.
pub(super) fn merge<S: RowSource>(
    sources: Vec<S>,
    arity: usize,
    mut visit: impl FnMut(&[StorageValue], RowChange),
) {
    let mut heap: BinaryHeap<Reverse<ByFront<S>>> = sources
        .into_iter()
        .filter(RowSource::has_row)
        .map(|source| Reverse(ByFront(source)))
        .collect();

    let mut distinct = Distinct::copied(arity);
    while let Some(mut top) = heap.peek_mut() {
        let source = &mut top.0.0;
        if let Some(change) = distinct.pass(source.row()) {
            visit(source.row(), change);
        }

        source.advance();
        if !source.has_row() {
            PeekMut::pop(top);
        }
    }
}

/// A source ordered by its front row, for the heap of a [merge];
/// only sources that have a row are compared.
struct ByFront<S>(S);

impl<S: RowSource> PartialEq for ByFront<S> {
    fn eq(&self, other: &Self) -> bool {
        self.0.row() == other.0.row()
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
        self.0.row().cmp(other.0.row())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A run of integer rows of arity 2, read as a source.
    struct Rows(Vec<[StorageValue; 2]>, usize);

    impl RowSource for Rows {
        fn row(&self) -> &[StorageValue] {
            &self.0[self.1]
        }

        fn has_row(&self) -> bool {
            self.1 < self.0.len()
        }

        fn advance(&mut self) {
            self.1 += 1;
        }
    }

    /// A source of the given rows.
    fn rows(rows: &[[i64; 2]]) -> Rows {
        Rows(
            rows.iter()
                .map(|row| row.map(StorageValue::integer))
                .collect(),
            0,
        )
    }

    #[test]
    fn merged_sources_interleave_and_drop_repeats() {
        let sources = vec![
            rows(&[[1, 1], [2, 2], [3, 3]]),
            rows(&[]),
            rows(&[[1, 1], [1, 5], [3, 3]]),
            rows(&[[0, 9], [2, 2]]),
        ];
        let mut merged = Vec::new();
        merge(sources, 2, |row, change| {
            merged.push((row.to_vec(), change))
        });

        let expected = [
            ([0, 9], RowChange::First),
            ([1, 1], RowChange::At(0)),
            ([1, 5], RowChange::At(1)),
            ([2, 2], RowChange::At(0)),
            ([3, 3], RowChange::At(0)),
        ];
        assert_eq!(merged.len(), expected.len());
        for ((row, change), (expected_row, expected_change)) in merged.iter().zip(&expected) {
            assert_eq!(row, &expected_row.map(StorageValue::integer));
            assert_eq!(change, expected_change);
        }
    }
}
