//! Row revisions: the revision at which each row of a subtable was added.

use crate::trie::Trie;

/// The revision at which each row of a subtable was added.
///
/// A revision is stored as its *distance* from the start of the subtable's block,
/// by the row's index in the subtable's first arrangement.
/// The distances are stored in the narrowest width that fits the block's span.
#[derive(Debug)]
pub(super) enum RowRevisions {
    /// Every row was added at the block's start, as in the subtable of one batch.
    Start,
    /// Distances in a block spanning at most 2^8 revisions.
    U8(Vec<u8>),
    /// Distances in a block spanning at most 2^16 revisions.
    U16(Vec<u16>),
    /// Distances in a block spanning at most 2^32 revisions.
    U32(Vec<u32>),
    /// Distances in any block.
    U64(Vec<u64>),
}

impl RowRevisions {
    /// Merges tries as [Trie::merge] does,
    /// and returns the merged trie with the distances of its rows' revisions.
    ///
    /// `span` is the number of revisions that the merged block spans.
    /// `distance(source, row)` is the distance from the merged block's start
    /// of the row with the given index in the given source, and is below `span`.
    /// A row in several sources takes the distance from the first source in the list
    /// that contains it.
    pub(super) fn merge(
        sources: &[&Trie],
        span: u64,
        distance: impl Fn(usize, usize) -> u64 + Sync,
    ) -> (Trie, Self) {
        if span <= 1 << 8 {
            let (trie, distances) = narrowed(sources, distance);
            (trie, Self::U8(distances))
        } else if span <= 1 << 16 {
            let (trie, distances) = narrowed(sources, distance);
            (trie, Self::U16(distances))
        } else if span <= 1 << 32 {
            let (trie, distances) = narrowed(sources, distance);
            (trie, Self::U32(distances))
        } else {
            let (trie, distances) = Trie::merge_carrying(sources, distance);
            (trie, Self::U64(distances))
        }
    }

    /// Returns the distance of a row's revision from the block's start, by the row's index.
    pub(super) fn distance(&self, row: usize) -> u64 {
        match self {
            Self::Start => 0,
            Self::U8(distances) => u64::from(distances[row]),
            Self::U16(distances) => u64::from(distances[row]),
            Self::U32(distances) => u64::from(distances[row]),
            Self::U64(distances) => distances[row],
        }
    }

    /// Returns the allocated bytes of the distances.
    pub(super) fn memory_bytes(&self) -> usize {
        match self {
            Self::Start => 0,
            Self::U8(distances) => distances.capacity(),
            Self::U16(distances) => distances.capacity() * size_of::<u16>(),
            Self::U32(distances) => distances.capacity() * size_of::<u32>(),
            Self::U64(distances) => distances.capacity() * size_of::<u64>(),
        }
    }
}

/// Merges the tries as [Trie::merge_carrying] does, with the distances in the width `T`.
///
/// Panics if a distance does not fit in `T`.
fn narrowed<T: TryFrom<u64> + Send>(
    sources: &[&Trie],
    distance: impl Fn(usize, usize) -> u64 + Sync,
) -> (Trie, Vec<T>) {
    Trie::merge_carrying(sources, |source, row| {
        T::try_from(distance(source, row))
            .ok()
            .expect("the width holds the block's span")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::int_trie;

    /// A merged row keeps the distance from the first source that contains it,
    /// and the distances are stored in the narrowest width that fits the span.
    #[test]
    fn merged_rows_keep_the_earliest_distance_in_the_narrowest_width() {
        let early = int_trie(1, &[[1], [3]]);
        let late = int_trie(1, &[[1], [2]]);
        let distance = |source: usize, row: usize| (source * 10 + row) as u64;

        let (trie, revisions) = RowRevisions::merge(&[&early, &late], 256, distance);
        assert_eq!(trie.row_count(), 3);
        assert!(matches!(revisions, RowRevisions::U8(_)));
        // The rows are 1, 2 and 3. Row 1 is in both sources and keeps its early distance,
        // row 2 is only in the late source, and row 3 only in the early one.
        let distances: Vec<u64> = (0..3).map(|row| revisions.distance(row)).collect();
        assert_eq!(distances, [0, 11, 1]);

        let (_, revisions) = RowRevisions::merge(&[&early, &late], 257, distance);
        assert!(matches!(revisions, RowRevisions::U16(_)));
        let (_, revisions) = RowRevisions::merge(&[&early, &late], 1 << 33, distance);
        assert!(matches!(revisions, RowRevisions::U64(_)));
        assert_eq!(revisions.distance(1), 11);
    }
}
