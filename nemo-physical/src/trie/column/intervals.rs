//! The intervals: the navigation part of a trie column,
//! which says where the children of each node of the column above lie.

use std::ops::Range;

/// The block boundaries of a trie column, indexed by the nodes of the column above.
///
/// The values of a column are partitioned into blocks:
/// the children of node `i` of the column above are one block, in storage order.
/// Entry `i` is the offset in this column's values at which that block starts
/// and a final sentinel entry holds the number of values.
/// So the children of node `i` of the above column
/// are the range `get(i)..get(i + 1)` in this column.
///
/// Offsets are stored in the smallest integer type that addresses every value,
/// decided when the column is created.
#[derive(Debug)]
pub(crate) enum Intervals {
    /// Offsets that fit in 32 bits.
    U32(Vec<u32>),
    /// Offsets that need 64 bits.
    U64(Vec<u64>),
}

impl Intervals {
    /// Creates the intervals of a column without values, which has no blocks.
    pub(super) fn empty() -> Self {
        Intervals::U32(Vec::new())
    }

    /// Creates empty intervals in the smallest representation
    /// for offsets up to `values`, the number of values the column will hold,
    /// with room for `blocks` blocks, one per node of the column above.
    pub(in crate::trie) fn with_capacity(values: usize, blocks: usize) -> Self {
        if u32::try_from(values).is_ok() {
            Intervals::U32(Vec::with_capacity(blocks + 1))
        } else {
            Intervals::U64(Vec::with_capacity(blocks + 1))
        }
    }

    /// Returns the offset at the given index:
    /// where the children of that node of the column above start,
    /// or the number of values for the sentinel.
    pub(crate) fn get(&self, index: usize) -> usize {
        match self {
            Intervals::U32(offsets) => offsets[index] as usize,
            Intervals::U64(offsets) => {
                usize::try_from(offsets[index]).expect("offsets address an in-memory vector")
            }
        }
    }

    /// Returns the range of children in this column of the given node of the column above.
    pub(crate) fn children(&self, node: usize) -> Range<usize> {
        self.get(node)..self.get(node + 1)
    }

    /// Returns the node of the column above whose children contain the given position:
    /// the inverse of [children](Self::children) for a position within the values.
    pub(crate) fn parent(&self, position: usize) -> usize {
        // The containing block is the last one starting at or before the
        // position; the blocks partition the values, so it also ends beyond it.
        let beyond = match self {
            Intervals::U32(offsets) => {
                offsets.partition_point(|&offset| offset as usize <= position)
            }
            Intervals::U64(offsets) => offsets.partition_point(|&offset| {
                usize::try_from(offset).expect("offsets address an in-memory vector") <= position
            }),
        };

        beyond - 1
    }

    /// Returns the number of blocks, which is the number of nodes of the column above:
    /// one less than the stored offsets, none for an [empty](Self::empty) column.
    pub(crate) fn blocks(&self) -> usize {
        let stored = match self {
            Intervals::U32(offsets) => offsets.len(),
            Intervals::U64(offsets) => offsets.len(),
        };
        stored.saturating_sub(1)
    }

    /// Appends the start offset of the next block, the children of the next node above,
    /// at most the number of values the intervals were created for.
    pub(in crate::trie) fn push(&mut self, offset: usize) {
        match self {
            Intervals::U32(offsets) => {
                offsets.push(u32::try_from(offset).expect("offsets stay within the bound"));
            }
            Intervals::U64(offsets) => {
                offsets.push(u64::try_from(offset).expect("usize fits u64"));
            }
        }
    }

    /// Gives back the capacity a bound left unused.
    pub(in crate::trie) fn shrink_to_fit(&mut self) {
        match self {
            Intervals::U32(offsets) => offsets.shrink_to_fit(),
            Intervals::U64(offsets) => offsets.shrink_to_fit(),
        }
    }

    /// Returns the allocated bytes of the intervals.
    pub(super) fn memory_bytes(&self) -> usize {
        match self {
            Intervals::U32(offsets) => offsets.capacity() * size_of::<u32>(),
            Intervals::U64(offsets) => offsets.capacity() * size_of::<u64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn navigation_uses_the_sentinel() {
        let mut intervals = Intervals::with_capacity(7, 3);
        for offset in [0, 2, 3, 7] {
            intervals.push(offset);
        }

        assert!(matches!(intervals, Intervals::U32(_)));
        assert_eq!(intervals.blocks(), 3);
        assert_eq!(intervals.children(0), 0..2);
        assert_eq!(intervals.children(1), 2..3);
        assert_eq!(intervals.children(2), 3..7);
        for position in 0..7 {
            let parent = intervals.parent(position);
            assert!(intervals.children(parent).contains(&position));
        }
    }

    #[test]
    fn large_offsets_widen() {
        let large = u32::MAX as usize + 1;
        let mut intervals = Intervals::with_capacity(large, 1);

        intervals.push(0);
        intervals.push(large);

        assert!(matches!(intervals, Intervals::U64(_)));
        assert_eq!(intervals.children(0), 0..large);
        assert_eq!(intervals.parent(large - 1), 0);
    }
}
