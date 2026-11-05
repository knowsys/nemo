//! The input cursor: the cursor over one column of a join's input, stored in one block or several.

use std::ops::Range;

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::execution::join::{
    cursor::{Clip, SeekCursor},
    union::UnionCursor,
};

/// The cursor over one column of a join's input.
///
/// An input stored in one block is read by that block's cursor,
/// and an input stored in several by the [union](UnionCursor) of their cursors.
#[derive(Debug)]
pub(crate) enum InputCursor<'a, C: SeekCursor> {
    /// The cursor over the column of the input's only block.
    Block(C),
    /// The union of the cursors over the column of each of the input's blocks.
    Union(UnionCursor<'a, C>),
}

impl<'a, C: SeekCursor> InputCursor<'a, C> {
    /// Creates the cursor over the given block cursors, one per block of the input, in block order.
    pub(crate) fn new(mut blocks: impl ExactSizeIterator<Item = C>, bump: &'a Bump) -> Self {
        if blocks.len() == 1 {
            InputCursor::Block(blocks.next().expect("one block is there"))
        } else {
            InputCursor::Union(UnionCursor::new(BumpVec::from_iter_in(blocks, bump), bump))
        }
    }

    /// Withdraws the ranges of every block, before the blocks holding a value are given new ones.
    ///
    /// The single block of an input holds every value the join descends from,
    /// so it is given its range right after.
    #[inline]
    pub(crate) fn clear_ranges(&mut self) {
        if let InputCursor::Union(union) = self {
            union.clear_ranges();
        }
    }

    /// Restricts the cursor of one block to a range, read from the next [rewind](Self::rewind) on.
    ///
    /// Only a block holding the value the join descends from is given a range,
    /// and that range is never empty, since trie nodes always have children.
    #[inline]
    pub(crate) fn set_range(&mut self, block: usize, range: Range<usize>) {
        debug_assert!(!range.is_empty(), "trie nodes always have children");
        match self {
            InputCursor::Block(cursor) => {
                debug_assert!(block == 0, "an input of one block has only block 0");
                cursor.set_range(range);
            }
            InputCursor::Union(union) => union.set_range(block, range),
        }
    }

    /// Clips the cursor to a range of values, for every block it enters.
    pub(crate) fn clip(&mut self, clip: Clip<C::Value>) {
        match self {
            InputCursor::Block(cursor) => cursor.clip(clip),
            InputCursor::Union(union) => union.clip(clip),
        }
    }

    /// Rewinds the cursor to the start of its range.
    #[inline]
    pub(crate) fn rewind(&mut self) {
        match self {
            InputCursor::Block(cursor) => cursor.rewind(),
            InputCursor::Union(union) => union.rewind(),
        }
    }

    /// Returns whether the cursor points at a value.
    #[inline]
    pub(crate) fn has_current(&self) -> bool {
        match self {
            InputCursor::Block(cursor) => cursor.has_current(),
            InputCursor::Union(union) => union.has_current(),
        }
    }

    /// Returns the value at the cursor;
    /// valid while the cursor [has a current value](Self::has_current).
    #[inline]
    pub(crate) fn current_value(&self) -> C::Value {
        match self {
            InputCursor::Block(cursor) => cursor.current_value(),
            InputCursor::Union(union) => union.current_value(),
        }
    }

    /// Calls `found` with each block holding the value the cursor stands at,
    /// and the position of the value in the block's column.
    #[inline]
    pub(crate) fn for_each_match(&self, mut found: impl FnMut(usize, usize)) {
        match self {
            InputCursor::Block(cursor) => found(0, cursor.position()),
            InputCursor::Union(union) => union.for_each_match(found),
        }
    }

    /// Steps to the next value and returns it, or returns `None` at the end.
    #[inline]
    pub(crate) fn next(&mut self) -> Option<C::Value> {
        match self {
            InputCursor::Block(cursor) => cursor.next(),
            InputCursor::Union(union) => union.next(),
        }
    }

    /// Moves forward to the first value at or above `target` and returns it,
    /// or returns `None` if there is no such value.
    #[inline]
    pub(crate) fn seek(&mut self, target: C::Value) -> Option<C::Value> {
        match self {
            InputCursor::Block(cursor) => cursor.seek(target),
            InputCursor::Union(union) => union.seek(target),
        }
    }
}
