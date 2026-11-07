//! The union cursor: one cursor over a column of several blocks.

use std::ops::Range;

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::execution::join::cursor::{Clip, SeekCursor};

/// A cursor over the union of one column in several blocks of an input.
///
/// Each block has a cursor of its own,
/// and the union stands at the smallest value any of them stands at.
/// A value that several blocks hold is read once,
/// so the union reads like one sorted column without repeats.
///
/// When the join descends below a value,
/// only the blocks holding it are given ranges below it, and the union reads only those:
/// a block lacking the value costs nothing in its subtree.
/// A union reading a single block moves that block's cursor alone.
#[derive(Debug)]
pub(crate) struct UnionCursor<'a, C: SeekCursor> {
    /// The cursors over the column of each block, in block order.
    blocks: BumpVec<'a, C>,
    /// The blocks the union reads: those given a range since the ranges were last cleared,
    /// or every block before the first clear.
    entered: BumpVec<'a, usize>,
    /// The value the union stands at, the smallest one among the entered block cursors;
    /// `None` once every one of them is exhausted.
    current: Option<C::Value>,
}

impl<'a, C: SeekCursor> UnionCursor<'a, C> {
    /// Creates the union of the given block cursors, reading every block,
    /// and standing at their smallest value.
    pub(crate) fn new(blocks: BumpVec<'a, C>, bump: &'a Bump) -> Self {
        let mut union = Self {
            entered: BumpVec::from_iter_in(0..blocks.len(), bump),
            blocks,
            current: None,
        };
        union.settle();
        union
    }

    /// Withdraws every block from the union,
    /// until [set_range](Self::set_range) gives it a range again.
    #[inline]
    pub(crate) fn clear_ranges(&mut self) {
        self.entered.clear();
    }

    /// Restricts the cursor of one block to a range, and enters the block into the union;
    /// the union takes the change into account at its next [rewind](Self::rewind).
    ///
    /// A block is entered at most once between two clears.
    #[inline]
    pub(crate) fn set_range(&mut self, block: usize, range: Range<usize>) {
        debug_assert!(
            !self.entered.contains(&block),
            "a block is entered once per descent"
        );
        self.blocks[block].set_range(range);
        self.entered.push(block);
    }

    /// Clips every block cursor to a range of values.
    pub(crate) fn clip(&mut self, clip: Clip<C::Value>) {
        for cursor in &mut self.blocks {
            cursor.clip(clip);
        }
        self.settle();
    }

    /// Rewinds the cursor of every entered block to the start of its range.
    #[inline]
    pub(crate) fn rewind(&mut self) {
        for &block in &self.entered {
            self.blocks[block].rewind();
        }
        self.settle();
    }

    /// Returns whether some entered block cursor still points at a value.
    #[inline]
    pub(crate) fn has_current(&self) -> bool {
        self.current.is_some()
    }

    /// Returns the value the union stands at;
    /// valid while the union [has a current value](Self::has_current).
    #[inline]
    pub(crate) fn current_value(&self) -> C::Value {
        self.current
            .expect("the union is read only while it has a current value")
    }

    /// Calls `found` with each entered block whose cursor stands at the union's value,
    /// and that cursor's position in the block's column, in the order the blocks were entered.
    #[inline]
    pub(crate) fn for_each_match(&self, mut found: impl FnMut(usize, usize)) {
        let Some(current) = self.current else {
            return;
        };
        for &block in &self.entered {
            let cursor = &self.blocks[block];
            if cursor.has_current() && cursor.current_value() == current {
                found(block, cursor.position());
            }
        }
    }

    /// Steps past the union's value and returns the next one,
    /// or returns `None` once every entered block cursor is exhausted.
    #[inline]
    pub(crate) fn next(&mut self) -> Option<C::Value> {
        let current = self.current?;
        if let [block] = *self.entered {
            self.current = self.blocks[block].next();
            return self.current;
        }
        self.move_blocks(|value| value == current, SeekCursor::next)
    }

    /// Moves forward to the first value at or above `target` and returns it,
    /// or returns `None` if no entered block holds such a value.
    #[inline]
    pub(crate) fn seek(&mut self, target: C::Value) -> Option<C::Value> {
        let current = self.current?;
        if current >= target {
            return Some(current);
        }
        if let [block] = *self.entered {
            self.current = self.blocks[block].seek(target);
            return self.current;
        }
        self.move_blocks(|value| value < target, |cursor| cursor.seek(target))
    }

    /// Moves each entered block cursor whose value is `stale` by `step`,
    /// and stands the union at the smallest value the entered block cursors then stand at.
    #[inline]
    fn move_blocks(
        &mut self,
        stale: impl Fn(C::Value) -> bool,
        mut step: impl FnMut(&mut C) -> Option<C::Value>,
    ) -> Option<C::Value> {
        let mut smallest: Option<C::Value> = None;
        for &block in &self.entered {
            let cursor = &mut self.blocks[block];
            if !cursor.has_current() {
                continue;
            }

            let mut value = cursor.current_value();
            if stale(value) {
                let Some(moved) = step(cursor) else {
                    continue;
                };
                value = moved;
            }

            smallest = Some(smallest.map_or(value, |least| least.min(value)));
        }

        self.current = smallest;
        smallest
    }

    /// Stands the union at the smallest value among the entered block cursors.
    #[inline]
    fn settle(&mut self) {
        self.current = self
            .entered
            .iter()
            .map(|&block| &self.blocks[block])
            .filter(|cursor| cursor.has_current())
            .map(SeekCursor::current_value)
            .min();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bumpalo::Bump;

    use crate::execution::join::cursor::PayloadCursor;

    /// Returns the union of cursors over the given columns.
    fn union<'a>(bump: &'a Bump, columns: &[&'a [u32]]) -> UnionCursor<'a, PayloadCursor<'a, u32>> {
        UnionCursor::new(
            BumpVec::from_iter_in(
                columns.iter().map(|&column| PayloadCursor::new(column)),
                bump,
            ),
            bump,
        )
    }

    /// Returns the blocks standing at the union's value, with their positions.
    fn matches(cursor: &UnionCursor<'_, PayloadCursor<'_, u32>>) -> Vec<(usize, usize)> {
        let mut matches = Vec::new();
        cursor.for_each_match(|block, position| matches.push((block, position)));
        matches
    }

    /// Returns the values of a union from its current one on, stepping it to its end.
    fn values(cursor: &mut UnionCursor<'_, PayloadCursor<'_, u32>>) -> Vec<u32> {
        let mut values = Vec::new();
        if cursor.has_current() {
            values.push(cursor.current_value());
            while let Some(value) = cursor.next() {
                values.push(value);
            }
        }
        values
    }

    #[test]
    fn unions_read_each_value_once_in_order() {
        let bump = Bump::new();
        let mut cursor = union(&bump, &[&[1, 4, 6], &[2, 4, 7], &[4]]);
        assert_eq!(values(&mut cursor), [1, 2, 4, 6, 7]);
        assert!(!cursor.has_current());
        assert_eq!(cursor.next(), None);

        cursor.rewind();
        assert_eq!(cursor.seek(3), Some(4));
        // Every block holding the value stands at it.
        assert_eq!(matches(&cursor), [(0, 1), (1, 1), (2, 0)]);
        assert_eq!(cursor.seek(5), Some(6));
        // A seek below the union's value leaves it in place.
        assert_eq!(cursor.seek(2), Some(6));
        assert_eq!(matches(&cursor), [(0, 2)]);
        assert_eq!(cursor.seek(8), None);
        assert!(!cursor.has_current());
        assert!(matches(&cursor).is_empty());
    }

    #[test]
    fn only_entered_blocks_are_read() {
        let bump = Bump::new();
        let mut cursor = union(&bump, &[&[1, 3, 5, 7], &[2, 3, 8], &[4, 6]]);
        cursor.clear_ranges();
        cursor.set_range(2, 0..2);
        cursor.set_range(0, 1..3);
        cursor.rewind();
        assert_eq!(values(&mut cursor), [3, 4, 5, 6]);

        // A single entered block is read alone, seeks included.
        cursor.clear_ranges();
        cursor.set_range(1, 1..3);
        cursor.rewind();
        assert_eq!(matches(&cursor), [(1, 1)]);
        assert_eq!(cursor.seek(4), Some(8));
        assert_eq!(cursor.next(), None);
        assert!(!cursor.has_current());

        // Rewinding restores the entered blocks, and no others.
        cursor.rewind();
        assert_eq!(values(&mut cursor), [3, 8]);

        cursor.clear_ranges();
        cursor.rewind();
        assert!(!cursor.has_current());
        assert_eq!(cursor.seek(1), None);
    }

    #[test]
    fn clips_narrow_every_block() {
        let bump = Bump::new();
        let mut cursor = union(&bump, &[&[1, 3, 5, 7], &[2, 4, 6, 8]]);
        cursor.clip(Clip {
            low: Some(3),
            high: Some(7),
        });
        assert_eq!(values(&mut cursor), [3, 4, 5, 6]);
    }
}
