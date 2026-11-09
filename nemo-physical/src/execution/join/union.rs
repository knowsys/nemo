//! The union cursor: one cursor over a column of several blocks.

use std::ops::Range;

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::execution::join::cursor::{Clip, SeekCursor};

/// A cursor over the union of one column in several blocks of an input.
///
/// Each block has a cursor of its own,
/// and the union points at the smallest value that any of them points at.
/// A value that several blocks contain is read once,
/// so the union reads like one sorted column without repeats.
///
/// When the join descends below a value,
/// only the blocks that contain the value are given ranges below it,
/// and the union reads only those.
/// So a block without the value costs nothing below it.
/// A union that reads a single block moves only that block's cursor.
#[derive(Debug)]
pub(crate) struct UnionCursor<'a, C: SeekCursor> {
    /// The cursors over the column of each block, in block order.
    cursors: BumpVec<'a, C>,
    /// The blocks the union reads: those given a range since the ranges were last cleared,
    /// or every block before the first clear.
    reading: BumpVec<'a, usize>,
    /// The value the union points at, which is the smallest value
    /// that the cursors of the read blocks point at, or `None` once all of them are exhausted.
    current: Option<C::Value>,
}

impl<'a, C: SeekCursor> UnionCursor<'a, C> {
    /// Creates the union of the given block cursors,
    /// which reads every block and points at their smallest value.
    pub(crate) fn new(cursors: BumpVec<'a, C>, bump: &'a Bump) -> Self {
        let mut union = Self {
            reading: BumpVec::from_iter_in(0..cursors.len(), bump),
            cursors,
            current: None,
        };
        union.settle();
        union
    }

    /// Withdraws every block from the union,
    /// until [set_range](Self::set_range) gives it a range again.
    #[inline]
    pub(crate) fn clear_ranges(&mut self) {
        self.reading.clear();
    }

    /// Restricts the cursor of one block to a range, and adds the block to those the union reads.
    ///
    /// The union reads the new range from its next [rewind](Self::rewind) on.
    /// A block is added at most once between two clears.
    #[inline]
    pub(crate) fn set_range(&mut self, block: usize, range: Range<usize>) {
        debug_assert!(
            !self.reading.contains(&block),
            "a block is added once per descent"
        );
        self.cursors[block].set_range(range);
        self.reading.push(block);
    }

    /// Clips every block cursor to a range of values.
    pub(crate) fn clip(&mut self, clip: Clip<C::Value>) {
        for cursor in &mut self.cursors {
            cursor.clip(clip);
        }
        self.settle();
    }

    /// Rewinds the cursor of every block the union reads to the start of its range.
    #[inline]
    pub(crate) fn rewind(&mut self) {
        for &block in &self.reading {
            self.cursors[block].rewind();
        }
        self.settle();
    }

    /// Returns whether the cursor of some block the union reads still points at a value.
    #[inline]
    pub(crate) fn has_current(&self) -> bool {
        self.current.is_some()
    }

    /// Returns the value the union points at.
    ///
    /// Panics if the union [has no current value](Self::has_current).
    #[inline]
    pub(crate) fn current_value(&self) -> C::Value {
        self.current
            .expect("the union is read only while it has a current value")
    }

    /// Calls `found` with each read block whose cursor points at the union's value,
    /// and that cursor's position in the block's column, in the order the blocks were added.
    #[inline]
    pub(crate) fn for_each_match(&self, mut found: impl FnMut(usize, usize)) {
        let Some(current) = self.current else {
            return;
        };
        for &block in &self.reading {
            let cursor = &self.cursors[block];
            if cursor.has_current() && cursor.current_value() == current {
                found(block, cursor.position());
            }
        }
    }

    /// Steps past the union's value and returns the next one,
    /// or returns `None` once the cursor of every block the union reads is exhausted.
    #[inline]
    pub(crate) fn next(&mut self) -> Option<C::Value> {
        let current = self.current?;
        if let [block] = *self.reading {
            self.current = self.cursors[block].next();
            return self.current;
        }
        self.move_blocks(|value| value == current, SeekCursor::next)
    }

    /// Moves forward to the first value at or above `target` and returns it,
    /// or returns `None` if no block the union reads has such a value.
    #[inline]
    pub(crate) fn seek(&mut self, target: C::Value) -> Option<C::Value> {
        let current = self.current?;
        if current >= target {
            return Some(current);
        }
        if let [block] = *self.reading {
            self.current = self.cursors[block].seek(target);
            return self.current;
        }
        self.move_blocks(|value| value < target, |cursor| cursor.seek(target))
    }

    /// Moves the cursor of each read block whose value is `stale` by `step`,
    /// and points the union at the smallest value that the cursors then point at.
    #[inline]
    fn move_blocks(
        &mut self,
        stale: impl Fn(C::Value) -> bool,
        mut step: impl FnMut(&mut C) -> Option<C::Value>,
    ) -> Option<C::Value> {
        let mut smallest: Option<C::Value> = None;
        for &block in &self.reading {
            let cursor = &mut self.cursors[block];
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

    /// Points the union at the smallest value that the cursors of the read blocks point at.
    #[inline]
    fn settle(&mut self) {
        self.current = self
            .reading
            .iter()
            .map(|&block| &self.cursors[block])
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

    /// Returns the blocks pointing at the union's value, with their positions.
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

    /// A union reads each value of its blocks once, in order,
    /// and reports every block that contains the value it points at.
    #[test]
    fn unions_read_each_value_once_in_order() {
        let bump = Bump::new();
        let mut cursor = union(&bump, &[&[1, 4, 6], &[2, 4, 7], &[4]]);
        assert_eq!(values(&mut cursor), [1, 2, 4, 6, 7]);
        assert!(!cursor.has_current());
        assert_eq!(cursor.next(), None);

        cursor.rewind();
        assert_eq!(cursor.seek(3), Some(4));
        // Every block that contains the value points at it.
        assert_eq!(matches(&cursor), [(0, 1), (1, 1), (2, 0)]);
        assert_eq!(cursor.seek(5), Some(6));
        // A seek below the union's value leaves it in place.
        assert_eq!(cursor.seek(2), Some(6));
        assert_eq!(matches(&cursor), [(0, 2)]);
        assert_eq!(cursor.seek(8), None);
        assert!(!cursor.has_current());
        assert!(matches(&cursor).is_empty());
    }

    /// After its ranges are cleared, a union reads only the blocks given new ranges.
    #[test]
    fn only_the_blocks_given_ranges_are_read() {
        let bump = Bump::new();
        let mut cursor = union(&bump, &[&[1, 3, 5, 7], &[2, 3, 8], &[4, 6]]);
        cursor.clear_ranges();
        cursor.set_range(2, 0..2);
        cursor.set_range(0, 1..3);
        cursor.rewind();
        assert_eq!(values(&mut cursor), [3, 4, 5, 6]);

        // A union that reads a single block moves only that block's cursor, in seeks too.
        cursor.clear_ranges();
        cursor.set_range(1, 1..3);
        cursor.rewind();
        assert_eq!(matches(&cursor), [(1, 1)]);
        assert_eq!(cursor.seek(4), Some(8));
        assert_eq!(cursor.next(), None);
        assert!(!cursor.has_current());

        // Rewinding reads the blocks given ranges, and no others.
        cursor.rewind();
        assert_eq!(values(&mut cursor), [3, 8]);

        cursor.clear_ranges();
        cursor.rewind();
        assert!(!cursor.has_current());
        assert_eq!(cursor.seek(1), None);
    }

    /// A clip narrows the cursor of every block.
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
