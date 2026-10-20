//! The cursors: readers moving forward through a range of one trie column.
//!
//! A cursor is restricted to a range of its column, usually one block,
//! and moves forward through it.
//! Its [seek](SeekCursor::seek) gallops to the first value at or above a target,
//! the step the leapfrog intersection is built from.
//! - A [PayloadCursor] reads the raw payloads of a column whose values share one tag.
//! - A [TaggedCursor] compares full values, tag and payload, over a column of any layout.
//!
//! A cursor can be [clipped](SeekCursor::clip) to a range of values.
//! The clip narrows every block the cursor enters, once per block,
//! so moving through the block never checks it.

use std::{cmp::Ordering, ops::Range};

use crate::{
    storage::{StorageValue, Tag},
    trie::ColumnView,
};

/// A width of payloads, `u32` or `u64`,
/// over which the payload cursors are monomorphized.
pub(crate) trait Payload: Copy + Ord {
    /// Whether this is the 32-bit width.
    const IS_U32: bool;

    /// Returns a payload at this width,
    /// or `None` if it exceeds the width, and thus every payload stored at it.
    fn narrow(payload: u64) -> Option<Self>;

    /// Returns the payload at 64 bits.
    fn widen(self) -> u64;

    /// Returns the payloads of a column stored at this width whose values share one tag,
    /// with the tag; `None` for a column of another layout.
    fn payloads<'a>(column: ColumnView<'a>) -> Option<(&'a [Self], Tag)>;
}

impl Payload for u32 {
    const IS_U32: bool = true;

    fn narrow(payload: u64) -> Option<Self> {
        u32::try_from(payload).ok()
    }

    fn widen(self) -> u64 {
        u64::from(self)
    }

    fn payloads<'a>(column: ColumnView<'a>) -> Option<(&'a [Self], Tag)> {
        match column {
            ColumnView::U32 { values, tag } => Some((values, tag.tag())),
            _ => None,
        }
    }
}

impl Payload for u64 {
    const IS_U32: bool = false;

    fn narrow(payload: u64) -> Option<Self> {
        Some(payload)
    }

    fn widen(self) -> u64 {
        self
    }

    fn payloads<'a>(column: ColumnView<'a>) -> Option<(&'a [Self], Tag)> {
        match column {
            ColumnView::U64 { values, tag } => Some((values, tag)),
            _ => None,
        }
    }
}

/// A range of values a cursor is clipped to:
/// the values from `low` up to `high`, excluded,
/// with `None` for an unbounded side.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clip<V> {
    /// The smallest value in the range.
    pub(crate) low: Option<V>,
    /// The smallest value above the range.
    pub(crate) high: Option<V>,
}

/// A cursor over a range of a sorted trie column.
pub(crate) trait SeekCursor {
    /// The values the cursor reads, ordered like the column.
    type Value: Copy + Ord;

    /// Restricts the cursor to a block, narrowed to the clip if there is one,
    /// and rewinds it; the range may turn out empty.
    fn set_range(&mut self, range: Range<usize>);

    /// Clips the cursor to a range of values,
    /// narrowing its current range and every block it enters from now on.
    ///
    /// The current range must be one sorted block,
    /// as the whole column is for the first column of a trie.
    fn clip(&mut self, clip: Clip<Self::Value>);

    /// Rewinds the cursor to the start of its range.
    fn rewind(&mut self);

    /// Returns whether the cursor points at a value, before the end of its range.
    fn has_current(&self) -> bool;

    /// Returns the value at the cursor;
    /// valid while the cursor [has a current value](Self::has_current).
    fn current_value(&self) -> Self::Value;

    /// Returns the position of the cursor in the column.
    fn position(&self) -> usize;

    /// Steps to the next value and returns it, or returns `None` at the end of the range.
    fn next(&mut self) -> Option<Self::Value>;

    /// Moves forward to the first value at or above `target` and returns it,
    /// or returns `None` if the rest of the range holds no such value.
    fn seek(&mut self, target: Self::Value) -> Option<Self::Value>;
}

/// A cursor over the raw payloads of a column whose values share one tag,
/// monomorphized for the width of the payloads.
///
/// The layer resolves the tag once, up front,
/// so the cursor runs as over an untyped column and never touches a tag.
#[derive(Debug)]
pub(crate) struct PayloadCursor<'a, P> {
    /// The payloads of the column.
    values: &'a [P],
    /// The range the cursor is restricted to.
    interval: Range<usize>,
    /// The position of the cursor in the column.
    position: usize,
    /// The range of values the cursor is clipped to, if any.
    clip: Option<Clip<P>>,
}

impl<'a, P: Payload> PayloadCursor<'a, P> {
    /// Creates a cursor over a whole column.
    pub(crate) fn new(values: &'a [P]) -> Self {
        Self {
            values,
            interval: 0..values.len(),
            position: 0,
            clip: None,
        }
    }

    /// Returns the block narrowed to the clip.
    fn clipped(&self, mut range: Range<usize>) -> Range<usize> {
        if let Some(clip) = self.clip {
            let block = &self.values[range.clone()];

            let end = clip.high.map_or(block.len(), |high| {
                block.partition_point(|&stored| stored < high)
            });
            let start = clip.low.map_or(0, |low| {
                block[..end].partition_point(|&stored| stored < low)
            });

            range = range.start + start..range.start + end;
        }

        range
    }
}

impl<P: Payload> SeekCursor for PayloadCursor<'_, P> {
    type Value = P;

    #[inline]
    fn set_range(&mut self, range: Range<usize>) {
        let range = self.clipped(range);

        self.position = range.start;
        self.interval = range;
    }

    fn clip(&mut self, clip: Clip<P>) {
        self.clip = Some(clip);
        self.set_range(self.interval.clone());
    }

    #[inline]
    fn rewind(&mut self) {
        self.position = self.interval.start;
    }

    #[inline]
    fn has_current(&self) -> bool {
        self.position < self.interval.end
    }

    #[inline]
    fn current_value(&self) -> P {
        self.values[self.position]
    }

    #[inline]
    fn position(&self) -> usize {
        self.position
    }

    #[inline]
    fn next(&mut self) -> Option<P> {
        let next = self.position + 1;
        if next >= self.interval.end {
            return None;
        }

        self.position = next;

        Some(self.values[next])
    }

    #[inline]
    fn seek(&mut self, target: P) -> Option<P> {
        if self.position >= self.interval.end {
            return None;
        }

        let position = gallop(self.values, self.position, self.interval.end, target)?;
        self.position = position;

        Some(self.values[position])
    }
}

/// A cursor comparing full values, tag and payload, over a column of any layout.
///
/// Over a column whose values share one tag,
/// a seek compares the tag once and then searches the raw payloads.
#[derive(Debug)]
pub(crate) struct TaggedCursor<'a> {
    /// The column.
    column: ColumnView<'a>,
    /// The tag the values of the column share; unused for a mixed column.
    tag: Tag,
    /// The range the cursor is restricted to.
    interval: Range<usize>,
    /// The position of the cursor in the column.
    position: usize,
    /// The range of values the cursor is clipped to, if any.
    clip: Option<Clip<StorageValue>>,
}

impl<'a> TaggedCursor<'a> {
    /// Creates a cursor over a whole column.
    pub(crate) fn new(column: ColumnView<'a>) -> Self {
        Self {
            column,
            tag: column.uniform_tag().unwrap_or(Tag::Term),
            interval: 0..column.len(),
            position: 0,
            clip: None,
        }
    }

    /// Returns the value at the given index.
    #[inline]
    fn entry(&self, index: usize) -> StorageValue {
        match self.column {
            ColumnView::U32 { values, .. } => StorageValue {
                tag: self.tag,
                payload: u64::from(values[index]),
            },
            ColumnView::U64 { values, .. } => StorageValue {
                tag: self.tag,
                payload: values[index],
            },
            ColumnView::Mixed { values, tags } => StorageValue {
                tag: tags[index],
                payload: values[index],
            },
        }
    }

    /// Returns the block narrowed to the clip.
    fn clipped(&self, mut range: Range<usize>) -> Range<usize> {
        if let Some(clip) = self.clip {
            let end = clip.high.map_or(range.end, |high| {
                self.column.lower_bound_in(range.clone(), high)
            });
            let start = clip.low.map_or(range.start, |low| {
                self.column.lower_bound_in(range.start..end, low)
            });
            range = start..end;
        }
        range
    }

    /// Seeks in a column whose values share one tag:
    /// the tag is compared once, and the search runs over the raw payloads.
    fn seek_uniform<P: Payload>(
        &mut self,
        values: &[P],
        target: StorageValue,
    ) -> Option<StorageValue> {
        match self.tag.cmp(&target.tag) {
            // Every value in this column exceeds the target.
            Ordering::Greater => return Some(self.entry(self.position)),
            // Every value in this column is below the target.
            Ordering::Less => return None,
            Ordering::Equal => {}
        }

        let narrowed = P::narrow(target.payload)?;
        let position = gallop(values, self.position, self.interval.end, narrowed)?;
        self.position = position;

        Some(StorageValue {
            tag: self.tag,
            payload: values[position].widen(),
        })
    }
}

impl SeekCursor for TaggedCursor<'_> {
    type Value = StorageValue;

    #[inline]
    fn set_range(&mut self, range: Range<usize>) {
        let range = self.clipped(range);
        self.position = range.start;
        self.interval = range;
    }

    fn clip(&mut self, clip: Clip<StorageValue>) {
        self.clip = Some(clip);
        self.set_range(self.interval.clone());
    }

    #[inline]
    fn rewind(&mut self) {
        self.position = self.interval.start;
    }

    #[inline]
    fn has_current(&self) -> bool {
        self.position < self.interval.end
    }

    #[inline]
    fn current_value(&self) -> StorageValue {
        self.entry(self.position)
    }

    #[inline]
    fn position(&self) -> usize {
        self.position
    }

    #[inline]
    fn next(&mut self) -> Option<StorageValue> {
        let next = self.position + 1;
        if next >= self.interval.end {
            return None;
        }
        self.position = next;
        Some(self.entry(next))
    }

    #[inline]
    fn seek(&mut self, target: StorageValue) -> Option<StorageValue> {
        if self.position >= self.interval.end {
            return None;
        }

        match self.column {
            ColumnView::U32 { values, .. } => self.seek_uniform(values, target),
            ColumnView::U64 { values, .. } => self.seek_uniform(values, target),
            ColumnView::Mixed { values, tags } => {
                let end = self.interval.end;
                let entry = |index: usize| StorageValue {
                    tag: tags[index],
                    payload: values[index],
                };

                let position = gallop_by(self.position, end, target, entry)?;
                self.position = position;

                Some(entry(position))
            }
        }
    }
}

/// Returns the first index from `start` up to `end`, excluded,
/// whose value is at or above `target`, or `None` if there is none.
///
/// The search gallops forward in doubling steps until it passes the target,
/// then bisects the last step.
/// Its worst case is that of a binary search,
/// but it finds a value close to the start much faster,
/// and in a leapfrog join the next match usually is.
#[inline]
fn gallop<P: Ord + Copy>(values: &[P], start: usize, end: usize, target: P) -> Option<usize> {
    gallop_by(start, end, target, |index| values[index])
}

/// Returns what [gallop] returns, reading the values through an accessor:
/// for the layouts that store a value in two slices.
#[inline]
fn gallop_by<V: Ord, F: Fn(usize) -> V>(
    start: usize,
    end: usize,
    target: V,
    entry: F,
) -> Option<usize> {
    // The start may already hold a value at or above the target.
    if entry(start) >= target {
        return Some(start);
    }

    // Gallop, keeping entry(low) < target.
    let mut low = start;
    let mut step: usize = 1;
    let high = loop {
        let probe = low + step;

        if probe >= end {
            break end;
        }
        if entry(probe) >= target {
            break probe + 1;
        }

        low = probe;
        step *= 2;
    };

    // Bisect (low, high) for the first entry at or above the target.
    let mut bottom = low + 1;
    let mut top = high;
    while bottom < top {
        let middle = bottom + (top - bottom) / 2;

        if entry(middle) < target {
            bottom = middle + 1;
        } else {
            top = middle;
        }
    }

    (bottom < end).then_some(bottom)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::Tag32;

    /// Returns a cursor over a 64-bit column of terms with the given payloads.
    fn u64_cursor(values: &[u64]) -> TaggedCursor<'_> {
        TaggedCursor::new(ColumnView::U64 {
            values,
            tag: Tag::Term,
        })
    }

    #[test]
    fn seeking_finds_matches_and_gaps() {
        let data = [1, 3, 5, 7, 9];
        let mut cursor = u64_cursor(&data);
        assert_eq!(
            cursor.seek(StorageValue::term(5)),
            Some(StorageValue::term(5))
        );
        assert_eq!(cursor.position(), 2);
        assert_eq!(
            cursor.seek(StorageValue::term(6)),
            Some(StorageValue::term(7))
        );
        assert_eq!(cursor.seek(StorageValue::term(10)), None);
    }

    #[test]
    fn ranges_restrict_the_search() {
        let data = [1, 3, 5, 7, 9];
        let mut cursor = u64_cursor(&data);
        cursor.set_range(1..4);
        assert_eq!(
            cursor.seek(StorageValue::term(4)),
            Some(StorageValue::term(5))
        );
        assert_eq!(cursor.seek(StorageValue::term(8)), None);
        cursor.rewind();
        assert_eq!(cursor.current_value(), StorageValue::term(3));
    }

    #[test]
    fn u32_columns_narrow_the_target() {
        let data: [u32; 3] = [1, 3, 5];
        let mut cursor = TaggedCursor::new(ColumnView::U32 {
            values: &data,
            tag: Tag32::Term,
        });
        assert_eq!(
            cursor.seek(StorageValue::term(4)),
            Some(StorageValue::term(5))
        );
        // A target beyond `u32` exhausts a 32-bit column.
        assert_eq!(
            cursor.seek(StorageValue::term(u64::from(u32::MAX) + 1)),
            None
        );
    }

    #[test]
    fn uniform_tag_comparison_short_circuits() {
        let data = [1, 3, 5];
        let mut terms = u64_cursor(&data);
        // Terms sort below integers: no term can reach an integer target.
        assert_eq!(terms.seek(StorageValue::integer(i64::MIN)), None);

        let payloads = [
            StorageValue::integer(-5).payload,
            StorageValue::integer(7).payload,
        ];
        let mut integers = TaggedCursor::new(ColumnView::U64 {
            values: &payloads,
            tag: Tag::Int,
        });
        // Every integer exceeds every term.
        assert_eq!(
            integers.seek(StorageValue::term(u64::MAX)),
            Some(StorageValue::integer(-5))
        );
    }

    #[test]
    fn mixed_columns_seek_across_tag_groups() {
        let entries = [
            StorageValue::term(2),
            StorageValue::term(7),
            StorageValue::integer(-3),
            StorageValue::integer(5),
        ];
        let values = entries.map(|entry| entry.payload);
        let tags = entries.map(|entry| entry.tag);
        let mut cursor = TaggedCursor::new(ColumnView::Mixed {
            values: &values,
            tags: &tags,
        });

        assert_eq!(
            cursor.seek(StorageValue::term(3)),
            Some(StorageValue::term(7))
        );
        assert_eq!(
            cursor.seek(StorageValue::integer(-10)),
            Some(StorageValue::integer(-3))
        );
        assert_eq!(
            cursor.seek(StorageValue::integer(0)),
            Some(StorageValue::integer(5))
        );
        assert_eq!(cursor.seek(StorageValue::integer(6)), None);
    }

    #[test]
    fn payload_cursors_run_untyped() {
        let data: [u32; 5] = [1, 3, 5, 7, 9];
        let mut cursor = PayloadCursor::new(&data);
        assert_eq!(cursor.seek(4), Some(5));
        assert_eq!(cursor.next(), Some(7));
        assert_eq!(cursor.seek(10), None);
    }

    #[test]
    fn empty_ranges_are_exhausted() {
        let data: [u64; 0] = [];
        let mut cursor = u64_cursor(&data);
        assert!(!cursor.has_current());
        assert_eq!(cursor.seek(StorageValue::term(1)), None);
        assert_eq!(cursor.next(), None);
    }

    #[test]
    fn clips_narrow_every_block_entered() {
        // Two blocks of a deeper column; the clip narrows each block
        // entered to the values in [3, 7).
        let data: [u32; 8] = [1, 3, 5, 7, 2, 4, 6, 8];
        let mut cursor = PayloadCursor::new(&data);
        cursor.set_range(0..4);
        cursor.clip(Clip {
            low: Some(3),
            high: Some(7),
        });
        assert_eq!(cursor.current_value(), 3);
        assert_eq!(cursor.next(), Some(5));
        assert_eq!(cursor.next(), None);
        cursor.set_range(4..8);
        assert_eq!(cursor.current_value(), 4);
        assert_eq!(cursor.next(), Some(6));
        assert_eq!(cursor.next(), None);
        // A block with nothing in range is empty.
        cursor.set_range(3..4);
        assert!(!cursor.has_current());
        assert_eq!(cursor.seek(1), None);

        // Unbounded sides.
        let mut open = u64_cursor(&[1, 3, 5, 7]);
        open.clip(Clip {
            low: None,
            high: Some(StorageValue::term(5)),
        });
        assert_eq!(open.position(), 0);
        assert_eq!(open.seek(StorageValue::term(4)), None);
        let mut open = u64_cursor(&[1, 3, 5, 7]);
        open.clip(Clip {
            low: Some(StorageValue::term(4)),
            high: None,
        });
        assert_eq!(open.current_value(), StorageValue::term(5));
        assert_eq!(open.next(), Some(StorageValue::term(7)));
    }
}
