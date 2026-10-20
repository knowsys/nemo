//! The layers of a join: leapfrog intersections of cursors.
//!
//! A layer intersects the input columns mapped to it.
//! Each column is read by a cursor restricted to the block
//! below the values bound at the shallower layers,
//! and the layer's values are those in the range of every cursor.

use std::{cmp::Ordering, ops::Range};

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    execution::join::cursor::{Clip, Payload, PayloadCursor, SeekCursor, TaggedCursor},
    storage::{StorageValue, Tag},
    trie::ColumnView,
};

/// The leapfrog intersection of cursors:
/// the values in the range of every cursor, in ascending order.
///
/// The intersection is monomorphized per kind of cursor,
/// and the positions of the cursors are its whole state, exhaustion included.
/// Every node of a trie has children, so no block is empty,
/// and only a clipped cursor can start on an empty range:
/// emptiness is checked once per restart, not per value.
#[derive(Debug)]
pub(crate) struct Intersection<'a, C> {
    /// The intersected cursors.
    cursors: BumpVec<'a, C>,
    /// The cursor the leapfrog moves next.
    active: usize,
    /// Whether the next step must rewind the cursors first;
    /// set when the layer is entered anew.
    fresh: bool,
}

impl<C: SeekCursor> Intersection<'_, C> {
    /// Creates the intersection of the given cursors.
    ///
    /// There is at least one cursor, and none over an empty column:
    /// a layer over an empty column is [AnyLayer::Empty] instead,
    /// which keeps every range a cursor sees nonempty.
    pub(crate) fn new(cursors: BumpVec<'_, C>) -> Intersection<'_, C> {
        debug_assert!(!cursors.is_empty(), "an intersection needs cursors");
        debug_assert!(
            cursors.iter().all(SeekCursor::has_current),
            "empty columns must be caught at layer construction"
        );

        Intersection {
            cursors,
            active: 0,
            fresh: true,
        }
    }

    /// Replaces the cursors one for one, for a restart,
    /// and returns whether their number matched.
    pub(crate) fn rebind(&mut self, cursors: impl ExactSizeIterator<Item = C>) -> bool {
        if cursors.len() != self.cursors.len() {
            return false;
        }

        for (slot, cursor) in self.cursors.iter_mut().zip(cursors) {
            *slot = cursor;
        }

        self.active = 0;
        self.fresh = true;

        true
    }

    /// Restricts a cursor to a block, rewinding it,
    /// and marks the intersection for a restart.
    #[inline]
    pub(crate) fn set_range(&mut self, cursor: usize, range: Range<usize>) {
        debug_assert!(!range.is_empty(), "trie nodes always have children");

        self.cursors[cursor].set_range(range);
        self.fresh = true;
    }

    /// Clips a cursor to a range of values, for every block it enters,
    /// and marks the intersection for a restart.
    pub(crate) fn clip(&mut self, cursor: usize, clip: Clip<C::Value>) {
        self.cursors[cursor].clip(clip);
        self.fresh = true;
    }

    /// Marks the intersection for a restart at the next step,
    /// when its layer is entered anew.
    #[inline]
    pub(crate) fn mark_for_reset(&mut self) {
        self.fresh = true;
    }

    /// Returns the position of a cursor in its column;
    /// valid after a value was found.
    #[inline]
    pub(crate) fn position(&self, cursor: usize) -> usize {
        self.cursors[cursor].position()
    }

    /// Probes for exactly `target`, and returns it if every cursor holds it,
    /// leaving the cursors on it.
    ///
    /// The probe consumes a pending restart either way.
    /// Cursors only move forward, so a layer is probed at most once per descent.
    #[inline]
    pub(crate) fn seek_exact(&mut self, target: C::Value) -> Option<C::Value> {
        self.restart();

        for cursor in &mut self.cursors {
            if cursor.seek(target)? != target {
                return None;
            }
        }

        Some(target)
    }

    /// Advances to the next value in the range of every cursor and returns it,
    /// or returns `None` if the intersection is exhausted.
    #[inline]
    pub(crate) fn advance(&mut self) -> Option<C::Value> {
        let count = self.cursors.len();
        let mut current = if self.restart() {
            // Only a clipped block can be empty;
            // the seeks below catch an empty range in every other cursor.
            if !self.cursors[0].has_current() {
                return None;
            }
            self.cursors[0].current_value()
        } else {
            self.cursors[self.active].next()?
        };

        let mut matches = 1;
        loop {
            if matches == count {
                return Some(current);
            }

            self.active += 1;
            if self.active == count {
                self.active = 0;
            }

            let value = self.cursors[self.active].seek(current)?;

            if value == current {
                matches += 1;
            } else {
                current = value;
                matches = 1;
            }
        }
    }

    /// Rewinds every cursor whose restart is pending, and returns whether one was.
    #[inline]
    fn restart(&mut self) -> bool {
        if !self.fresh {
            return false;
        }

        self.fresh = false;
        self.active = 0;
        for cursor in &mut self.cursors {
            cursor.rewind();
        }

        true
    }
}

/// A layer of a join: an intersection,
/// with whatever the layout of its columns needs to produce its values.
///
/// The trait lets the join's driver be written once and monomorphized per kind of layer.
/// The uniform layers run over raw payloads:
/// they never widen a payload or touch a tag,
/// and [stamp](Self::stamp) makes a storage value only for an emitted row.
pub(crate) trait JoinLayer<'a> {
    /// The values the layer's leapfrog runs over.
    type Value: Copy + PartialEq;

    /// Advances to the layer's next value and returns it,
    /// or returns `None` if the layer is exhausted.
    fn advance(&mut self) -> Option<Self::Value>;

    /// Probes the layer for exactly the given value, and returns it if the layer holds it.
    ///
    /// A bound layer descends this way, since its value is computed rather than enumerated.
    fn seek_exact(&mut self, value: StorageValue) -> Option<Self::Value>;

    /// Returns the storage value of one of the layer's values.
    fn stamp(&self, value: Self::Value) -> StorageValue;

    /// Marks the layer for a restart, when it is entered anew.
    fn mark_for_reset(&mut self);

    /// Restricts one of the layer's cursors to a block.
    fn set_range(&mut self, cursor: usize, range: Range<usize>);

    /// Clips one of the layer's cursors to the values from `low` up to `high`, excluded,
    /// in storage order; `None` leaves a side unbounded.
    fn clip(&mut self, cursor: usize, low: Option<StorageValue>, high: Option<StorageValue>);

    /// Returns the number of cursors the layer intersects.
    fn cursors(&self) -> usize;

    /// Returns the position of one of the layer's cursors in its column;
    /// valid after a value was found.
    fn position(&self, cursor: usize) -> usize;

    /// Rebinds the layer to other columns of the same layout, one per cursor, for a restart,
    /// and returns whether the columns fit the layer.
    ///
    /// Columns that need a layer of another kind leave the layer unusable.
    fn rebind(&mut self, columns: &[ColumnView<'a>]) -> bool;
}

/// A layer over columns whose values share one tag, all of one payload width.
///
/// The leapfrog runs over the raw payloads,
/// and the tag is stamped onto the emitted rows only.
#[derive(Debug)]
pub(crate) struct UniformLayer<'a, P> {
    /// The intersection of the payloads.
    intersection: Intersection<'a, PayloadCursor<'a, P>>,
    /// The tag the values of every column share.
    tag: Tag,
}

impl<'a, P: Payload> JoinLayer<'a> for UniformLayer<'a, P> {
    type Value = P;

    #[inline]
    fn advance(&mut self) -> Option<P> {
        self.intersection.advance()
    }

    #[inline]
    fn seek_exact(&mut self, value: StorageValue) -> Option<P> {
        if value.tag != self.tag {
            return None;
        }

        self.intersection.seek_exact(P::narrow(value.payload)?)
    }

    #[inline]
    fn stamp(&self, value: P) -> StorageValue {
        StorageValue {
            tag: self.tag,
            payload: value.widen(),
        }
    }

    #[inline]
    fn mark_for_reset(&mut self) {
        self.intersection.mark_for_reset();
    }

    #[inline]
    fn set_range(&mut self, cursor: usize, range: Range<usize>) {
        self.intersection.set_range(cursor, range);
    }

    fn clip(&mut self, cursor: usize, low: Option<StorageValue>, high: Option<StorageValue>) {
        // A bound of another tag lies below or above every value of the layer,
        // and a payload beyond the width lies above them.
        // The side is then unbounded, or the range holds nothing,
        // which a high bound at the smallest payload expresses.
        let nothing = P::narrow(0);
        let mut clip = Clip {
            low: None,
            high: None,
        };

        if let Some(bound) = low {
            match bound.tag.cmp(&self.tag) {
                Ordering::Less => {}
                Ordering::Equal => match P::narrow(bound.payload) {
                    Some(value) => clip.low = Some(value),
                    None => clip.high = nothing,
                },
                Ordering::Greater => clip.high = nothing,
            }
        }
        if let Some(bound) = high {
            match bound.tag.cmp(&self.tag) {
                Ordering::Less => clip.high = nothing,
                Ordering::Equal => {
                    if let Some(value) = P::narrow(bound.payload) {
                        clip.high = Some(clip.high.map_or(value, |high| high.min(value)));
                    }
                }
                Ordering::Greater => {}
            }
        }

        self.intersection.clip(cursor, clip);
    }

    #[inline]
    fn cursors(&self) -> usize {
        self.intersection.cursors.len()
    }

    #[inline]
    fn position(&self, cursor: usize) -> usize {
        self.intersection.position(cursor)
    }

    fn rebind(&mut self, columns: &[ColumnView<'a>]) -> bool {
        let uniform = match classify(columns) {
            LayerKind::Uniform { tag, is_u32 } => tag == self.tag && is_u32 == P::IS_U32,
            LayerKind::Tagged | LayerKind::Empty => false,
        };

        uniform
            && self
                .intersection
                .rebind(columns.iter().map(|&column| payload_cursor(column)))
    }
}

/// A layer over columns of any layout, dispatching on the layout per call.
///
/// A join whose layers are not all 32-bit or all 64-bit runs on these.
#[derive(Debug)]
pub(crate) enum AnyLayer<'a> {
    /// Columns whose values share one tag, all with 32-bit payloads.
    U32(UniformLayer<'a, u32>),
    /// Columns whose values share one tag, all with 64-bit payloads.
    U64(UniformLayer<'a, u64>),
    /// Any other layout: the leapfrog compares full values.
    Tagged(Intersection<'a, TaggedCursor<'a>>),
    /// A layer that provably holds no value:
    /// some column is empty, or the tags of the columns differ.
    Empty,
}

impl<'a> AnyLayer<'a> {
    /// Returns the layer over the given columns.
    ///
    /// The layout of a column holds for every range of it,
    /// so the kind of a layer holds for its lifetime.
    /// A layer over an empty column is empty,
    /// which keeps every range an intersection starts on nonempty.
    pub(crate) fn build(columns: &[ColumnView<'a>], bump: &'a Bump) -> Self {
        match classify(columns) {
            LayerKind::Empty => AnyLayer::Empty,
            LayerKind::Tagged => Self::tagged(columns, bump),
            LayerKind::Uniform { tag, is_u32: true } => AnyLayer::U32(UniformLayer {
                intersection: Intersection::new(Self::payload_cursors::<u32>(columns, bump)),
                tag,
            }),
            LayerKind::Uniform { tag, is_u32: false } => AnyLayer::U64(UniformLayer {
                intersection: Intersection::new(Self::payload_cursors::<u64>(columns, bump)),
                tag,
            }),
        }
    }

    /// Returns the payload cursors over columns classified as uniform at width `P`.
    fn payload_cursors<P: Payload>(
        columns: &[ColumnView<'a>],
        bump: &'a Bump,
    ) -> BumpVec<'a, PayloadCursor<'a, P>> {
        BumpVec::from_iter_in(columns.iter().map(|&column| payload_cursor(column)), bump)
    }

    /// Returns the layer comparing full values over the given columns.
    fn tagged(columns: &[ColumnView<'a>], bump: &'a Bump) -> Self {
        AnyLayer::Tagged(Intersection::new(BumpVec::from_iter_in(
            columns.iter().copied().map(TaggedCursor::new),
            bump,
        )))
    }

    /// Returns the 32-bit uniform layer this is, if it is one.
    pub(crate) fn into_u32(self) -> Option<UniformLayer<'a, u32>> {
        match self {
            AnyLayer::U32(layer) => Some(layer),
            _ => None,
        }
    }

    /// Returns the 64-bit uniform layer this is, if it is one.
    pub(crate) fn into_u64(self) -> Option<UniformLayer<'a, u64>> {
        match self {
            AnyLayer::U64(layer) => Some(layer),
            _ => None,
        }
    }

    /// Returns whether the layer provably holds no value.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, AnyLayer::Empty)
    }
}

impl<'a> JoinLayer<'a> for AnyLayer<'a> {
    type Value = StorageValue;

    fn advance(&mut self) -> Option<StorageValue> {
        match self {
            AnyLayer::U32(layer) => layer.advance().map(|value| layer.stamp(value)),
            AnyLayer::U64(layer) => layer.advance().map(|value| layer.stamp(value)),
            AnyLayer::Tagged(intersection) => intersection.advance(),
            AnyLayer::Empty => None,
        }
    }

    fn seek_exact(&mut self, value: StorageValue) -> Option<StorageValue> {
        match self {
            AnyLayer::U32(layer) => layer.seek_exact(value).map(|found| layer.stamp(found)),
            AnyLayer::U64(layer) => layer.seek_exact(value).map(|found| layer.stamp(found)),
            AnyLayer::Tagged(intersection) => intersection.seek_exact(value),
            AnyLayer::Empty => None,
        }
    }

    fn stamp(&self, value: StorageValue) -> StorageValue {
        value
    }

    fn mark_for_reset(&mut self) {
        match self {
            AnyLayer::U32(layer) => layer.mark_for_reset(),
            AnyLayer::U64(layer) => layer.mark_for_reset(),
            AnyLayer::Tagged(intersection) => intersection.mark_for_reset(),
            AnyLayer::Empty => {}
        }
    }

    fn set_range(&mut self, cursor: usize, range: Range<usize>) {
        match self {
            AnyLayer::U32(layer) => layer.set_range(cursor, range),
            AnyLayer::U64(layer) => layer.set_range(cursor, range),
            AnyLayer::Tagged(intersection) => intersection.set_range(cursor, range),
            AnyLayer::Empty => {}
        }
    }

    fn clip(&mut self, cursor: usize, low: Option<StorageValue>, high: Option<StorageValue>) {
        match self {
            AnyLayer::U32(layer) => layer.clip(cursor, low, high),
            AnyLayer::U64(layer) => layer.clip(cursor, low, high),
            AnyLayer::Tagged(intersection) => intersection.clip(cursor, Clip { low, high }),
            AnyLayer::Empty => {}
        }
    }

    fn cursors(&self) -> usize {
        match self {
            AnyLayer::U32(layer) => layer.cursors(),
            AnyLayer::U64(layer) => layer.cursors(),
            AnyLayer::Tagged(intersection) => intersection.cursors.len(),
            AnyLayer::Empty => 0,
        }
    }

    fn position(&self, cursor: usize) -> usize {
        match self {
            AnyLayer::U32(layer) => layer.position(cursor),
            AnyLayer::U64(layer) => layer.position(cursor),
            AnyLayer::Tagged(intersection) => intersection.position(cursor),
            AnyLayer::Empty => 0,
        }
    }

    fn rebind(&mut self, columns: &[ColumnView<'a>]) -> bool {
        match self {
            AnyLayer::U32(layer) => layer.rebind(columns),
            AnyLayer::U64(layer) => layer.rebind(columns),
            AnyLayer::Tagged(intersection) => {
                classify(columns) == LayerKind::Tagged
                    && intersection.rebind(columns.iter().copied().map(TaggedCursor::new))
            }
            AnyLayer::Empty => false,
        }
    }
}

/// Returns a cursor over the payloads of a column classified as uniform at width `P`.
fn payload_cursor<P: Payload>(column: ColumnView<'_>) -> PayloadCursor<'_, P> {
    let (values, _) = P::payloads(column).expect("the layer's kind was checked");
    PayloadCursor::new(values)
}

/// The kind of layer a set of columns makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayerKind {
    /// Some column is empty, or the tags of the columns differ.
    Empty,
    /// The values of every column share one tag, and the columns share one width.
    Uniform {
        /// The tag every value shares.
        tag: Tag,
        /// Whether the payloads have 32 bits.
        is_u32: bool,
    },
    /// Any other layout: the leapfrog compares full values.
    Tagged,
}

/// Returns the kind of layer the columns make.
fn classify(columns: &[ColumnView<'_>]) -> LayerKind {
    let mut shared: Option<(Tag, bool)> = None;
    let mut same_tag = true;
    let mut same_width = true;
    let mut mixed = false;

    for column in columns {
        let (length, uniform) = match column {
            ColumnView::U32 { values, tag } => (values.len(), Some((tag.tag(), true))),
            ColumnView::U64 { values, tag } => (values.len(), Some((*tag, false))),
            ColumnView::Mixed { values, .. } => (values.len(), None),
        };
        if length == 0 {
            return LayerKind::Empty;
        }
        let Some((tag, is_u32)) = uniform else {
            mixed = true;
            continue;
        };
        match shared {
            None => shared = Some((tag, is_u32)),
            Some((shared_tag, shared_is_u32)) => {
                same_tag &= tag == shared_tag;
                same_width &= is_u32 == shared_is_u32;
            }
        }
    }

    if mixed {
        return LayerKind::Tagged;
    }
    let (tag, is_u32) = shared.expect("a layer intersects at least one column");
    if !same_tag {
        LayerKind::Empty
    } else if !same_width {
        LayerKind::Tagged
    } else {
        LayerKind::Uniform { tag, is_u32 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::Tag32;

    /// Returns a 64-bit column of terms with the given payloads.
    fn uniform(values: &[u64]) -> ColumnView<'_> {
        ColumnView::U64 {
            values,
            tag: Tag::Term,
        }
    }

    /// Returns the values of a layer, advancing it to its end.
    fn collect<'a>(layer: &mut impl JoinLayer<'a, Value = StorageValue>) -> Vec<StorageValue> {
        let mut result = Vec::new();
        while let Some(value) = layer.advance() {
            result.push(value);
        }
        result
    }

    /// Returns the terms with the given payloads.
    fn ids(values: &[u64]) -> Vec<StorageValue> {
        values.iter().map(|&id| StorageValue::term(id)).collect()
    }

    #[test]
    fn intersections_leapfrog_multiple_columns() {
        let bump = Bump::new();
        let a = [1u64, 2, 3, 4, 5];
        let b = [2u64, 3, 5, 7];
        let c = [1u64, 3, 5, 6];
        let mut layer = AnyLayer::build(&[uniform(&a), uniform(&b), uniform(&c)], &bump);
        assert!(matches!(layer, AnyLayer::U64(_)));
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
        // Exhausted layers stay exhausted until reset.
        assert_eq!(layer.advance(), None);
        layer.mark_for_reset();
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }

    #[test]
    fn u32_columns_use_the_u32_core() {
        let bump = Bump::new();
        let a: [u32; 4] = [1, 3, 5, 7];
        let b: [u32; 3] = [3, 5, 8];
        let mut layer = AnyLayer::build(
            &[
                ColumnView::U32 {
                    values: &a,
                    tag: Tag32::Term,
                },
                ColumnView::U32 {
                    values: &b,
                    tag: Tag32::Term,
                },
            ],
            &bump,
        );
        assert!(matches!(layer, AnyLayer::U32(_)));
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }

    #[test]
    fn mismatched_uniform_tags_are_provably_empty() {
        let bump = Bump::new();
        let a = [1u64, 2, 3];
        let b = [StorageValue::integer(1).payload];
        let layer = AnyLayer::build(
            &[
                uniform(&a),
                ColumnView::U64 {
                    values: &b,
                    tag: Tag::Int,
                },
            ],
            &bump,
        );
        assert!(layer.is_empty());
    }

    #[test]
    fn mixed_width_and_mixed_columns_fall_back_to_tagged() {
        let bump = Bump::new();
        let values32 = [1u32, 3, 5];
        let values64 = [3u64, 5, u64::from(u32::MAX) + 5];
        let mut layer = AnyLayer::build(
            &[
                ColumnView::U32 {
                    values: &values32,
                    tag: Tag32::Term,
                },
                ColumnView::U64 {
                    values: &values64,
                    tag: Tag::Term,
                },
            ],
            &bump,
        );
        assert!(matches!(layer, AnyLayer::Tagged(_)));
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }

    #[test]
    fn mixed_columns_intersect_across_tags() {
        let bump = Bump::new();
        let entries_a = [
            StorageValue::term(2),
            StorageValue::integer(-3),
            StorageValue::integer(5),
        ];
        let entries_b = [
            StorageValue::term(2),
            StorageValue::term(4),
            StorageValue::integer(5),
        ];
        let values_a = entries_a.map(|entry| entry.payload);
        let tags_a = entries_a.map(|entry| entry.tag);
        let values_b = entries_b.map(|entry| entry.payload);
        let tags_b = entries_b.map(|entry| entry.tag);

        let mut layer = AnyLayer::build(
            &[
                ColumnView::Mixed {
                    values: &values_a,
                    tags: &tags_a,
                },
                ColumnView::Mixed {
                    values: &values_b,
                    tags: &tags_b,
                },
            ],
            &bump,
        );
        assert_eq!(
            collect(&mut layer),
            vec![StorageValue::term(2), StorageValue::integer(5)]
        );
    }

    #[test]
    fn empty_columns_exhaust_immediately() {
        let bump = Bump::new();
        let a = [1u64, 2];
        let empty: [u64; 0] = [];
        let mut layer = AnyLayer::build(&[uniform(&a), uniform(&empty)], &bump);
        assert_eq!(layer.advance(), None);
    }

    #[test]
    fn ranges_restrict_the_intersection() {
        let bump = Bump::new();
        let a = [1u64, 3, 5, 7];
        let b = [1u64, 3, 5, 7];
        let mut layer = AnyLayer::build(&[uniform(&a), uniform(&b)], &bump);
        layer.set_range(0, 1..3);
        layer.set_range(1, 1..3);
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }
}
