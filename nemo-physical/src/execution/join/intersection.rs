//! The layers of a join: leapfrog intersections of cursors.
//!
//! A layer intersects the input columns mapped to it.
//! Each column is read by a cursor restricted to the children of the node
//! that the values chosen at the shallower layers lead to,
//! and the layer's values are those in the range of every cursor.
//! The column of an input stored in several blocks is read by the union of the blocks' cursors.

use std::{cmp::Ordering, ops::Range};

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    execution::join::{
        cursor::{Clip, Payload, PayloadCursor, SeekCursor, TaggedCursor},
        input::InputCursor,
    },
    storage::{StorageValue, Tag},
    trie::ColumnView,
};

/// The leapfrog intersection of cursors:
/// the values in the range of every cursor, in ascending order.
///
/// The intersection is monomorphized per kind of cursor,
/// and the positions of the cursors are its whole state, exhaustion included.
/// Every node of a trie has children, so no cursor starts on an empty range
/// unless it is clipped.
/// So emptiness is checked once per restart, not per value.
#[derive(Debug)]
pub(crate) struct Intersection<'a, C: SeekCursor> {
    /// The intersected cursors, one per input column.
    cursors: BumpVec<'a, InputCursor<'a, C>>,
    /// The cursor the leapfrog moves next.
    active: usize,
    /// Whether the next step must rewind the cursors first,
    /// which is set when the layer is entered again.
    fresh: bool,
}

impl<'a, C: SeekCursor> Intersection<'a, C> {
    /// Creates the intersection of the given cursors.
    ///
    /// There must be at least one cursor, and none over an empty column.
    /// A layer over an empty column is [AnyLayer::Empty] instead,
    /// which keeps every range a cursor sees nonempty.
    pub(crate) fn new(cursors: BumpVec<'a, InputCursor<'a, C>>) -> Self {
        debug_assert!(!cursors.is_empty(), "an intersection needs cursors");
        debug_assert!(
            cursors.iter().all(InputCursor::has_current),
            "empty columns must be caught at layer construction"
        );

        Intersection {
            cursors,
            active: 0,
            fresh: true,
        }
    }

    /// Withdraws the ranges of every block of a cursor's input,
    /// and marks the intersection for a restart.
    #[inline]
    pub(crate) fn clear_ranges(&mut self, cursor: usize) {
        self.cursors[cursor].clear_ranges();
        self.fresh = true;
    }

    /// Restricts the cursor of one block of a cursor's input to a range,
    /// and marks the intersection for a restart.
    #[inline]
    pub(crate) fn set_range(&mut self, cursor: usize, block: usize, range: Range<usize>) {
        self.cursors[cursor].set_range(block, range);
        self.fresh = true;
    }

    /// Clips a cursor to a range of values, for every range of children it enters,
    /// and marks the intersection for a restart.
    pub(crate) fn clip(&mut self, cursor: usize, clip: Clip<C::Value>) {
        self.cursors[cursor].clip(clip);
        self.fresh = true;
    }

    /// Marks the intersection for a restart at the next step,
    /// when its layer is entered again.
    #[inline]
    pub(crate) fn mark_for_reset(&mut self) {
        self.fresh = true;
    }

    /// Calls `found` with each block of a cursor's input that contains the value found,
    /// and the position of the value in the block's column.
    ///
    /// It may be called only after the intersection found a value.
    #[inline]
    pub(crate) fn for_each_match(&self, cursor: usize, found: impl FnMut(usize, usize)) {
        self.cursors[cursor].for_each_match(found);
    }

    /// Probes for exactly `target`, and returns it if every cursor has it,
    /// leaving the cursors on it.
    ///
    /// The probe performs a pending restart in any case.
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
            // Only a clipped range of children can be empty.
            // The seeks below catch an empty range in every other cursor.
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

    /// Rewinds every cursor if a restart is pending, and returns whether one was.
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
/// The uniform layers work on raw payloads.
/// They never widen a payload or read a tag,
/// and [stamp](Self::stamp) makes a storage value only for an emitted row.
pub(crate) trait JoinLayer<'a> {
    /// The values the layer's intersection works on.
    type Value: Copy + PartialEq;

    /// Advances to the layer's next value and returns it,
    /// or returns `None` if the layer is exhausted.
    fn advance(&mut self) -> Option<Self::Value>;

    /// Probes the layer for exactly the given value, and returns it if the layer has it.
    ///
    /// The driver enters a bound layer this way, since its value is given, not enumerated.
    fn seek_exact(&mut self, value: StorageValue) -> Option<Self::Value>;

    /// Returns the storage value of one of the layer's values.
    fn stamp(&self, value: Self::Value) -> StorageValue;

    /// Marks the layer for a restart, when it is entered again.
    fn mark_for_reset(&mut self);

    /// Withdraws the ranges of every block of a cursor's input,
    /// before the blocks that contain a value are given new ones.
    fn clear_ranges(&mut self, cursor: usize);

    /// Restricts the cursor of one block of a cursor's input to a range of its column.
    fn set_range(&mut self, cursor: usize, block: usize, range: Range<usize>);

    /// Clips one of the layer's cursors to the values from `low` up to `high`, excluded,
    /// in storage order.
    ///
    /// `None` leaves a side unbounded.
    fn clip(&mut self, cursor: usize, low: Option<StorageValue>, high: Option<StorageValue>);

    /// Returns the number of cursors the layer intersects.
    fn cursors(&self) -> usize;

    /// Calls `found` with each block of a cursor's input that contains the value found,
    /// and the position of the value in the block's column.
    ///
    /// It may be called only after the layer found a value.
    fn for_each_match(&self, cursor: usize, found: impl FnMut(usize, usize));
}

/// A layer over columns whose values all share one tag, and which have one payload width.
///
/// The intersection works on the raw payloads,
/// and the tag is added to the emitted rows only.
#[derive(Debug)]
pub(crate) struct UniformLayer<'a, P: Payload> {
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
    fn clear_ranges(&mut self, cursor: usize) {
        self.intersection.clear_ranges(cursor);
    }

    #[inline]
    fn set_range(&mut self, cursor: usize, block: usize, range: Range<usize>) {
        self.intersection.set_range(cursor, block, range);
    }

    fn clip(&mut self, cursor: usize, low: Option<StorageValue>, high: Option<StorageValue>) {
        // A bound of another tag lies below or above every value of the layer,
        // and a payload beyond the width lies above all of them.
        // Such a bound either leaves its side unbounded or empties the range,
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
    fn for_each_match(&self, cursor: usize, found: impl FnMut(usize, usize)) {
        self.intersection.for_each_match(cursor, found);
    }
}

/// A layer over columns of any layout, dispatching on the layout per call.
///
/// A join with hooks runs on these,
/// and so does a join whose layers are neither all 32-bit nor all 64-bit uniform layers.
#[derive(Debug)]
pub(crate) enum AnyLayer<'a> {
    /// Columns whose values share one tag, all with 32-bit payloads.
    U32(UniformLayer<'a, u32>),
    /// Columns whose values share one tag, all with 64-bit payloads.
    U64(UniformLayer<'a, u64>),
    /// Any other layout, where the intersection compares full values.
    Tagged(Intersection<'a, TaggedCursor<'a>>),
    /// A layer that provably has no value,
    /// because some input column is empty or the shared tags of the columns differ.
    Empty,
}

impl<'a> AnyLayer<'a> {
    /// Returns the layer over the given input columns.
    ///
    /// Each input column is given as its column in every block of its input, in block order.
    /// The layout of a column applies to every range of it,
    /// so the kind of a layer stays fixed for its lifetime.
    /// A layer over an empty input column is empty,
    /// which keeps every range an intersection starts on nonempty.
    pub(crate) fn build(columns: &[&[ColumnView<'a>]], bump: &'a Bump) -> Self {
        match classify(columns) {
            LayerKind::Empty => AnyLayer::Empty,
            LayerKind::Tagged => Self::tagged(columns, bump),
            LayerKind::Uniform(ColumnLayout { tag, is_u32: true }) => AnyLayer::U32(UniformLayer {
                intersection: Intersection::new(Self::payload_cursors::<u32>(columns, bump)),
                tag,
            }),
            LayerKind::Uniform(ColumnLayout { tag, is_u32: false }) => {
                AnyLayer::U64(UniformLayer {
                    intersection: Intersection::new(Self::payload_cursors::<u64>(columns, bump)),
                    tag,
                })
            }
        }
    }

    /// Returns the payload cursors over input columns classified as uniform at width `P`.
    fn payload_cursors<P: Payload>(
        columns: &[&[ColumnView<'a>]],
        bump: &'a Bump,
    ) -> BumpVec<'a, InputCursor<'a, PayloadCursor<'a, P>>> {
        BumpVec::from_iter_in(
            columns.iter().map(|blocks| {
                InputCursor::new(blocks.iter().map(|&column| payload_cursor(column)), bump)
            }),
            bump,
        )
    }

    /// Returns the layer comparing full values over the given input columns.
    fn tagged(columns: &[&[ColumnView<'a>]], bump: &'a Bump) -> Self {
        AnyLayer::Tagged(Intersection::new(BumpVec::from_iter_in(
            columns.iter().map(|blocks| {
                InputCursor::new(blocks.iter().copied().map(TaggedCursor::new), bump)
            }),
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

    /// Returns whether the layer provably has no value.
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

    fn clear_ranges(&mut self, cursor: usize) {
        match self {
            AnyLayer::U32(layer) => layer.clear_ranges(cursor),
            AnyLayer::U64(layer) => layer.clear_ranges(cursor),
            AnyLayer::Tagged(intersection) => intersection.clear_ranges(cursor),
            AnyLayer::Empty => {}
        }
    }

    fn set_range(&mut self, cursor: usize, block: usize, range: Range<usize>) {
        match self {
            AnyLayer::U32(layer) => layer.set_range(cursor, block, range),
            AnyLayer::U64(layer) => layer.set_range(cursor, block, range),
            AnyLayer::Tagged(intersection) => intersection.set_range(cursor, block, range),
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

    fn for_each_match(&self, cursor: usize, found: impl FnMut(usize, usize)) {
        match self {
            AnyLayer::U32(layer) => layer.for_each_match(cursor, found),
            AnyLayer::U64(layer) => layer.for_each_match(cursor, found),
            AnyLayer::Tagged(intersection) => intersection.for_each_match(cursor, found),
            AnyLayer::Empty => {}
        }
    }
}

/// Returns a cursor over the payloads of a column classified as uniform at width `P`,
/// or over nothing for an empty column, which has no layout.
fn payload_cursor<P: Payload>(column: ColumnView<'_>) -> PayloadCursor<'_, P> {
    if column.is_empty() {
        return PayloadCursor::new(&[]);
    }
    let (values, _) = P::payloads(column).expect("the layer's kind was checked");
    PayloadCursor::new(values)
}

/// The kind of layer that a set of input columns forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayerKind {
    /// Some input column is empty, or the shared tags of the input columns differ.
    Empty,
    /// The values of every input column share one tag, and the columns share one width.
    Uniform(ColumnLayout),
    /// Any other layout, where the intersection compares full values.
    Tagged,
}

/// Returns the kind of layer the input columns form,
/// each given as its column in every block of its input.
///
/// An input column is uniform if the nonempty columns of all its blocks
/// share one tag and one width.
/// Its blocks may each be uniform but differ from each other,
/// and then their union mixes values.
fn classify(columns: &[&[ColumnView<'_>]]) -> LayerKind {
    let mut shared: Option<ColumnLayout> = None;
    let mut same_tag = true;
    let mut same_width = true;
    let mut mixed = false;

    for blocks in columns {
        let mut stored = blocks.iter().filter(|column| !column.is_empty()).peekable();
        if stored.peek().is_none() {
            return LayerKind::Empty;
        }
        let Some(layout) = shared_layout(stored) else {
            mixed = true;
            continue;
        };
        match shared {
            None => shared = Some(layout),
            Some(first) => {
                same_tag &= layout.tag == first.tag;
                same_width &= layout.is_u32 == first.is_u32;
            }
        }
    }

    if mixed {
        return LayerKind::Tagged;
    }
    let layout = shared.expect("a layer intersects at least one column");
    if !same_tag {
        LayerKind::Empty
    } else if !same_width {
        LayerKind::Tagged
    } else {
        LayerKind::Uniform(layout)
    }
}

/// The layout every value of a column shares: one tag, at one payload width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColumnLayout {
    /// The tag every value shares.
    tag: Tag,
    /// Whether the payloads have 32 bits.
    is_u32: bool,
}

/// Returns the layout every given column shares,
/// or `None` if some column mixes tags or two columns differ.
fn shared_layout<'c, 'a: 'c>(
    mut columns: impl Iterator<Item = &'c ColumnView<'a>>,
) -> Option<ColumnLayout> {
    let layout = |column: &ColumnView<'_>| {
        column.uniform_tag().map(|tag| ColumnLayout {
            tag,
            is_u32: matches!(column, ColumnView::U32 { .. }),
        })
    };

    let first = layout(columns.next()?)?;
    columns
        .all(|column| layout(column) == Some(first))
        .then_some(first)
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

    /// Returns the layer over the given columns, each of an input stored in one block.
    fn one_block_each<'a>(columns: &[ColumnView<'a>], bump: &'a Bump) -> AnyLayer<'a> {
        let inputs: Vec<&[ColumnView<'a>]> = columns.iter().map(std::slice::from_ref).collect();
        AnyLayer::build(&inputs, bump)
    }

    /// A layer over an input of several blocks intersects like the merged column,
    /// and ranges given to some blocks narrow the union to them.
    #[test]
    fn unions_of_blocks_intersect_like_their_merged_column() {
        let bump = Bump::new();
        // The first input is stored in three blocks, one of them empty, the second in one.
        let (a1, a2, a3) = ([1u64, 4, 6], [2u64, 4, 7], [0u64; 0]);
        let b = [2u64, 4, 6, 9];
        let blocks = [uniform(&a1), uniform(&a2), uniform(&a3)];
        let mut layer = AnyLayer::build(&[&blocks, &[uniform(&b)]], &bump);
        assert!(matches!(layer, AnyLayer::U64(_)));
        assert_eq!(collect(&mut layer), ids(&[2, 4, 6]));

        // After two values, the layer is at 4, and both blocks that contain it report a match.
        layer.mark_for_reset();
        layer.advance();
        assert_eq!(layer.advance(), Some(StorageValue::term(4)));
        let matches = |layer: &AnyLayer<'_>, cursor| {
            let mut matches = Vec::new();
            layer.for_each_match(cursor, |block, position| matches.push((block, position)));
            matches
        };
        assert_eq!(matches(&layer, 0), [(0, 1), (1, 1)]);
        assert_eq!(matches(&layer, 1), [(0, 1)]);

        // Ranges for some blocks narrow the union to them.
        layer.clear_ranges(0);
        layer.set_range(0, 0, 0..1);
        assert_eq!(collect(&mut layer), ids(&[]));
        layer.clear_ranges(0);
        layer.set_range(0, 0, 0..1);
        layer.set_range(0, 1, 0..2);
        assert_eq!(collect(&mut layer), ids(&[2, 4]));
    }

    /// An input whose blocks are uniform with different tags or widths
    /// makes a layer that compares full values.
    #[test]
    fn unions_of_differently_tagged_blocks_compare_full_values() {
        let bump = Bump::new();
        // One block of terms and one of integers. Each is uniform, but their union is not.
        let terms = [3u64, 5];
        let integers = [StorageValue::integer(7).payload];
        let blocks = [
            uniform(&terms),
            ColumnView::U64 {
                values: &integers,
                tag: Tag::Int,
            },
        ];
        let probe = [
            StorageValue::term(5),
            StorageValue::integer(7),
            StorageValue::integer(8),
        ];
        let probe_values = probe.map(|value| value.payload);
        let probe_tags = probe.map(|value| value.tag);
        let mixed = ColumnView::Mixed {
            values: &probe_values,
            tags: &probe_tags,
        };
        let mut layer = AnyLayer::build(&[&blocks, &[mixed]], &bump);
        assert!(matches!(layer, AnyLayer::Tagged(_)));
        assert_eq!(
            collect(&mut layer),
            vec![StorageValue::term(5), StorageValue::integer(7)]
        );

        // Uniform blocks of one tag but different widths compare full values too.
        let narrow = [1u32, 3];
        let wide = [3u64, u64::from(u32::MAX) + 1];
        let blocks = [
            ColumnView::U32 {
                values: &narrow,
                tag: Tag32::Term,
            },
            uniform(&wide),
        ];
        let mut layer = AnyLayer::build(&[&blocks], &bump);
        assert!(matches!(layer, AnyLayer::Tagged(_)));
        assert_eq!(collect(&mut layer), ids(&[1, 3, u64::from(u32::MAX) + 1]));
    }

    /// A layer over three columns yields the values that all three have, again after a reset.
    #[test]
    fn intersections_leapfrog_multiple_columns() {
        let bump = Bump::new();
        let a = [1u64, 2, 3, 4, 5];
        let b = [2u64, 3, 5, 7];
        let c = [1u64, 3, 5, 6];
        let mut layer = one_block_each(&[uniform(&a), uniform(&b), uniform(&c)], &bump);
        assert!(matches!(layer, AnyLayer::U64(_)));
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
        // Exhausted layers stay exhausted until reset.
        assert_eq!(layer.advance(), None);
        layer.mark_for_reset();
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }

    /// A layer over uniform 32-bit columns is a 32-bit uniform layer.
    #[test]
    fn u32_columns_use_the_u32_core() {
        let bump = Bump::new();
        let a: [u32; 4] = [1, 3, 5, 7];
        let b: [u32; 3] = [3, 5, 8];
        let mut layer = one_block_each(
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

    /// A layer over uniform columns of different tags is empty.
    #[test]
    fn mismatched_uniform_tags_are_provably_empty() {
        let bump = Bump::new();
        let a = [1u64, 2, 3];
        let b = [StorageValue::integer(1).payload];
        let layer = one_block_each(
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

    /// A layer over uniform columns of different widths compares full values.
    #[test]
    fn mixed_width_and_mixed_columns_fall_back_to_tagged() {
        let bump = Bump::new();
        let values32 = [1u32, 3, 5];
        let values64 = [3u64, 5, u64::from(u32::MAX) + 5];
        let mut layer = one_block_each(
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

    /// A layer over columns that mix tags intersects them across the tags.
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

        let mut layer = one_block_each(
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

    /// A layer with an empty input column yields no value.
    #[test]
    fn empty_columns_exhaust_immediately() {
        let bump = Bump::new();
        let a = [1u64, 2];
        let empty: [u64; 0] = [];
        let mut layer = one_block_each(&[uniform(&a), uniform(&empty)], &bump);
        assert_eq!(layer.advance(), None);
    }

    /// Ranges given to the cursors restrict the layer's values.
    #[test]
    fn ranges_restrict_the_intersection() {
        let bump = Bump::new();
        let a = [1u64, 3, 5, 7];
        let b = [1u64, 3, 5, 7];
        let mut layer = one_block_each(&[uniform(&a), uniform(&b)], &bump);
        layer.set_range(0, 0, 1..3);
        layer.set_range(1, 0, 1..3);
        assert_eq!(collect(&mut layer), ids(&[3, 5]));
    }
}
