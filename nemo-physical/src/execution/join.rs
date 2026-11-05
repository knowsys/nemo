//! The join: a leapfrog trie join over immutable tries.
//!
//! [TrieJoin] is built over a [shape](JoinShape) and the inputs it joins,
//! each stored in one or several tries, its blocks,
//! and yields the result rows in storage order.
//! Beneath it, the [driver] descends through the layers.
//! Each layer is an [intersection] of [cursors](input) over the input columns,
//! each the [cursor] over the column of a block or the [union] of those of several,
//! the [hooks] decide whether a value found at a layer stands,
//! and the working state comes from an [arena](JoinArena).
//! A [template](JoinTemplate) builds the joins of one shape over different inputs.

mod arena;
mod cursor;
mod driver;
mod hooks;
mod input;
mod intersection;
mod template;
mod union;

pub use self::arena::JoinArena;
pub(crate) use self::template::JoinTemplate;

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    dictionary::Dictionary,
    execution::{
        join::{
            driver::{Driver, NextLink},
            hooks::{FullHooks, NoHooks},
            intersection::{AnyLayer, UniformLayer},
        },
        shape::JoinShape,
        sink::RowSink,
    },
    function::EvaluationContext,
    storage::StorageValue,
    trie::Trie,
    value::ErrorCounts,
};

/// The core a join runs on, monomorphized for the layout of its layers and for its hooks.
///
/// The variants differ widely in size, and the enum lives on the stack:
/// an allocation per join would cost more than the space.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Core<'a> {
    /// No hooks; each layer holds values of a single tag, with 32-bit payloads.
    U32(Driver<'a, UniformLayer<'a, u32>, NoHooks>),
    /// No hooks; each layer holds values of a single tag, with 64-bit payloads.
    U64(Driver<'a, UniformLayer<'a, u64>, NoHooks>),
    /// No hooks, over any other layout; each layer dispatches on its own.
    Mixed(Driver<'a, AnyLayer<'a>, NoHooks>),
    /// Hooks, over any layout.
    Hooked(Driver<'a, AnyLayer<'a>, FullHooks<'a>>),
    /// Some layer is provably empty, so the join has no rows.
    Empty,
}

/// Evaluates an expression on the core a join runs on,
/// or the given stand-in for the empty join.
macro_rules! dispatch {
    ($core:expr, $name:ident => $body:expr, $empty:expr) => {
        match $core {
            Core::U32($name) => $body,
            Core::U64($name) => $body,
            Core::Mixed($name) => $body,
            Core::Hooked($name) => $body,
            Core::Empty => $empty,
        }
    };
}

/// A leapfrog trie join over immutable tries.
///
/// The join yields its rows in storage order, each once,
/// with the shallowest layer that changed since the previous row.
/// It hands them to a [sink](RowSink) through [drain](Self::drain).
///
/// Each input of the join is stored in one or several tries, its blocks,
/// and the join reads the union of their rows.
///
/// The hooks of a join are its subtracted inputs, its filters and its bound layers.
/// The join selects its core once, when it is built,
/// so that the loop over the rows does not dispatch:
/// - a join without hooks whose layers each hold values of a single tag
///   runs over the raw payloads, and stamps the tags onto the emitted rows only,
/// - any other join without hooks dispatches per layer on the layer's layout,
/// - only a join with hooks pays for checking them.
#[derive(Debug)]
pub(crate) struct TrieJoin<'a> {
    /// The core the join runs on.
    core: Core<'a>,
}

impl<'a> TrieJoin<'a> {
    /// Creates the join of a shape,
    /// over the blocks of each of its inputs, which the shape's input columns index,
    /// and one trie per subtracted input of the shape, in the same order.
    ///
    /// A shape with filters or bound layers decodes values through the dictionary,
    /// and its programs evaluate in the context.
    ///
    /// # Panics
    ///
    /// Panics if the shape is malformed, does not fit the inputs,
    /// or needs a dictionary and is given none.
    pub(crate) fn new(
        shape: JoinShape<'a>,
        inputs: &[&[&'a Trie]],
        subtracted: &[&'a Trie],
        dictionary: Option<&'a Dictionary>,
        context: EvaluationContext,
        arena: &'a JoinArena,
    ) -> Self {
        if let Err(violation) = shape.validate() {
            panic!("{violation}");
        }
        if let Err(mismatch) = shape.validate_fit(inputs, subtracted, dictionary) {
            panic!("{mismatch}");
        }

        Self::with_trusted_shape(shape, inputs, subtracted, dictionary, context, arena)
    }

    /// Creates the join of a shape taken to be valid
    /// and to fit the inputs and the dictionary it is given; see [new](Self::new).
    ///
    /// Only debug builds check the fit.
    pub(crate) fn with_trusted_shape(
        shape: JoinShape<'a>,
        inputs: &[&[&'a Trie]],
        subtracted: &[&'a Trie],
        dictionary: Option<&'a Dictionary>,
        context: EvaluationContext,
        arena: &'a JoinArena,
    ) -> Self {
        debug_assert!(
            shape.validate_fit(inputs, subtracted, dictionary).is_ok(),
            "a trusted shape fits what the join is given"
        );

        let bump = arena.bump();
        // Empty blocks add no rows, and an input without rows leaves the join without rows.
        let inputs = BumpVec::from_iter_in(
            inputs.iter().map(|blocks| {
                BumpVec::from_iter_in(
                    blocks.iter().copied().filter(|block| !block.is_empty()),
                    bump,
                )
                .into_bump_slice()
            }),
            bump,
        );
        if inputs.iter().any(|blocks| blocks.is_empty()) {
            return Self { core: Core::Empty };
        }

        let occurrences = column_occurrences(&shape, inputs.len(), bump);
        let (layers, links) = build_layers(&shape, &inputs, &occurrences, bump);

        if layers.iter().any(AnyLayer::is_empty) {
            return Self { core: Core::Empty };
        }

        let hooks = FullHooks::build(&shape, &inputs, subtracted, dictionary, context, bump);

        Self {
            core: select_core(layers, links, hooks, shape.emitted, bump),
        }
    }

    /// Clips a layer to the values from `low` up to `high`, excluded, in storage order;
    /// `None` leaves a side unbounded.
    ///
    /// Joins clipped to disjoint ranges yield disjoint parts of the result,
    /// which is how a join is partitioned across workers.
    /// The clip applies to every block the layer enters,
    /// and must precede the drain.
    pub(crate) fn clip_layer(
        &mut self,
        layer: usize,
        low: Option<StorageValue>,
        high: Option<StorageValue>,
    ) {
        dispatch!(&mut self.core, core => core.clip_layer(layer, low, high), ())
    }

    /// Returns the evaluation errors counted so far, by kind.
    ///
    /// A filter or bound layer whose program fails on a row drops the row,
    /// and the caller reports the count.
    pub(crate) fn evaluation_errors(&self) -> ErrorCounts {
        match &self.core {
            Core::Hooked(core) => core.hooks().errors(),
            _ => ErrorCounts::default(),
        }
    }

    /// Drains the remaining rows into a sink,
    /// in one loop monomorphized for the core and the sink.
    pub(crate) fn drain<S: RowSink>(&mut self, sink: &mut S) {
        dispatch!(&mut self.core, core => core.drain(sink), ())
    }

    /// Returns whether a filter runs its integer form.
    #[cfg(test)]
    pub(crate) fn filter_uses_integer_form(&self, index: usize) -> bool {
        match &self.core {
            Core::Hooked(core) => core.hooks().filter_uses_integer_form(index),
            _ => false,
        }
    }

    /// Returns whether a filter is decided on storage values.
    #[cfg(test)]
    pub(crate) fn filter_uses_identity_form(&self, index: usize) -> bool {
        match &self.core {
            Core::Hooked(core) => core.hooks().filter_uses_identity_form(index),
            _ => false,
        }
    }
}

/// Returns, per input, where each of its columns is intersected, in column order:
/// the layer, and the index of the column's cursor there.
fn column_occurrences<'a>(
    shape: &JoinShape<'a>,
    inputs: usize,
    bump: &'a Bump,
) -> BumpVec<'a, BumpVec<'a, (usize, usize)>> {
    let mut occurrences = BumpVec::from_iter_in((0..inputs).map(|_| BumpVec::new_in(bump)), bump);
    for (layer_index, layer) in shape.layers.iter().enumerate() {
        for (cursor, input_column) in layer.iter().enumerate() {
            let slots = &mut occurrences[input_column.input];
            debug_assert_eq!(
                input_column.column,
                slots.len(),
                "shapes map prefixes in order"
            );
            slots.push((layer_index, cursor));
        }
    }

    occurrences
}

/// Builds, for each layer of the shape, the intersection over the input columns mapped to it.
///
/// Per layer and cursor, it also builds the link to the cursor over the input's next column,
/// if that column is mapped.
/// When the layer finds a value, the link restricts that cursor to the nodes below it.
fn build_layers<'a>(
    shape: &JoinShape<'a>,
    inputs: &[&'a [&'a Trie]],
    occurrences: &[BumpVec<'a, (usize, usize)>],
    bump: &'a Bump,
) -> (
    BumpVec<'a, AnyLayer<'a>>,
    BumpVec<'a, BumpVec<'a, Option<NextLink<'a>>>>,
) {
    let depth = shape.layers.len();
    let mut layers = BumpVec::with_capacity_in(depth, bump);
    let mut links = BumpVec::with_capacity_in(depth, bump);
    // One buffer of input columns serves every layer.
    let mut columns = BumpVec::new_in(bump);

    for layer in shape.layers {
        columns.clear();
        columns.extend(layer.iter().map(|input_column| {
            let blocks = inputs[input_column.input].iter();
            BumpVec::from_iter_in(blocks.map(|trie| trie.column(input_column.column)), bump)
                .into_bump_slice()
        }));
        layers.push(AnyLayer::build(&columns, bump));

        let layer_links = BumpVec::from_iter_in(
            layer.iter().map(|input_column| {
                let (layer, cursor) =
                    *occurrences[input_column.input].get(input_column.column + 1)?;
                let blocks = inputs[input_column.input].iter();
                Some(NextLink {
                    intervals: BumpVec::from_iter_in(
                        blocks.map(|trie| trie.intervals(input_column.column + 1)),
                        bump,
                    )
                    .into_bump_slice(),
                    layer,
                    cursor,
                })
            }),
            bump,
        );
        links.push(layer_links);
    }

    (layers, links)
}

/// Returns the core a join runs on:
/// the core with hooks if there are any,
/// and otherwise the core the layouts of the layers allow.
fn select_core<'a>(
    layers: BumpVec<'a, AnyLayer<'a>>,
    links: BumpVec<'a, BumpVec<'a, Option<NextLink<'a>>>>,
    hooks: Option<FullHooks<'a>>,
    emitted: usize,
    bump: &'a Bump,
) -> Core<'a> {
    if let Some(hooks) = hooks {
        return Core::Hooked(Driver::new(
            layers,
            links,
            hooks,
            emitted,
            StorageValue::term(0),
            bump,
        ));
    }

    if layers.iter().all(|layer| matches!(layer, AnyLayer::U32(_))) {
        let layers = BumpVec::from_iter_in(
            layers
                .into_iter()
                .map(|layer| layer.into_u32().expect("every layer is 32-bit")),
            bump,
        );
        Core::U32(Driver::new(layers, links, NoHooks, emitted, 0u32, bump))
    } else if layers.iter().all(|layer| matches!(layer, AnyLayer::U64(_))) {
        let layers = BumpVec::from_iter_in(
            layers
                .into_iter()
                .map(|layer| layer.into_u64().expect("every layer is 64-bit")),
            bump,
        );
        Core::U64(Driver::new(layers, links, NoHooks, emitted, 0u64, bump))
    } else {
        Core::Mixed(Driver::new(
            layers,
            links,
            NoHooks,
            emitted,
            StorageValue::term(0),
            bump,
        ))
    }
}
