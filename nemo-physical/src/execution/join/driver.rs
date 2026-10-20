//! The driver: the loop descending through a join's layers,
//! monomorphized over the kind of layer and the hooks.
//!
//! The driver fills a row with one value per layer, outermost first.
//! At the current layer it finds the next value the hooks admit,
//! restricts the deeper cursors to the blocks below that value, and descends.
//! When a layer is exhausted, it returns to the layer above.
//! A row is complete once the deepest layer has a value;
//! the driver then resumes at the deepest emitted layer,
//! since the layers after it only established that the row exists.

use bumpalo::{Bump, collections::Vec as BumpVec};

use crate::{
    execution::{
        JoinShape,
        join::{
            hooks::{Bind, Hooks},
            intersection::JoinLayer,
        },
        sink::RowSink,
    },
    storage::StorageValue,
    trie::{Intervals, Trie},
};

/// A link from a cursor to the cursor over the next column of its input, at a deeper layer.
///
/// The position of a match indexes the intervals of the next column,
/// which give the block the next cursor is restricted to.
#[derive(Debug, Clone, Copy)]
pub(super) struct NextLink<'a> {
    /// The intervals of the input's next column.
    pub(super) intervals: &'a Intervals,
    /// The layer intersecting the next column.
    pub(super) layer: usize,
    /// The index of the next column's cursor within its layer.
    pub(super) cursor: usize,
}

/// The loop of a join, monomorphized over the kind of layer `L` and the hooks `H`.
#[derive(Debug)]
pub(super) struct Driver<'a, L: JoinLayer<'a>, H> {
    /// The layers, outermost first.
    layers: BumpVec<'a, L>,
    /// Per layer and cursor, the link to the cursor over the input's next column, if any.
    links: BumpVec<'a, BumpVec<'a, Option<NextLink<'a>>>>,
    /// The hooks.
    hooks: H,
    /// The number of leading layers emitted as result rows.
    emitted: usize,
    /// The values of the current, possibly partial, row in the layers' own type:
    /// payloads for the uniform layers.
    row: BumpVec<'a, L::Value>,
    /// The emitted values of the current row as storage values;
    /// a layer's value is stamped when it changes, never per candidate.
    emitted_row: BumpVec<'a, StorageValue>,
    /// The layer the driver acts on next;
    /// the depth of the join right after a row was completed.
    current_layer: usize,
    /// The shallowest layer that found a value since the last row;
    /// `usize::MAX` while none did.
    pending_changed: usize,
    /// The shallowest layer whose value changed in the current row.
    changed: usize,
    /// Whether the join has no further rows.
    exhausted: bool,
}

impl<'a, L: JoinLayer<'a>, H: Hooks<'a, L::Value>> Driver<'a, L, H> {
    /// Creates the driver over the given layers.
    ///
    /// `filler` is any value; it fills the row until the layers write it.
    pub(super) fn new(
        layers: BumpVec<'a, L>,
        links: BumpVec<'a, BumpVec<'a, Option<NextLink<'a>>>>,
        hooks: H,
        emitted: usize,
        filler: L::Value,
        bump: &'a Bump,
    ) -> Self {
        let depth = layers.len();

        let mut driver = Self {
            layers,
            links,
            hooks,
            emitted,
            row: BumpVec::from_iter_in(std::iter::repeat_n(filler, depth), bump),
            emitted_row: BumpVec::from_iter_in(
                std::iter::repeat_n(StorageValue::term(0), emitted),
                bump,
            ),
            current_layer: 0,
            pending_changed: usize::MAX,
            changed: 0,
            exhausted: false,
        };
        // The run is the one descent into the first layer.
        driver.hooks.enter_layer(0);
        driver
    }

    /// Rebinds the driver to other tries of the same shape for a fresh run,
    /// and returns whether that succeeded.
    ///
    /// The layers take the tries' columns and the links their intervals;
    /// the hooks and the descent restart.
    /// If some layer's columns need a layer of another kind,
    /// the driver is unusable and must be built anew.
    pub(super) fn rebind(
        &mut self,
        shape: &JoinShape<'a>,
        tries: &[&'a Trie],
        bump: &'a Bump,
    ) -> bool {
        let mut views = BumpVec::new_in(bump);
        for (layer, columns) in shape.layers.iter().enumerate() {
            views.clear();
            views.extend(
                columns
                    .iter()
                    .map(|input_column| tries[input_column.input].column(input_column.column)),
            );

            if !self.layers[layer].rebind(&views) {
                return false;
            }

            for (cursor, link) in self.links[layer].iter_mut().enumerate() {
                if let Some(link) = link {
                    let input_column = columns[cursor];
                    link.intervals = tries[input_column.input].intervals(input_column.column + 1);
                }
            }
        }

        self.hooks.rebind(shape, tries);
        self.hooks.enter_layer(0);
        self.current_layer = 0;
        self.pending_changed = usize::MAX;
        self.changed = 0;
        self.exhausted = false;

        true
    }

    /// Clips every cursor of a layer to a range of values;
    /// see [TrieJoin::clip_layer](super::TrieJoin::clip_layer).
    pub(super) fn clip_layer(
        &mut self,
        layer: usize,
        low: Option<StorageValue>,
        high: Option<StorageValue>,
    ) {
        for cursor in 0..self.layers[layer].cursors() {
            self.layers[layer].clip(cursor, low, high);
        }
    }

    /// Advances to the next row, and returns whether there is one.
    fn advance(&mut self) -> bool {
        if self.exhausted {
            return false;
        }

        let depth = self.layers.len();
        if self.current_layer == depth {
            // Resuming at the deepest emitted layer makes the rows distinct:
            // the layers after it only established that the last row exists.
            let Some(deepest) = self.emitted.checked_sub(1) else {
                self.exhausted = true;
                return false;
            };
            self.current_layer = deepest;
        }

        while self.current_layer < depth {
            match self.next_admissible(self.current_layer) {
                Some(()) => {
                    self.pending_changed = self.pending_changed.min(self.current_layer);
                    self.propagate_ranges(self.current_layer);

                    self.current_layer += 1;

                    if self.current_layer < depth {
                        // Entered from above: the layer's intersection and
                        // its hooks restart within their new ranges.
                        self.layers[self.current_layer].mark_for_reset();
                        self.hooks.enter_layer(self.current_layer);
                    }
                }
                None => {
                    if self.current_layer == 0 {
                        self.exhausted = true;
                        return false;
                    }

                    self.current_layer -= 1;
                }
            }
        }

        // Every row is reached through some layer that found a value.
        self.changed = std::mem::replace(&mut self.pending_changed, usize::MAX);
        debug_assert!(self.changed < depth, "a row was reached without a value");

        // Stamp the changed layers of the emitted row;
        // the shallower ones keep their values from the previous row.
        for layer in self.changed..self.emitted {
            self.emitted_row[layer] = self.layers[layer].stamp(self.row[layer]);
        }

        true
    }

    /// Returns the hooks.
    pub(super) fn hooks(&self) -> &H {
        &self.hooks
    }

    /// Advances a layer to its next value the hooks admit,
    /// and writes it into the row, where the filters read it;
    /// `None` if the layer is exhausted.
    ///
    /// A bound layer probes for its computed value instead, once per descent.
    #[inline]
    fn next_admissible(&mut self, layer: usize) -> Option<()> {
        match self.hooks.bind(layer, &self.row) {
            Bind::Free => loop {
                let value = self.layers[layer].advance()?;
                self.row[layer] = value;

                if self.hooks.admit(layer, value, &self.row) {
                    return Some(());
                }
            },
            Bind::Value(target) => {
                let value = self.layers[layer].seek_exact(target)?;
                self.row[layer] = value;

                self.hooks.admit(layer, value, &self.row).then_some(())
            }
            Bind::Dead => None,
        }
    }

    /// Restricts the cursors linked from a layer's cursors
    /// to the blocks below the positions where the layer found its value.
    #[inline]
    fn propagate_ranges(&mut self, layer: usize) {
        for (cursor, link) in self.links[layer].iter().enumerate() {
            let Some(link) = link else {
                continue;
            };

            let position = self.layers[layer].position(cursor);
            self.layers[link.layer].set_range(link.cursor, link.intervals.children(position));
        }
    }

    /// Drains the remaining rows into a sink,
    /// in one loop monomorphized over the layers, the hooks and the sink.
    pub(super) fn drain<S: RowSink>(&mut self, sink: &mut S) {
        while self.advance() {
            sink.accept(&self.emitted_row, self.changed);
        }
    }
}
