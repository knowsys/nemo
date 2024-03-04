//! This module defines [TrieScanPrune].

use std::{cell::UnsafeCell, rc::Rc};

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::prune::ColumnScanPrune,
    },
    datatypes::{
        storage_type_name::StorageTypeBitSet, ColumnDataType, StorageTypeName, StorageValueT,
    },
    tabular::triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
};

/// [`TrieScan`] which only returns values that would actually exists during materialization.
///
/// This trie scan given the following guarantees for the [`TrieScan`] API:
///   * For every value returned by a column using `next` or `current`, there exists values for all the lower layers (they can be retrieved using `down()` and `next()`).
///   * The trie scan does not skip any tuples.
///
/// To achieve this behavior, before returning a value from `next()`, the input trie is traversed downwards to check if the value would exists in a materialized version of the trie scan.
/// Therefore, every column and the trie scan itself has shared access to the input trie and associated state, through [`SharedTrieScanPruneState`].
#[derive(Debug)]
pub(crate) struct TrieScanPrune<'a> {
    state: SharedTrieScanPruneState<'a>,
    output_column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> TrieScanPrune<'a> {
    /// Create a new [TrieScanPrune].
    pub(crate) fn new(input_trie_scan: TrieScanEnum<'a>) -> Self {
        let input_trie_scan_arity = input_trie_scan.arity();

        let mut output_column_scans =
            Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(input_trie_scan.arity());

        // TODO: One could check if some of the entries are empty here
        // and from that deduce that the result of this will be empty
        let possible_types = (0..input_trie_scan_arity)
            .map(|layer| input_trie_scan.possible_types(layer).storage_types())
            .collect();

        let state = Rc::new(UnsafeCell::new(TrieScanPruneState {
            input_trie_scan,
            input_trie_scan_current_layer: 0,
            highest_peeked_layer: None,
            initialized: false,
            external_current_layer: 0,
            external_path_types: Vec::new(),
            possible_types,
            input_trie_scan_current_type: Vec::with_capacity(input_trie_scan_arity),
        }));

        // Create one `ColumnScanPrune` for every input column
        for layer in 0..input_trie_scan_arity {
            // Generate code for every possible data type of the input column
            macro_rules! prune_scan {
                ($type:ty, $variant:ident, $scan:ident) => {{
                    // Get input column scan
                    // SAFETY: we're the only one accessing the shared state at this moment
                    let input_scan = unsafe {
                        let scan_typed = &*(*state.get()).input_trie_scan.scan(layer).get();
                        &scan_typed.$scan
                    };

                    // let mut input_scan: &'a ColumnScanCell<$type> =
                    //     &unsafe { &*input_trie_scan.scan(layer).get() }.$scan;

                    ColumnScanEnum::Prune(ColumnScanPrune::new(
                        Rc::clone(&state),
                        layer,
                        StorageTypeName::$variant,
                        input_scan,
                    ))
                }};
            }

            let prune_scan_id32 = prune_scan!(u32, Id32, scan_id32);
            let prune_scan_id64 = prune_scan!(u64, Id64, scan_id64);
            let prune_scan_i64 = prune_scan!(i64, Int64, scan_i64);
            let prune_scan_float = prune_scan!(Float, Float, scan_float);
            let prune_scan_double = prune_scan!(Double, Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                prune_scan_id32,
                prune_scan_id64,
                prune_scan_i64,
                prune_scan_float,
                prune_scan_double,
            );
            output_column_scans.push(UnsafeCell::new(new_scan));
        }

        TrieScanPrune {
            state,
            output_column_scans,
        }
    }
}

/// Allows for shared access to the input trie and it's columns.
/// This is required because every output column (`ColumnScanPrune`) needs to
/// (at least indirectly) call `up` and `down` on the input trie.
///
/// `Rc<UnsafeCell<_>>>` is required here, because we cannot guarantee that the trie scan exists longer than the individual output columns.
/// The overhead after initialization should be negligible and the same as a single pointer indirection.
pub(crate) type SharedTrieScanPruneState<'a> = Rc<UnsafeCell<TrieScanPruneState<'a>>>;

/// State which is shared with the individual output column scans and the trie scan
#[derive(Debug)]
pub(crate) struct TrieScanPruneState<'a> {
    /// Trie scan which is being pruned
    input_trie_scan: TrieScanEnum<'a>,
    /// Possible types in each layer of the `input_trie_scan`
    possible_types: Vec<Vec<StorageTypeName>>,

    /// Whether the first `external_down()` has been made (to go to layer `0`)
    initialized: bool,

    /// Current column scan layer of the input trie scan
    /// Layer zero is at to top of the trie scan
    input_trie_scan_current_layer: usize,
    /// For each layer the current type index (index in `possible`) we are at
    input_trie_scan_current_type: Vec<usize>,

    /// Index of the highest layer which has been peeked into by the `advance()` function
    /// This layer and layers below this index should ignore the next call to `advance`, because they have already been advanced to the next value behind the scenes.
    /// This variable is required to implement the lookahead which checks if
    /// a value would exist in a materialized version of the trie scan.
    highest_peeked_layer: Option<usize>,
    /// Layer on which an outside consumer would believe this trie scan to be
    /// TODO: Maybe move to the `TrieScanPrune` itself, if we do not want to use this for validating the trie is on the correct layer in `ColumnScanPrune`
    external_current_layer: usize,
    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    external_path_types: Vec<StorageTypeName>,
}

impl<'a> TrieScanPruneState<'a> {
    /// Decrements the `external_current_layer` and goes up to the `external_current_layer` to reset already seen rows.
    pub(crate) fn external_up(&mut self) {
        debug_assert!(self.initialized);

        if self.external_current_layer == 0 {
            // Move above the first input layer, thus resetting the input trie scan
            self.input_trie_scan.up();
            self.initialized = false;
            self.highest_peeked_layer = None;
            self.input_trie_scan_current_type.pop();
            return;
        }

        self.external_current_layer -= 1;
        self.external_path_types.pop();

        // Actually go up to the external layer
        // This ensures that already seen rows will be seen again
        // This is required by the partial trie scan interface
        while self.input_trie_scan_current_layer > self.external_current_layer {
            self.input_up();
        }

        // Reset column peeks up to the current layer
        // The current external layer is guaranteed to be above the highest peeked layer
        self.highest_peeked_layer = None;
    }

    /// Increments the `external_current_layer`
    pub(crate) fn external_down(&mut self, storage_type: StorageTypeName) {
        self.external_path_types.push(storage_type);

        if !self.initialized {
            // First down initializes the trie scan
            self.initialized = true;
            self.input_trie_scan.down(storage_type);
            self.input_trie_scan_current_type.push(0);
            return;
        }

        assert!(self.external_current_layer < self.input_trie_scan.arity() - 1);
        assert!(self
            .highest_peeked_layer
            .map_or(true, |layer| layer > self.external_current_layer));

        self.external_current_layer += 1;
    }

    /// Return the number of columns in the input trie.
    pub(crate) fn arity(&self) -> usize {
        self.input_trie_scan.arity()
    }

    /// Return the possible types for a given layer in this scan.
    pub(crate) fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        self.input_trie_scan.possible_types(layer)
    }

    fn input_up(&mut self) {
        assert!(self.input_trie_scan_current_layer > 0);

        self.input_trie_scan.up();
        self.input_trie_scan_current_type.pop();
        self.input_trie_scan_current_layer -= 1;
    }

    fn input_down(&mut self) {
        assert!(self.input_trie_scan_current_layer < self.input_trie_scan.arity() - 1);

        self.input_trie_scan_current_layer += 1;

        let first_type = *self.possible_types[self.input_trie_scan_current_layer]
            .first()
            .unwrap();

        self.input_trie_scan.down(first_type);
        self.input_trie_scan_current_type.push(0);
    }

    fn go_to_next_type(&mut self) -> Option<StorageTypeName> {
        let current_type_index =
            &mut self.input_trie_scan_current_type[self.input_trie_scan_current_layer];
        *current_type_index += 1;

        self.possible_types[self.input_trie_scan_current_layer]
            .get(*current_type_index)
            .cloned()
    }

    fn input_jump_type(&mut self) -> bool {
        let next_type = self.go_to_next_type();

        if let Some(next_type) = next_type {
            self.input_trie_scan.up();
            self.input_trie_scan.down(next_type);

            true
        } else {
            false
        }
    }

    /// Returns the current layer of this trie scan
    pub(crate) fn current_layer(&self) -> Option<usize> {
        self.initialized.then_some(self.external_current_layer)
    }

    /// Moves the input trie to the target layer through `up` and `down`, without advancing this trie scan at all.
    ///
    /// Going upwards will always work (until layer 0).
    /// Going downwards requires that the current value of the layer is not currently `None`, the `highest_peeked_layer` is ignored and going down also works for layers which are currently peeked.
    /// If going downwards was not possible, this method will return `false` with the input trie scan ending up on the layer where it was not possible to go down further.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scans between `input_trie_scan_current_layer` and `target_layer` and thus the [`UnsafeCell`] is safe to access when going downwards.
    /// This is required to check if going downwards is allowed.
    unsafe fn try_to_go_to_layer(&mut self, target_layer: usize) -> bool {
        assert!(target_layer < self.input_trie_scan.arity());

        // Move up or down to the correct layer
        // Note that the uppermost layer has index 0

        for _ in target_layer..self.input_trie_scan_current_layer {
            self.input_up();
        }
        for _ in self.input_trie_scan_current_layer..target_layer {
            if self.current_input_value().is_none() {
                return false;
            }
            self.input_down();
        }

        true
    }

    /// Returns whether a column has been peeked already.
    ///
    /// See [`TrieScanPruneState`] for more information.
    #[inline]
    pub(crate) fn is_column_peeked(
        &self,
        index: usize,
        storage_type_opt: Option<StorageTypeName>,
    ) -> bool {
        // Cant you just check whether the input trie layer is lower than the external layer?
        // TODO: Make prettier
        self.highest_peeked_layer.map_or(false, |p| {
            index >= p
                && if let Some(storage_type) = storage_type_opt {
                    if let Some(&input_type) =
                        self.possible_types[index].get(self.input_trie_scan_current_type[index])
                    {
                        input_type == storage_type
                    } else {
                        false
                    }
                } else {
                    true
                }
        })
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    #[inline]
    pub(crate) unsafe fn current_input_trie_value(&self, index: usize) -> Option<StorageValueT> {
        let current_input_type =
            self.possible_types[index].get(self.input_trie_scan_current_type[index])?;
        let scan = self.input_trie_scan.scan(index);

        unsafe { (*scan.get()).current(*current_input_type) }
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no immutable/mutable references to the column scan at `index` and thus the value inside the [`UnsafeCell`] is safe to mutate.
    #[inline]
    unsafe fn next_input_trie_value(&mut self) -> Option<StorageValueT> {
        let current_input_type = self.possible_types[self.input_trie_scan_current_layer]
            .get(self.input_trie_scan_current_type[self.input_trie_scan_current_layer])?;
        let scan = self
            .input_trie_scan
            .scan(self.input_trie_scan_current_layer);

        unsafe { (*scan.get()).next(*current_input_type) }
    }

    /// Directly gets the current output value for a column ignoring layer peeks (see `highest_peeked_layer`).
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    ///
    /// TODO: Update to allow for direct access
    #[inline]
    unsafe fn current_input_value(&self) -> Option<StorageValueT> {
        debug_assert!(self.initialized);

        self.current_input_trie_value(self.input_trie_scan_current_layer)
    }

    /// Helper method for the `advance_on_layer()` and `advance_on_layer_with_seek()`
    ///
    /// Goes to next value without checking layer peeks (see `highest_peeked_layer`), and returns if a value exists.
    fn advance_has_next_value(&mut self) -> bool {
        // SAFETY: this requires that no other references to the current layer scan exist. We currently have no way of ensuring this.
        let next_value = unsafe { self.next_input_trie_value() };

        next_value.is_some()
    }

    /// Marks layers below the `target_layer` as peeked. Should only be used by the `advance()` functions.
    fn set_highest_peeked_layer_by_target_layer(&mut self, target_layer: usize) {
        self.highest_peeked_layer = if target_layer == self.input_trie_scan.arity() - 1 {
            None
        } else {
            Some(target_layer + 1)
        };
    }

    /// Same as `advance_on_layer()`, but calls `seek()` at the target layer to find advance to the next relevant tuple more efficiently.
    ///
    /// Currently, this function does not support returning the uppermost changed layer. This can be implemented in the future.
    pub(crate) fn advance_on_layer_with_seek<T: ColumnDataType>(
        &mut self,
        _target_layer: usize,
        _allow_advancements_above_target_layer: bool,
        _underlying_column_scan: &'a ColumnScanCell<'a, T>,
        _seek_minimum_value: T,
    ) -> Option<T> {
        // debug_assert!(self.initialized);

        // // Semantics of the `boundary_layer`:
        // //   * If we're at the boundary layer, we may not call `up()`.
        // //   * If we're above the boundary layer, we may not call `up()` or advance the layer (using `next()` or by removing layer peeks)
        // let boundary_layer = if allow_advancements_above_target_layer {
        //     0
        // } else {
        //     target_layer
        // };

        // // Fully handle layer peeks before advancing any layers
        // if self.is_column_peeked(target_layer) {
        //     if self.highest_peeked_layer.unwrap() == target_layer
        //         || allow_advancements_above_target_layer
        //     {
        //         // Just remove the peek to advance the layer
        //         // Check if the value behind the peek satisfies the seek condition, otherwise continue search after removing the peek
        //         let next_value = underlying_column_scan.current();
        //         if next_value.unwrap() >= seek_minimum_value {
        //             // Mark layers below target layer as peeked
        //             self.set_highest_peeked_layer_by_target_layer(target_layer);

        //             return Some(next_value.unwrap());
        //         }
        //     } else {
        //         // We would need to unpeek (advance) layers above the boundary_layer, which is not allowed
        //         return None;
        //     }
        // }
        // // This needs to be adjusted before returning from the function
        // self.highest_peeked_layer = None;

        // // SAFETY: this requires that no other references to the layers below exist. We currently have no way of ensuring this.
        // unsafe { self.try_to_go_to_layer(target_layer) };

        // if self.input_trie_scan_current_layer < boundary_layer {
        //     // We would need to advance to even get to the target layer, but are not allowed to because of the `boundary_layer`.
        //     return None;
        // }

        // // Whether seek was already called on the trie scan of the target layer
        // // In this case `next` can be called instead
        // let mut perform_next_instead_of_seek = false;

        // let mut return_value = None;

        // // Traverse trie downwards to check if the value would actually exists in the materialized version of the trie
        // // If at one layer there is no materialized value, move up, go to the next item, and move down again
        // loop {
        //     // Go down one layer and check if there exists a item
        //     // If there exists no value, go upwards until either
        //     // a new value is found or we hit the the `boundary_layer`

        //     let has_next_value = if self.input_trie_scan_current_layer == target_layer {
        //         if perform_next_instead_of_seek {
        //             return_value = underlying_column_scan.next();
        //             return_value.is_some()
        //         } else {
        //             // Perform `next` instead of `seek` until we go above the `target_layer` again
        //             perform_next_instead_of_seek = true;

        //             return_value = underlying_column_scan.seek(seek_minimum_value);
        //             return_value.is_some()
        //         }
        //     } else {
        //         // On other layers than the `target_layer` there are no seeks going on, only `next()`
        //         self.advance_has_next_value()
        //     };

        //     if !has_next_value {
        //         if self.input_trie_scan_current_layer == boundary_layer {
        //             // Boundary layer has been reached and has no next value

        //             // `highest_peeked_layer`has already been set to None
        //             return None;
        //         }

        //         self.up();

        //         if self.input_trie_scan_current_layer < target_layer {
        //             perform_next_instead_of_seek = false;
        //         }
        //     } else if self.input_trie_scan_current_layer == self.input_trie_scan.arity() - 1 {
        //         // Lowest layer has been reached and an materialized value has been found

        //         // Mark layers below the `target_layer` as peeked
        //         self.set_highest_peeked_layer_by_target_layer(target_layer);

        //         return return_value;
        //     } else {
        //         self.down();
        //     }
        // }

        unimplemented!()
    }

    /// Moves to the next value on a given layer while ensuring that only materialized tuples are returned (see guarantees provided by [`TrieScanPrune`]).
    ///
    /// See documentation of function `advance_on_layer()` of [`TrieScanPrune`].
    pub(crate) fn advance_on_layer(
        &mut self,
        target_layer: usize,
        stay_in_type: Option<StorageTypeName>,
    ) -> Option<usize> {
        debug_assert!(self.initialized);
        //????
        self.external_current_layer = target_layer; // Wird vielleicht unten gamacht
                                                    // self.external_path_types = //???

        let allow_advancements_above_target_layer = stay_in_type.is_none();

        // Semantics of the `boundary_layer`:
        //   * If we're at the boundary layer, we may not call `up()`.
        //   * If we're above the boundary layer, we may not call `up()` or advance the layer (using `next()` or by removing layer peeks)
        let boundary_layer = if allow_advancements_above_target_layer {
            0
        } else {
            target_layer
        };

        // Fully handle layer peeks before advancing any layers
        if self.is_column_peeked(target_layer, stay_in_type) {
            if self.highest_peeked_layer.unwrap() <= target_layer || stay_in_type.is_some() {
                // Just remove the peek to advance the layer
                let old_highest_peeked_layer = self.highest_peeked_layer;

                // Mark layers below target layer as peeked
                self.set_highest_peeked_layer_by_target_layer(target_layer);

                return Some(old_highest_peeked_layer.unwrap());
            } else {
                // We would need to unpeek (advance) layers above the boundary_layer, which is not allowed
                return None;
            }
        }
        // This needs to be adjusted before returning from the function
        self.highest_peeked_layer = None;

        // SAFETY: this requires that no other references to the layers below exist. We currently have no way of ensuring this.
        unsafe { self.try_to_go_to_layer(target_layer) };

        if self.input_trie_scan_current_layer < boundary_layer {
            // We would need to advance to even get down to the target layer, but are not allowed to because of the `boundary_layer`.
            return None;
        }

        // Layer with the smallest index where the trie scan has advanced to the next item, from the perspective of someone who consumes this trie scan
        // `self.input_trie_scan_current_layer` will immediately get advanced by the loop following, thus this is the initial value of this variable.
        let mut uppermost_advanced_layer_index = self.input_trie_scan_current_layer;

        // Traverse trie downwards to check if the value would actually exists in the materialized version of the trie
        // If at one layer there is no materialized value, move up, go to the next item, and move down again
        loop {
            // Go down one layer and check if there exists a item
            // If there exists no value, go upwards until either
            // a new value is found or we hit the the `boundary_layer`

            let has_next_value = self.advance_has_next_value();

            if !has_next_value {
                if !self.input_jump_type() {
                    if self.input_trie_scan_current_layer == boundary_layer {
                        // Boundary layer has been reached and has no next value

                        // `highest_peeked_layer`has already been set to None
                        return None;
                    }

                    self.input_up();

                    if self.input_trie_scan_current_layer < uppermost_advanced_layer_index {
                        uppermost_advanced_layer_index = self.input_trie_scan_current_layer;
                    }
                }
            } else if self.input_trie_scan_current_layer == self.input_trie_scan.arity() - 1 {
                // Lowest layer has been reached and an materialized value has been found

                // Mark layers below the `target_layer` as peeked
                self.set_highest_peeked_layer_by_target_layer(target_layer);

                return Some(uppermost_advanced_layer_index);
            } else {
                self.input_down();
            }
        }
    }

    fn clear_highest_peeked_layer(&mut self) {
        self.highest_peeked_layer = None;
    }
}

impl<'a> TrieScanPrune<'a> {
    /// Moves to the next value on a given layer while ensuring that only materialized tuples are returned (see guarantees provided by [`TrieScanPrune`]).
    ///
    /// It is assumed that the trie scan has been initialized by calling `down()` at least once.
    ///
    /// In the future and if needed, a similar function `advance_on_layer_with_seek()` might be exposed here, too (see `advance_on_layer_with_seek()` of [`crate::tabular::operations::triescan_prune::TrieScanPrune`]).
    ///
    ///   * `allow_advancements_above_target_layer` - Whether the underlying trie scan may be advanced on layers above the `target_layer`.
    ///     If this is set to `false`, the `advance_on_layer()` function has the same effect as calling `next()` on the column scan at the `target_layer`, meaning that layers above the `target_layer` are seen as read-only.
    ///     If this is set to `true`, layers above the `target_layer` once `next()` on the `target_layer` returns `None`. This allows to e.g. call `advance_on_layer()` on the bottommost layer to iterate though all the tuples of the underlying trie scan.
    ///
    /// Returns the uppermost layer (layer with lowest index) that has been advanced. This layer, and all the layers below, can have a new current value. This is relevant if `allow_advancements_above_target_layer` is set to `true`.
    ///
    /// # Panics
    ///
    /// If the underlying trie scan has not been initialized.
    pub(crate) fn advance_on_layer(
        &mut self,
        target_layer: usize,
        stay_in_type: Option<StorageTypeName>,
    ) -> Option<usize> {
        unsafe {
            assert!((*self.state.get()).initialized);
            (*self.state.get()).advance_on_layer(target_layer, stay_in_type)
        }
    }

    /// Resets the highest peeked layer.
    ///
    /// This is required when using the [`TrieScanPrune`] as a full [`TrieScan`], where there are no column peek semantics as in a [`PartialTrieScan`].
    pub(crate) fn clear_column_peeks(&mut self) {
        unsafe {
            assert!((*self.state.get()).initialized);
            (*self.state.get()).clear_highest_peeked_layer()
        }
    }

    /// Return the current value the input trie is on for the given layer.
    pub(crate) fn input_trie_value(&self, layer: usize) -> Option<StorageValueT> {
        unsafe {
            assert!((*self.state.get()).initialized);
            (*self.state.get()).current_input_trie_value(layer)
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanPrune<'a> {
    fn up(&mut self) {
        unsafe {
            (*self.state.get()).external_up();
        }
    }

    fn down(&mut self, next_type: StorageTypeName) {
        unsafe {
            (*self.state.get()).external_down(next_type);
        }
    }

    fn current_layer(&self) -> Option<usize> {
        unsafe { (*self.state.get()).current_layer() }
    }

    fn arity(&self) -> usize {
        unsafe { (*self.state.get()).arity() }
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.output_column_scans[layer]
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        unsafe { (*self.state.get()).possible_types(layer) }
    }
}

impl<'a> TrieScan for TrieScanPrune<'a> {
    fn advance_on_layer(&mut self, layer: usize) -> Option<usize> {
        if !unsafe { (*self.state.get()).initialized } {
            for current_layer in 0..=layer {
                let first_type =
                    unsafe { (*self.state.get()).possible_types[current_layer].first()? };
                self.down(*first_type);
            }
        }

        let uppermost_modified_column_index = self.advance_on_layer(layer, None);
        // Reset column peeks because this behavior is not wanted by the [`TrieScan`] trait
        self.clear_column_peeks();
        uppermost_modified_column_index
    }

    fn current_value(&mut self, layer: usize) -> StorageValueT {
        self.input_trie_value(layer)
            .expect("advance_at_layer needs to return Some before this is called")
    }

    fn num_columns(&self) -> usize {
        self.output_column_scans.len()
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::{StorageTypeName, StorageValueT},
        datavalues::AnyDataValue,
        dictionary::meta_dv_dict::MetaDvDictionary,
        management::database::Dict,
        tabular::{
            operations::{
                filter::{Filter, GeneratorFilter},
                join::test::generate_join_scan,
                OperationGenerator, OperationTable,
            },
            trie::Trie,
            triescan::{PartialTrieScan, TrieScanEnum},
        },
        util::test_util::test::{
            partial_scan_current, partial_scan_current_at_layer, partial_scan_next,
            partial_scan_seek, trie_dfs, trie_id32, trie_int64,
        },
    };

    use super::TrieScanPrune;

    /// Short helper function for creating `Some(StorageValueT::Int64(value))` from `value`.
    fn svo_i(value: i64) -> Option<StorageValueT> {
        Some(StorageValueT::Int64(value))
    }

    /// Short helper function for creating `Some(StorageValueT::Int64(value))` from `value`.
    fn sv_i(value: i64) -> StorageValueT {
        StorageValueT::Int64(value)
    }

    /// Creates an example trie with unmaterialized tuples
    fn create_example_trie() -> Trie {
        trie_int64(vec![
            &[1, 4, 0, 3],
            &[1, 4, 0, 7],
            &[1, 4, 1, 5],
            &[1, 4, 1, 7],
            &[1, 4, 2, 3],
            &[1, 4, 2, 4],
            &[1, 5, 1, 6],
            &[2, 4, 8, 7],
        ])
    }

    /// Creates an example trie with unmaterialized tuples
    fn create_example_trie_scan<'a>(
        dictionary: &'a RefCell<Dict>,
        input_trie: &'a Trie,
        layer_1_equality: i64,
        layer_3_equality: i64,
    ) -> TrieScanPrune<'a> {
        let scan_generic = TrieScanEnum::Generic(input_trie.partial_iterator());

        let input_markers = OperationTable::new_unique(input_trie.arity());
        let marker_layer_1 = *input_markers.get(1);
        let marker_layer_3 = *input_markers.get(3);

        let filters = vec![
            Filter::equals(
                Filter::reference(marker_layer_1),
                Filter::constant(AnyDataValue::new_integer_from_i64(layer_1_equality)),
            ),
            Filter::equals(
                Filter::reference(marker_layer_3),
                Filter::constant(AnyDataValue::new_integer_from_i64(layer_3_equality)),
            ),
        ];

        let generator = GeneratorFilter::new(input_markers, &filters);
        let scan_restrict = generator
            .generate(vec![Some(scan_generic)], dictionary)
            .unwrap();

        TrieScanPrune::new(scan_restrict)
    }

    #[test]
    fn test_no_effect_on_full_trie_scans() {
        let input_trie = trie_id32(vec![
            &[1, 2],
            &[1, 7],
            &[1, 9],
            &[2, 1],
            &[2, 3],
            &[2, 12],
            &[4, 1],
            &[4, 2],
            &[4, 5],
        ]);

        let mut scan = TrieScanPrune::new(TrieScanEnum::Generic(input_trie.partial_iterator()));

        trie_dfs(
            &mut scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1),  // x = 1
                StorageValueT::Id32(2),  // y = 2
                StorageValueT::Id32(7),  // y = 7
                StorageValueT::Id32(9),  // y = 9
                StorageValueT::Id32(2),  // x = 2
                StorageValueT::Id32(1),  // y = 1
                StorageValueT::Id32(3),  // y = 3
                StorageValueT::Id32(12), // y = 12
                StorageValueT::Id32(4),  // x = 4
                StorageValueT::Id32(1),  // y = 1
                StorageValueT::Id32(2),  // y = 2
                StorageValueT::Id32(5),  // y = 5
            ],
        );
    }

    #[test]
    fn test_skip_unmaterialized_tuples() {
        let dictionary = RefCell::new(MetaDvDictionary::default());
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&dictionary, &trie, 4, 7);

        trie_dfs(
            &mut scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(0), // z = 0
                StorageValueT::Int64(7), // v = 7
                StorageValueT::Int64(1), // z = 1
                StorageValueT::Int64(7), // v = 7
                StorageValueT::Int64(2), // x = 2
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(8), // z = 8
                StorageValueT::Int64(7), // v = 7
            ],
        );
    }

    #[test]
    fn test_empty_input_trie() {
        let dictionary = RefCell::new(MetaDvDictionary::default());
        let trie = create_example_trie();

        // Equality on lowest layer changed to 99 to prevent any matches and create trie scan without materialized tuples
        let mut scan = create_example_trie_scan(&dictionary, &trie, 4, 99);

        trie_dfs(&mut scan, &[StorageTypeName::Int64], &[]);
    }

    #[test]
    #[should_panic]
    fn test_advance_on_uninitialized_trie_scan_should_panic() {
        let dictionary = RefCell::new(MetaDvDictionary::default());
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&dictionary, &trie, 4, 7);

        scan.advance_on_layer(0, None);
    }

    #[test]
    fn test_advance_above_target_layer() {
        let dictionary = RefCell::new(MetaDvDictionary::default());
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&dictionary, &trie, 4, 7);

        let low = scan.arity() - 1;
        let ty = StorageTypeName::Int64;

        // Initialize trie
        scan.down(StorageTypeName::Int64);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), None);

        assert_eq!(scan.advance_on_layer(low, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), Some(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), None);
    }

    #[test]
    fn test_advance_highest_advanced_layer() {
        let trie = create_example_trie();
        let scan = TrieScanEnum::Generic(trie.partial_iterator());
        let mut scan = TrieScanPrune::new(scan);

        let low = scan.arity() - 1;
        let ty = StorageTypeName::Int64;

        // Initialize trie
        scan.down(StorageTypeName::Int64);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), None);

        assert_eq!(scan.advance_on_layer(low, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(3));

        assert_eq!(scan.advance_on_layer(low, None), Some(3));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), Some(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(5));

        assert_eq!(scan.advance_on_layer(low, None), Some(3));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), Some(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(3));

        assert_eq!(scan.advance_on_layer(low, None), Some(3));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(4));

        assert_eq!(scan.advance_on_layer(low, None), Some(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(6));

        assert_eq!(scan.advance_on_layer(low, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), svo_i(7));

        assert_eq!(scan.advance_on_layer(low, None), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, low), None);
    }

    #[test]
    fn test_advance_with_column_peeks() {
        let trie = create_example_trie();
        let scan = TrieScanEnum::Generic(trie.partial_iterator());
        let mut scan = TrieScanPrune::new(scan);

        let ty = StorageTypeName::Int64;

        // Initialize trie
        scan.down(ty);

        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);

        // Only advance on highest layer
        assert_eq!(scan.advance_on_layer(0, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);

        // On one layer below
        assert_eq!(scan.advance_on_layer(1, None), Some(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);

        // On lowest layer
        assert_eq!(scan.advance_on_layer(3, None), Some(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), svo_i(3));

        assert_eq!(scan.advance_on_layer(3, None), Some(3));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), svo_i(7));

        assert_eq!(scan.advance_on_layer(3, None), Some(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), svo_i(5));

        assert_eq!(scan.advance_on_layer(1, None), Some(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(5));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);

        // Advance on highest layer
        assert_eq!(scan.advance_on_layer(0, None), Some(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), None);
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);
    }

    #[ignore]
    #[test]
    fn test_advance_with_seek() {
        let trie = create_example_trie();
        let scan = TrieScanEnum::Generic(trie.partial_iterator());
        let mut scan = TrieScanPrune::new(scan);

        let ty = StorageTypeName::Int64;

        // Initialize trie
        scan.down(ty);
        assert_eq!(partial_scan_seek(&scan, sv_i(0)), svo_i(1));

        scan.down(ty);
        assert_eq!(partial_scan_seek(&scan, sv_i(0)), svo_i(4));

        scan.down(ty);
        assert_eq!(partial_scan_seek(&scan, sv_i(0)), svo_i(0));

        scan.down(ty);
        assert_eq!(partial_scan_seek(&scan, sv_i(5)), svo_i(7));
        assert_eq!(partial_scan_seek(&scan, sv_i(0)), svo_i(7));
        assert_eq!(partial_scan_seek(&scan, sv_i(7)), svo_i(7));

        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), svo_i(7));

        assert_eq!(partial_scan_seek(&scan, sv_i(8)), None);

        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(0));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);

        scan.up();
        assert_eq!(partial_scan_seek(&scan, sv_i(2)), svo_i(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 0), svo_i(1));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 1), svo_i(4));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 2), svo_i(2));
        assert_eq!(partial_scan_current_at_layer(&scan, ty, 3), None);
    }

    #[test]
    fn test_return_to_previous_layer() {
        let trie = create_example_trie();
        let scan = TrieScanEnum::Generic(trie.partial_iterator());
        let mut scan = TrieScanPrune::new(scan);

        let ty = StorageTypeName::Int64;

        // Initialize trie
        scan.down(ty);

        assert_eq!(partial_scan_next(&scan, ty), svo_i(1));
        assert_eq!(partial_scan_current(&scan, ty), svo_i(1));

        scan.down(ty);
        assert_eq!(partial_scan_current(&scan, ty), None);
        assert_eq!(partial_scan_next(&scan, ty), svo_i(4));
        assert_eq!(partial_scan_current(&scan, ty), svo_i(4));
        assert_eq!(partial_scan_next(&scan, ty), svo_i(5));
        assert_eq!(partial_scan_current(&scan, ty), svo_i(5));
        assert_eq!(partial_scan_next(&scan, ty), None);
        assert_eq!(partial_scan_current(&scan, ty), None);

        // Trie scan should return the same results after going up and down again
        scan.up();
        scan.down(ty);
        assert_eq!(partial_scan_current(&scan, ty), None);
        assert_eq!(partial_scan_next(&scan, ty), svo_i(4));
        assert_eq!(partial_scan_current(&scan, ty), svo_i(4));
        assert_eq!(partial_scan_next(&scan, ty), svo_i(5));
        assert_eq!(partial_scan_current(&scan, ty), svo_i(5));
        assert_eq!(partial_scan_next(&scan, ty), None);
        assert_eq!(partial_scan_current(&scan, ty), None);
    }

    #[ignore]
    #[test]
    fn test_partial_trie_scan_interface() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![
            &[1, 0],
            &[1, 3],
            &[1, 4],
            &[1, 5],
            &[2, 2],
            &[2, 4],
            &[2, 7],
        ]);
        let trie_b = trie_id32(vec![&[0], &[4]]);

        let scan_a = TrieScanEnum::Prune(TrieScanPrune::new(TrieScanEnum::Generic(
            trie_a.partial_iterator(),
        )));
        let scan_b = TrieScanEnum::Prune(TrieScanPrune::new(TrieScanEnum::Generic(
            trie_b.partial_iterator(),
        )));

        let join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y"],
            vec![(scan_a, vec!["x", "y"]), (scan_b, vec!["y"])],
        );

        let prune = TrieScanPrune::new(join_scan);
        let result = Trie::from_full_trie_scan(prune, 0)
            .row_iterator()
            .collect::<Vec<_>>();

        let expected = vec![vec![sv_i(0)], vec![sv_i(4)], vec![sv_i(4)]];

        assert_eq!(result, expected);
    }
}
