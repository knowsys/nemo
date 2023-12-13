//! This module defines [TrieScanPrune].

use std::{cell::UnsafeCell, rc::Rc};

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::ColumnScanPrune,
    },
    datatypes::{ColumnDataType, StorageTypeName, StorageValueT},
    dictionary::meta_dv_dict::MetaDictionary,
    tabular::triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
};

use super::OperationGenerator;

/// [`TrieScan`] which only returns values that would actually exists during materialization.
///
/// This trie scan given the following guarantees for the [`TrieScan`] API:
///   * For every value returned by a column using `next` or `current`, there exists values for all the lower layers (they can be retrieved using `down()` and `next()`).
///   * The trie scan does not skip any tuples.
///
/// To achieve this behavior, before returning a value from `next()`, the input trie is traversed downwards to check if the value would exists in a materialized version of the trie scan.
/// Therefore, every column and the trie scan itself has shared access to the input trie and associated state, through [`SharedTrieScanPruneState`].
#[derive(Debug)]
pub struct TrieScanPrune<'a> {
    state: SharedTrieScanPruneState<'a>,
    output_column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

/// Used to create a [TrieScanPrune]
#[derive(Debug, Clone, Copy)]
pub struct GeneratorPrune {}

impl OperationGenerator for GeneratorPrune {
    fn generate<'a>(
        &'_ self,
        mut input: Vec<TrieScanEnum<'a>>,
        _dictionary: &'a MetaDictionary,
    ) -> TrieScanEnum<'a> {
        debug_assert!(input.len() == 1);
        let input_trie_scan = input.remove(0);
        let input_trie_scan_arity = input_trie_scan.arity();

        let mut output_column_scans =
            Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(input_trie_scan.arity());

        let state = Rc::new(UnsafeCell::new(TrieScanPruneState {
            input_trie_scan,
            input_trie_scan_current_layer: 0,
            highest_peeked_layer: None,
            initialized: false,
            external_current_layer: 0,
            external_path_types: Vec::new(),
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

                    ColumnScanEnum::ColumnScanPrune(ColumnScanPrune::new(
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

        TrieScanEnum::TrieScanPrune(TrieScanPrune {
            state,
            output_column_scans,
        })
    }
}

/// Allows for shared access to the input trie and it's columns.
/// This is required because every output column (`ColumnScanPrune`) needs to
/// (at least indirectly) call `up` and `down` on the input trie.
///
/// `Rc<UnsafeCell<_>>>` is required here, because we cannot guarantee that the trie scan exists longer than the individual output columns.
/// The overhead after initialization should be negligible and the same as a single pointer indirection.
pub type SharedTrieScanPruneState<'a> = Rc<UnsafeCell<TrieScanPruneState<'a>>>;

/// State which is shared with the individual output column scans and the trie scan
#[derive(Debug)]
pub struct TrieScanPruneState<'a> {
    /// Trie scan which is being pruned
    input_trie_scan: TrieScanEnum<'a>,
    /// Whether the first `external_down()` has been made (to go to layer `0`)
    initialized: bool,
    /// Current column scan layer of the input trie scan
    /// Layer zero is at to top of the trie scan
    input_trie_scan_current_layer: usize,
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
    pub fn external_up(&mut self) {
        debug_assert!(self.initialized);

        if self.external_current_layer == 0 {
            // Move above the first input layer, thus resetting the input trie scan
            self.input_trie_scan.up();
            self.initialized = false;
            self.highest_peeked_layer = None;
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
        self.highest_peeked_layer = self.highest_peeked_layer.map(|highest_peeked_layer| {
            std::cmp::min(highest_peeked_layer, self.external_current_layer)
        });
    }

    /// Increments the `external_current_layer`
    pub fn external_down(&mut self, storage_type: StorageTypeName) {
        if !self.initialized {
            // First down initializes the trie scan
            self.initialized = true;
            self.input_trie_scan.down(storage_type);
            return;
        }

        assert!(self.external_current_layer < self.input_trie_scan.arity() - 1);

        self.external_current_layer += 1;
        self.external_path_types.push(storage_type);
    }

    /// Return the number of columns in the input trie.
    pub fn arity(&self) -> usize {
        self.input_trie_scan.arity()
    }

    /// Return the types of each active layer in this scan.
    pub fn path_types(&self) -> &[StorageTypeName] {
        &self.external_path_types
    }

    fn input_up(&mut self) {
        assert!(self.input_trie_scan_current_layer > 0);

        self.input_trie_scan.up();
        self.input_trie_scan_current_layer -= 1;
    }

    fn input_down(&mut self) {
        assert!(self.input_trie_scan_current_layer < self.input_trie_scan.arity() - 1);

        self.input_trie_scan.down(StorageTypeName::Id32);
        self.input_trie_scan_current_layer += 1;
    }

    fn input_jump_type(&mut self) -> bool {
        let next_type = match self.input_trie_scan.path_types()[self.input_trie_scan_current_layer]
        {
            StorageTypeName::Id32 => Some(StorageTypeName::Id64),
            StorageTypeName::Id64 => Some(StorageTypeName::Int64),
            StorageTypeName::Int64 => Some(StorageTypeName::Float),
            StorageTypeName::Float => Some(StorageTypeName::Double),
            StorageTypeName::Double => None,
        };

        if let Some(next_type) = next_type {
            self.input_trie_scan.up();
            self.input_trie_scan.down(next_type);

            true
        } else {
            false
        }
    }

    /// Returns the current layer of this trie scan
    pub fn current_layer(&self) -> Option<usize> {
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
            if self.current_value().is_none() {
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
    pub fn is_column_peeked(
        &self,
        index: usize,
        storage_type_opt: Option<StorageTypeName>,
    ) -> bool {
        self.highest_peeked_layer.map_or(false, |p| {
            index >= p
                && if let Some(storage_type) = storage_type_opt {
                    self.input_trie_scan.path_types()[index] == storage_type
                } else {
                    true
                }
        })
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    #[inline]
    pub unsafe fn current_input_trie_value(&mut self, index: usize) -> Option<StorageValueT> {
        let current_input_type = self.input_trie_scan.path_types()[index];
        let scan = self.input_trie_scan.scan(index);

        unsafe { (*scan.get()).current(current_input_type) }
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no immutable/mutable references to the column scan at `index` and thus the value inside the [`UnsafeCell`] is safe to mutate.
    #[inline]
    unsafe fn next_input_trie_value(&mut self) -> Option<StorageValueT> {
        let current_input_type =
            self.input_trie_scan.path_types()[self.input_trie_scan_current_layer];
        let scan = self
            .input_trie_scan
            .scan(self.input_trie_scan_current_layer);

        unsafe { (*scan.get()).next(current_input_type) }
    }

    /// Directly gets the current output value for a column ignoring layer peeks (see `highest_peeked_layer`).
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    ///
    /// TODO: Update to allow for direct access
    #[inline]
    unsafe fn current_value(&self) -> Option<StorageValueT> {
        debug_assert!(self.initialized);

        let current_input_type =
            self.input_trie_scan.path_types()[self.input_trie_scan_current_layer];
        let scan = self
            .input_trie_scan
            .scan(self.input_trie_scan_current_layer);

        unsafe { &*scan.get() }.current(current_input_type)
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
    pub fn advance_on_layer_with_seek<T: ColumnDataType>(
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
    pub fn advance_on_layer(
        &mut self,
        target_layer: usize,
        stay_in_type: Option<StorageTypeName>,
    ) -> Option<usize> {
        debug_assert!(self.initialized);

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
            if self.highest_peeked_layer.unwrap() == target_layer || stay_in_type.is_some() {
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
                if self.input_trie_scan_current_layer == boundary_layer {
                    // Boundary layer has been reached and has no next value

                    // `highest_peeked_layer`has already been set to None
                    return None;
                }

                if !self.input_jump_type() {
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
    pub fn advance_on_layer(
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
    pub fn clear_column_peeks(&mut self) {
        unsafe {
            assert!((*self.state.get()).initialized);
            (*self.state.get()).clear_highest_peeked_layer()
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

    fn path_types(&self) -> &[StorageTypeName] {
        unsafe { (*self.state.get()).path_types() }
    }

    fn arity(&self) -> usize {
        unsafe { (*self.state.get()).arity() }
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.output_column_scans[layer]
    }
}

impl<'a> TrieScan for TrieScanPrune<'a> {
    fn advance_on_layer(&mut self, layer: usize) -> Option<usize> {
        if !unsafe { (*self.state.get()).initialized } {
            for _ in 0..=layer {
                self.down(StorageTypeName::Id32);
            }
        }

        let uppermost_modified_column_index = self.advance_on_layer(layer, None);
        // Reset column peeks because this behavior is not wanted by the [`TrieScan`] trait
        self.clear_column_peeks();
        uppermost_modified_column_index
    }

    fn current(&mut self, layer: usize) -> StorageValueT {
        let current_type = self.path_types()[layer];

        self.output_column_scans[layer]
            .get_mut()
            .current(current_type)
            .expect("advance_at_layer needs to return Some before this is called")
    }
}

// #[cfg(test)]
// mod test {
//     use super::TrieScanPrune;

//     use crate::arithmetic::expression::StackValue;
//     use crate::columnar::column::interval::ColumnWithIntervalsT;
//     use crate::columnar::column::Column;
//     use crate::columnar::columnscan::ColumnScanT;
//     use crate::condition::statement::ConditionStatement;
//     use crate::datatypes::DataValueT;
//     use crate::management::database::Dict;
//     use crate::tabular::operations::{
//         materialize, JoinBindings, TrieScanJoin, TrieScanRestrictValues,
//     };
//     use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
//     use crate::tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum};

//     use crate::util::test_util::make_column_with_intervals_t;
//     use test_log::test;

//     fn get_next_scan_item(scan: &mut TrieScanPrune) -> Option<u64> {
//         if let ColumnScanT::Id64(rcs) = scan.current_scan().unwrap() {
//             rcs.next()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     fn get_seek_scan_item(scan: &mut TrieScanPrune, seek_value: u64) -> Option<u64> {
//         if let ColumnScanT::Id64(rcs) = scan.current_scan().unwrap() {
//             rcs.seek(seek_value)
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     fn get_current_scan_item(scan: &mut TrieScanPrune) -> Option<u64> {
//         if let ColumnScanT::Id64(rcs) = scan.current_scan().unwrap() {
//             rcs.current()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     fn get_current_scan_item_at_layer(scan: &mut TrieScanPrune, layer_index: usize) -> Option<u64> {
//         if let ColumnScanT::Id64(rcs) = unsafe { &*scan.get_scan(layer_index).unwrap().get() } {
//             rcs.current()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     /// Creates an example trie with unmaterialized tuples
//     fn create_example_trie() -> Trie {
//         let column_fst = make_column_with_intervals_t(&[1, 2], &[0, 1]);
//         let column_snd = make_column_with_intervals_t(&[4, 5, 4], &[0, 2]);
//         let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1, 8], &[0, 3, 4]);
//         let column_fth = make_column_with_intervals_t(&[3, 7, 5, 7, 3, 4, 6, 7], &[0, 2, 4, 6, 7]);

//         Trie::new(vec![column_fst, column_snd, column_trd, column_fth])
//     }

//     /// Creates an example trie with unmaterialized tuples
//     fn create_example_trie_scan(
//         input_trie: &Trie,
//         layer_1_equality: u64,
//         layer_3_equality: u64,
//     ) -> TrieScanPrune {
//         let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(input_trie));

//         let mut dict = Dict::default();
//         let scan = TrieScanEnum::TrieScanRestrictValues(TrieScanRestrictValues::new(
//             &mut dict,
//             scan,
//             &[
//                 ConditionStatement::equal(
//                     StackValue::Reference(1),
//                     StackValue::Constant(DataValueT::U64(layer_1_equality)),
//                 ),
//                 ConditionStatement::equal(
//                     StackValue::Reference(3),
//                     StackValue::Constant(DataValueT::U64(layer_3_equality)),
//                 ),
//             ],
//         ));
//         let scan = TrieScanPrune::new(scan);

//         scan
//     }

//     #[test]
//     fn test_no_effect_on_full_trie_scans() {
//         let column_a = make_column_with_intervals_t(&[1, 2, 4], &[0]);
//         let column_b = make_column_with_intervals_t(&[2, 7, 9, 1, 3, 12, 1, 2, 5], &[0, 3, 6]);

//         let input_trie = Trie::new(vec![column_a, column_b]);

//         let mut scan = TrieScanPrune::new(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
//             &input_trie,
//         )));

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(2));
//         assert_eq!(get_current_scan_item(&mut scan), Some(2));
//         assert_eq!(get_next_scan_item(&mut scan), Some(7));
//         assert_eq!(get_current_scan_item(&mut scan), Some(7));
//         assert_eq!(get_next_scan_item(&mut scan), Some(9));
//         assert_eq!(get_current_scan_item(&mut scan), Some(9));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);

//         scan.up();
//         assert_eq!(get_next_scan_item(&mut scan), Some(2));
//         assert_eq!(get_current_scan_item(&mut scan), Some(2));

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));
//         assert_eq!(get_next_scan_item(&mut scan), Some(3));
//         assert_eq!(get_current_scan_item(&mut scan), Some(3));
//         assert_eq!(get_next_scan_item(&mut scan), Some(12));
//         assert_eq!(get_current_scan_item(&mut scan), Some(12));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);

//         scan.up();
//         assert_eq!(get_next_scan_item(&mut scan), Some(4));
//         assert_eq!(get_current_scan_item(&mut scan), Some(4));

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));
//         assert_eq!(get_next_scan_item(&mut scan), Some(2));
//         assert_eq!(get_current_scan_item(&mut scan), Some(2));
//         assert_eq!(get_next_scan_item(&mut scan), Some(5));
//         assert_eq!(get_current_scan_item(&mut scan), Some(5));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//     }

//     #[test]
//     fn test_skip_unmaterialized_tuples() {
//         let trie = create_example_trie();
//         let mut scan = create_example_trie_scan(&trie, 4, 7);

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(4));
//         assert_eq!(get_current_scan_item(&mut scan), Some(4));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(0));
//         assert_eq!(get_current_scan_item(&mut scan), Some(0));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(7));
//         assert_eq!(get_current_scan_item(&mut scan), Some(7));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         scan.up();
//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(7));
//         assert_eq!(get_current_scan_item(&mut scan), Some(7));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         scan.up();
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         scan.up();
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         scan.up();
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));
//         assert_eq!(get_next_scan_item(&mut scan), Some(2));
//         assert_eq!(get_current_scan_item(&mut scan), Some(2));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(4));
//         assert_eq!(get_current_scan_item(&mut scan), Some(4));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(8));
//         assert_eq!(get_current_scan_item(&mut scan), Some(8));
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(7));
//         assert_eq!(get_current_scan_item(&mut scan), Some(7));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//     }

//     #[test]
//     fn test_empty_input_trie() {
//         let trie = create_example_trie();
//         // Equality on lowest layer changed to 99 to prevent any matches and create trie scan without materialized tuples
//         let mut scan = create_example_trie_scan(&trie, 4, 99);

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//     }

//     #[test]
//     #[should_panic]
//     fn test_advance_on_uninitialized_trie_scan_should_panic() {
//         let trie = create_example_trie();
//         let mut scan = create_example_trie_scan(&trie, 4, 7);

//         scan.advance_on_layer(0, false);
//     }

//     #[test]
//     fn test_advance_above_target_layer() {
//         let trie = create_example_trie();
//         let mut scan = create_example_trie_scan(&trie, 4, 7);

//         // Initialize trie
//         scan.down();

//         let lowest_layer_index = scan.get_types().len() - 1;

//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             None
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(2));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), None);
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             None
//         );
//     }

//     #[test]
//     fn test_advance_highest_advanced_layer() {
//         let trie = create_example_trie();
//         let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));
//         let mut scan = TrieScanPrune::new(scan);

//         // Initialize trie
//         scan.down();

//         let lowest_layer_index = scan.get_types().len() - 1;

//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             None
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(3)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(3));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(2));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(5)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(3));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(2));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(3)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(3));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(4)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(1));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(6)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             Some(7)
//         );
//         assert_eq!(scan.advance_on_layer(lowest_layer_index, true), None);
//         assert_eq!(
//             get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
//             None
//         );
//     }

//     #[test]
//     fn test_advance_with_column_peeks() {
//         let trie = create_example_trie();
//         let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));
//         let mut scan = TrieScanPrune::new(scan);

//         // Initialize trie
//         scan.down();

//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);

//         // Only advance on highest layer
//         assert_eq!(scan.advance_on_layer(0, true), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//         // On one layer below
//         assert_eq!(scan.advance_on_layer(1, true), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//         // On lowest layer
//         assert_eq!(scan.advance_on_layer(3, true), Some(2));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), Some(3));

//         assert_eq!(scan.advance_on_layer(3, true), Some(3));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), Some(7));

//         assert_eq!(scan.advance_on_layer(3, true), Some(2));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), Some(5));

//         assert_eq!(scan.advance_on_layer(1, true), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(5));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//         // Advance on highest layer
//         assert_eq!(scan.advance_on_layer(0, true), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(2));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//     }

//     #[test]
//     fn test_advance_with_seek() {
//         let trie = create_example_trie();
//         let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));
//         let mut scan = TrieScanPrune::new(scan);

//         // Initialize trie
//         scan.down();

//         assert_eq!(get_seek_scan_item(&mut scan, 0), Some(1));
//         scan.down();
//         assert_eq!(get_seek_scan_item(&mut scan, 0), Some(4));
//         scan.down();
//         assert_eq!(get_seek_scan_item(&mut scan, 0), Some(0));
//         scan.down();
//         assert_eq!(get_seek_scan_item(&mut scan, 5), Some(7));
//         assert_eq!(get_seek_scan_item(&mut scan, 0), Some(7));
//         assert_eq!(get_seek_scan_item(&mut scan, 7), Some(7));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), Some(7));
//         assert_eq!(get_seek_scan_item(&mut scan, 8), None);
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(0));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//         scan.up();
//         assert_eq!(get_seek_scan_item(&mut scan, 2), Some(2));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 0), Some(1));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 1), Some(4));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 2), Some(2));
//         assert_eq!(get_current_scan_item_at_layer(&mut scan, 3), None);
//     }

//     #[test]
//     fn test_return_to_previous_layer() {
//         let trie = create_example_trie();
//         let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));
//         let mut scan = TrieScanPrune::new(scan);

//         // Initialize trie
//         scan.down();

//         assert_eq!(get_next_scan_item(&mut scan), Some(1));
//         assert_eq!(get_current_scan_item(&mut scan), Some(1));

//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(4));
//         assert_eq!(get_current_scan_item(&mut scan), Some(4));
//         assert_eq!(get_next_scan_item(&mut scan), Some(5));
//         assert_eq!(get_current_scan_item(&mut scan), Some(5));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);

//         // Trie scan should return the same results after going up and down again
//         scan.up();
//         scan.down();
//         assert_eq!(get_current_scan_item(&mut scan), None);
//         assert_eq!(get_next_scan_item(&mut scan), Some(4));
//         assert_eq!(get_current_scan_item(&mut scan), Some(4));
//         assert_eq!(get_next_scan_item(&mut scan), Some(5));
//         assert_eq!(get_current_scan_item(&mut scan), Some(5));
//         assert_eq!(get_next_scan_item(&mut scan), None);
//         assert_eq!(get_current_scan_item(&mut scan), None);
//     }

//     #[test]
//     fn test_partial_trie_scan_interface() {
//         let column_a_x = make_column_with_intervals_t(&[1, 2], &[0, 1]);
//         let column_a_y = make_column_with_intervals_t(&[0, 3, 4, 5, 2, 4, 7], &[0, 4]);
//         let column_b = make_column_with_intervals_t(&[0, 4], &[0]);

//         let trie_a = Trie::new(vec![column_a_x, column_a_y]);
//         let trie_b = Trie::new(vec![column_b]);

//         let scan_a = TrieScanEnum::TrieScanPrune(TrieScanPrune::new(
//             TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
//         ));
//         let scan_b = TrieScanEnum::TrieScanPrune(TrieScanPrune::new(
//             TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
//         ));

//         let scan_join = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
//             vec![scan_a, scan_b],
//             &JoinBindings::new(vec![vec![0, 1], vec![1]]),
//         ));

//         let result = materialize(&mut TrieScanPrune::new(scan_join)).unwrap();

//         let expected_data = vec![0u64, 4, 4];
//         let expected_interval = vec![0usize, 2];

//         let (data, interval) = if let ColumnWithIntervalsT::Id64(column) = result.get_column(1) {
//             (
//                 column.get_data_column().iter().collect::<Vec<u64>>(),
//                 column.get_int_column().iter().collect::<Vec<usize>>(),
//             )
//         } else {
//             unreachable!()
//         };

//         assert_eq!(expected_data, data);
//         assert_eq!(expected_interval, interval);
//     }
// }
