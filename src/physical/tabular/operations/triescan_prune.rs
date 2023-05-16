use crate::physical::columnar::operations::ColumnScanPrune;
use crate::physical::columnar::traits::columnscan::ColumnScan;
use crate::physical::datatypes::{ColumnDataType, StorageValueT};
use crate::physical::{
    columnar::traits::columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanT},
    datatypes::StorageTypeName,
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
};
use std::cell::UnsafeCell;
use std::{fmt::Debug, rc::Rc};

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
    output_column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
    target_types: Vec<StorageTypeName>,
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
}

impl<'a> TrieScanPruneState<'a> {
    /// Decrements the [`external_current_layer`]
    pub fn external_up(&mut self) {
        debug_assert!(self.initialized);
        assert!(self.external_current_layer > 0);

        self.external_current_layer -= 1;
    }

    /// Increments the [`external_current_layer`]
    pub fn external_down(&mut self) {
        if !self.initialized {
            self.initialized = true;
            self.input_trie_scan.down();
            return;
        }

        assert!(self.external_current_layer < self.input_trie_scan.get_types().len() - 1);

        self.external_current_layer += 1;
    }

    fn up(&mut self) {
        assert!(self.input_trie_scan_current_layer > 0);

        self.input_trie_scan.up();
        self.input_trie_scan_current_layer -= 1;
    }

    fn down(&mut self) {
        assert!(self.input_trie_scan_current_layer < self.input_trie_scan.get_types().len() - 1);

        self.input_trie_scan.down();
        self.input_trie_scan_current_layer += 1;
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
        assert!(target_layer < self.input_trie_scan.get_types().len());

        // Move up or down to the correct layer
        // Note that the uppermost layer has index 0

        for _ in target_layer..self.input_trie_scan_current_layer {
            self.up();
        }
        for _ in self.input_trie_scan_current_layer..target_layer {
            if self
                .current_value(self.input_trie_scan_current_layer)
                .is_none()
            {
                return false;
            }
            self.down();
        }

        true
    }

    /// Returns whether a column has been peeked already.
    ///
    /// See [`TrieScanPruneState`] for more information.
    #[inline]
    pub fn is_column_peeked(&self, index: usize) -> bool {
        self.highest_peeked_layer.map_or(false, |p| index >= p)
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    #[inline]
    pub unsafe fn current_input_trie_value(&mut self, index: usize) -> Option<StorageValueT> {
        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { (*scan.get()).current() }
    }

    /// # Safety
    ///
    /// The caller must ensure that there exists no immutable/mutable references to the column scan at `index` and thus the value inside the [`UnsafeCell`] is safe to mutate.
    #[inline]
    unsafe fn next_input_trie_value(&mut self, index: usize) -> Option<StorageValueT> {
        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { (*scan.get()).next() }
    }

    /// Directly gets the current output value for a column ignoring layer peeks (see `highest_peeked_layer`).
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    ///
    /// TODO: Update to allow for direct access
    #[inline]
    unsafe fn current_value(&self, index: usize) -> Option<StorageValueT> {
        debug_assert!(self.initialized);

        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { &*scan.get() }.current()
    }

    /// Helper method for the `advance_on_layer()` and `advance_on_layer_with_seek()`
    ///
    /// Goes to next value without checking layer peeks (see `highest_peeked_layer`), and returns if a value exists.
    fn advance_has_next_value(&mut self) -> bool {
        // SAFETY: this requires that no other references to the current layer scan exist. We currently have no way of ensuring this.
        let next_value = unsafe { self.next_input_trie_value(self.input_trie_scan_current_layer) };

        next_value.is_some()
    }

    /// Marks layers below the `target_layer` as peeked. Should only be used by the `advance()` functions.
    fn set_highest_peeked_layer_by_target_layer(&mut self, target_layer: usize) {
        self.highest_peeked_layer = if target_layer == self.input_trie_scan.get_types().len() - 1 {
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
        target_layer: usize,
        allow_advancements_above_target_layer: bool,
        underlying_column_scan: &'a ColumnScanCell<'a, T>,
        seek_minimum_value: T,
    ) -> Option<T> {
        debug_assert!(self.initialized);

        // Semantics of the `boundary_layer`:
        //   * If we're at the boundary layer, we may not call `up()`.
        //   * If we're above the boundary layer, we may not call `up()` or advance the layer (using `next()` or by removing layer peeks)
        let boundary_layer = if allow_advancements_above_target_layer {
            0
        } else {
            target_layer
        };

        // Fully handle layer peeks before advancing any layers
        if self.is_column_peeked(target_layer) {
            if self.highest_peeked_layer.unwrap() == target_layer
                || allow_advancements_above_target_layer
            {
                // Just remove the peek to advance the layer
                // Check if the value behind the peek satisfies the seek condition, otherwise continue search after removing the peek
                let next_value = underlying_column_scan.current();
                if next_value.unwrap() >= seek_minimum_value {
                    // Mark layers below target layer as peeked
                    self.set_highest_peeked_layer_by_target_layer(target_layer);

                    return Some(next_value.unwrap());
                }
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
            // We would need to advance to even get to the target layer, but are not allowed to because of the `boundary_layer`.
            return None;
        }

        // Whether seek was already called on the trie scan of the target layer
        // In this case `next` can be called instead
        let mut perform_next_instead_of_seek = false;

        let mut return_value = None;

        // Traverse trie downwards to check if the value would actually exists in the materialized version of the trie
        // If at one layer there is no materialized value, move up, go to the next item, and move down again
        loop {
            // Go down one layer and check if there exists a item
            // If there exists no value, go upwards until either
            // a new value is found or we hit the the `boundary_layer`

            let has_next_value = if self.input_trie_scan_current_layer == target_layer {
                if perform_next_instead_of_seek {
                    // Perform `next` instead of `seek` until we go above the `target_layer` again
                    perform_next_instead_of_seek = true;

                    return_value = underlying_column_scan.seek(seek_minimum_value);
                    return_value.is_some()
                } else {
                    return_value = underlying_column_scan.next();
                    return_value.is_some()
                }
            } else {
                // On other layers than the `target_layer` there are no seeks going on, only `next()`
                self.advance_has_next_value()
            };

            if has_next_value {
                if self.input_trie_scan_current_layer == boundary_layer {
                    // Boundary layer has been reached and has no next value

                    // `highest_peeked_layer`has already been set to None
                    return None;
                } else {
                    self.up();
                    if self.input_trie_scan_current_layer < target_layer {
                        perform_next_instead_of_seek = true;
                    }
                }
            } else if self.input_trie_scan_current_layer
                == self.input_trie_scan.get_types().len() - 1
            {
                // Lowest layer has been reached and an materialized value has been found

                // Mark layers below the `target_layer` as peeked
                self.set_highest_peeked_layer_by_target_layer(target_layer);

                return return_value;
            } else {
                self.down();
            }
        }
    }

    /// Moves to the next value on a given layer while ensuring that only materialized tuples are returned (see guarantees provided by [`TrieScanPrune`]).
    ///
    /// See documentation of function `advance_on_layer()` of [`TrieScanPrune`].
    pub fn advance_on_layer(
        &mut self,
        target_layer: usize,
        allow_advancements_above_target_layer: bool,
    ) -> Option<usize> {
        debug_assert!(self.initialized);

        // Semantics of the `boundary_layer`:
        //   * If we're at the boundary layer, we may not call `up()`.
        //   * If we're above the boundary layer, we may not call `up()` or advance the layer (using `next()` or by removing layer peeks)
        let boundary_layer = if allow_advancements_above_target_layer {
            0
        } else {
            target_layer
        };

        // Fully handle layer peeks before advancing any layers
        if self.is_column_peeked(target_layer) {
            if self.highest_peeked_layer.unwrap() == target_layer
                || allow_advancements_above_target_layer
            {
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

                self.up();

                if self.input_trie_scan_current_layer < uppermost_advanced_layer_index {
                    uppermost_advanced_layer_index = self.input_trie_scan_current_layer;
                }
            } else if self.input_trie_scan_current_layer
                == self.input_trie_scan.get_types().len() - 1
            {
                // Lowest layer has been reached and an materialized value has been found

                // Mark layers below the `target_layer` as peeked
                self.set_highest_peeked_layer_by_target_layer(target_layer);

                return Some(uppermost_advanced_layer_index);
            } else {
                self.down();
            }
        }
    }
}

impl<'a> TrieScanPrune<'a> {
    /// Construct new [`TrieScanPrune`] object.
    pub fn new(input_trie_scan: TrieScanEnum<'a>) -> Self {
        let target_types = input_trie_scan.get_types().clone();

        let mut output_column_scans: Vec<UnsafeCell<ColumnScanT<'a>>> = Vec::new();

        let state = Rc::new(UnsafeCell::new(TrieScanPruneState {
            input_trie_scan,
            input_trie_scan_current_layer: 0,
            highest_peeked_layer: None,
            initialized: false,
            external_current_layer: 0,
        }));

        // Create one `ColumnScanPrune` for every input column
        for (i, target_type) in target_types.iter().enumerate() {
            // Generate code for every possible data type of the input column
            macro_rules! create_column_scan_for_storage_type {
                ($variant:ident, $type:ty) => {{
                    // Get input column scan
                    // SAFETY: we're the only one accessing the shared state at this moment
                    let column_scan_cell = unsafe { (*state.get()).input_trie_scan.get_scan(i) };

                    let column_scan_cell = column_scan_cell.unwrap();

                    // Create output column scan
                    if let ColumnScanT::$variant(referenced_scan_cell) =
                        unsafe { &*column_scan_cell.get() }
                    {
                        output_column_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                            ColumnScanCell::new(ColumnScanEnum::ColumnScanPrune(
                                ColumnScanPrune::new(Rc::clone(&state), i, referenced_scan_cell),
                            )),
                        )));
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($type));
                    }
                }};
            }

            match target_type {
                StorageTypeName::U32 => create_column_scan_for_storage_type!(U32, u32),
                StorageTypeName::U64 => create_column_scan_for_storage_type!(U64, u64),
                StorageTypeName::I64 => create_column_scan_for_storage_type!(I64, i64),
                StorageTypeName::Float => create_column_scan_for_storage_type!(Float, Float),
                StorageTypeName::Double => create_column_scan_for_storage_type!(Double, Double),
            };
        }

        Self {
            state,
            output_column_scans,
            target_types,
        }
    }

    /// Moves to the next value on a given layer while ensuring that only materialized tuples are returned (see guarantees provided by [`TrieScanPrune`]).
    ///
    /// It is assumed that the trie scan has been initialized by calling `down()` at least once.
    ///
    /// In the future and if needed, a similar function `advance_on_layer_with_seek()` might be exposed here, too (see `advance_on_layer_with_seek()` of [`crate::physical::tabular::operations::triescan_prune::TrieScanPrune`]).
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
        allow_advancements_above_target_layer: bool,
    ) -> Option<usize> {
        unsafe {
            assert!((*self.state.get()).initialized);
            (*self.state.get())
                .advance_on_layer(target_layer, allow_advancements_above_target_layer)
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanPrune<'a> {
    fn up(&mut self) {
        unsafe {
            (*self.state.get()).external_up();
        }
    }

    fn down(&mut self) {
        unsafe {
            (*self.state.get()).external_down();
        }
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        let current_layer = unsafe {
            if !(*self.state.get()).initialized {
                return None;
            }

            (*self.state.get()).external_current_layer
        };

        Some(self.output_column_scans[current_layer].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        self.output_column_scans.get(index)
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.target_types
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanPrune;
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::datatypes::DataValueT;
    use crate::physical::management::database::Dict;
    use crate::physical::tabular::operations::{TrieScanSelectValue, ValueAssignment};
    use crate::physical::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};

    use crate::physical::util::interval::Interval;
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn get_next_scan_item(scan: &mut TrieScanPrune) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan().unwrap() {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn get_current_scan_item(scan: &mut TrieScanPrune) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan().unwrap() {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    fn get_current_scan_item_at_layer(scan: &mut TrieScanPrune, layer_index: usize) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &*scan.get_scan(layer_index).unwrap().get() } {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    /// Creates an example trie with unmaterialized tuples
    fn create_example_trie() -> Trie {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0, 1]);
        let column_snd = make_column_with_intervals_t(&[4, 5, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1, 8], &[0, 3, 4]);
        let column_fth = make_column_with_intervals_t(&[3, 7, 5, 7, 3, 4, 6, 7], &[0, 2, 4, 6, 7]);

        Trie::new(vec![column_fst, column_snd, column_trd, column_fth])
    }

    /// Creates an example trie with unmaterialized tuples
    fn create_example_trie_scan(
        input_trie: &Trie,
        layer_1_equality: u64,
        layer_3_equality: u64,
    ) -> TrieScanPrune {
        let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(input_trie));

        let mut dict = Dict::default();
        let scan = TrieScanEnum::TrieScanSelectValue(TrieScanSelectValue::new(
            &mut dict,
            scan,
            &[
                ValueAssignment {
                    column_idx: 1,
                    interval: Interval::single(DataValueT::U64(layer_1_equality)),
                },
                ValueAssignment {
                    column_idx: 3,
                    interval: Interval::single(DataValueT::U64(layer_3_equality)),
                },
            ],
        ));
        let scan = TrieScanPrune::new(scan);

        scan
    }

    #[test]
    fn test_no_effect_on_full_trie_scans() {
        let column_a = make_column_with_intervals_t(&[1, 2, 4], &[0]);
        let column_b = make_column_with_intervals_t(&[2, 7, 9, 1, 3, 12, 1, 2, 5], &[0, 3, 6]);

        let input_trie = Trie::new(vec![column_a, column_b]);

        let mut scan = TrieScanPrune::new(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
            &input_trie,
        )));

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(1));
        assert_eq!(get_current_scan_item(&mut scan), Some(1));

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(2));
        assert_eq!(get_current_scan_item(&mut scan), Some(2));
        assert_eq!(get_next_scan_item(&mut scan), Some(7));
        assert_eq!(get_current_scan_item(&mut scan), Some(7));
        assert_eq!(get_next_scan_item(&mut scan), Some(9));
        assert_eq!(get_current_scan_item(&mut scan), Some(9));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);

        scan.up();
        assert_eq!(get_next_scan_item(&mut scan), Some(2));
        assert_eq!(get_current_scan_item(&mut scan), Some(2));

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(1));
        assert_eq!(get_current_scan_item(&mut scan), Some(1));
        assert_eq!(get_next_scan_item(&mut scan), Some(3));
        assert_eq!(get_current_scan_item(&mut scan), Some(3));
        assert_eq!(get_next_scan_item(&mut scan), Some(12));
        assert_eq!(get_current_scan_item(&mut scan), Some(12));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);

        scan.up();
        assert_eq!(get_next_scan_item(&mut scan), Some(4));
        assert_eq!(get_current_scan_item(&mut scan), Some(4));

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(1));
        assert_eq!(get_current_scan_item(&mut scan), Some(1));
        assert_eq!(get_next_scan_item(&mut scan), Some(2));
        assert_eq!(get_current_scan_item(&mut scan), Some(2));
        assert_eq!(get_next_scan_item(&mut scan), Some(5));
        assert_eq!(get_current_scan_item(&mut scan), Some(5));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
    }

    #[test]
    fn test_skip_unmaterialized_tuples() {
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&trie, 4, 7);

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(1));
        assert_eq!(get_current_scan_item(&mut scan), Some(1));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(4));
        assert_eq!(get_current_scan_item(&mut scan), Some(4));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(0));
        assert_eq!(get_current_scan_item(&mut scan), Some(0));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(7));
        assert_eq!(get_current_scan_item(&mut scan), Some(7));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
        scan.up();
        assert_eq!(get_next_scan_item(&mut scan), Some(1));
        assert_eq!(get_current_scan_item(&mut scan), Some(1));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(7));
        assert_eq!(get_current_scan_item(&mut scan), Some(7));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
        scan.up();
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
        scan.up();
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
        scan.up();
        assert_eq!(get_current_scan_item(&mut scan), Some(1));
        assert_eq!(get_next_scan_item(&mut scan), Some(2));
        assert_eq!(get_current_scan_item(&mut scan), Some(2));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(4));
        assert_eq!(get_current_scan_item(&mut scan), Some(4));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(8));
        assert_eq!(get_current_scan_item(&mut scan), Some(8));
        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), Some(7));
        assert_eq!(get_current_scan_item(&mut scan), Some(7));
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
    }

    #[test]
    fn test_empty_input_trie() {
        let trie = create_example_trie();
        // Equality on lowest layer changed to 99 to prevent any matches and create trie scan without materialized tuples
        let mut scan = create_example_trie_scan(&trie, 4, 99);

        scan.down();
        assert_eq!(get_current_scan_item(&mut scan), None);
        assert_eq!(get_next_scan_item(&mut scan), None);
        assert_eq!(get_current_scan_item(&mut scan), None);
    }

    #[test]
    #[should_panic]
    fn test_advance_on_uninitialized_trie_scan_should_panic() {
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&trie, 4, 7);

        scan.advance_on_layer(0, false);
    }

    #[test]
    fn test_advance_above_target_layer() {
        let trie = create_example_trie();
        let mut scan = create_example_trie_scan(&trie, 4, 7);

        // Initialize trie
        scan.down();

        let lowest_layer_index = scan.get_types().len() - 1;

        assert_eq!(
            get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
            None
        );
        assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
        assert_eq!(
            get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
            Some(7)
        );
        assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(2));
        assert_eq!(
            get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
            Some(7)
        );
        assert_eq!(scan.advance_on_layer(lowest_layer_index, true), Some(0));
        assert_eq!(
            get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
            Some(7)
        );
        assert_eq!(scan.advance_on_layer(lowest_layer_index, true), None);
        assert_eq!(
            get_current_scan_item_at_layer(&mut scan, lowest_layer_index),
            None
        );
    }
}
