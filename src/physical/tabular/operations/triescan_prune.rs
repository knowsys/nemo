use crate::physical::columnar::operations::ColumnScanPrune;
use crate::physical::columnar::traits::columnscan::ColumnScan;
use crate::physical::datatypes::StorageValueT;
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
/// The overhead after initialization should be negligible and the same a single pointer indirection.
pub type SharedTrieScanPruneState<'a> = Rc<UnsafeCell<TrieScanPruneState<'a>>>;

/// State which is shared with the individual output column scans and the trie scan
#[derive(Debug)]
pub struct TrieScanPruneState<'a> {
    /// Trie scan which is being pruned
    pub input_trie_scan: TrieScanEnum<'a>,
    initialized: bool,
    /// Current column scan layer of the input trie scan
    /// Layer zero is at to top of the trie scan
    input_trie_scan_current_layer: usize,
    /// This vector records for every column whether if has already
    /// been peeked into by the `advance` function.
    /// If so, the column should ignore the first call to `next` and
    /// instead return the result of `current`.
    /// This is required to implement the lookahead to check if
    /// a value would exist in a materialized version of the trie scan.
    column_peeks: Vec<bool>,
    /// Layer on which an outside consumer would believe this trie scan to be
    /// TODO: Maybe move to the `TrieScanPrune` itself
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

    /// Moves the input trie to the target layer through `up` and `down`
    fn go_to_layer(&mut self, target_layer: usize) {
        assert!(target_layer < self.input_trie_scan.get_types().len());

        // Move up or down to the correct layer
        // Note that the uppermost layer has index 0

        for _ in self.input_trie_scan_current_layer..target_layer {
            self.down();
        }
        for _ in target_layer..self.input_trie_scan_current_layer {
            self.up();
        }
    }

    /// Checks whether a column has been peeked already.
    ///
    /// See [`TrieScanPruneState`] for more information.
    #[inline]
    pub fn is_column_peeked(&self, index: usize) -> bool {
        self.column_peeks[index]
    }

    #[inline]
    unsafe fn get_input_trie_current_value(&mut self, index: usize) -> Option<StorageValueT> {
        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { &mut *scan.get() }.current()
    }

    #[inline]
    unsafe fn get_input_trie_next_value(&mut self, index: usize) -> Option<StorageValueT> {
        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { &mut *scan.get() }.next()
    }

    /// Gets the current output value for a column.
    ///
    /// This is None if the column has been peeked, otherwise the call is forwarded to the input column.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no mutable reference to the column scan at `index` and thus the [`UnsafeCell`] is safe to access.
    ///
    /// TODO: Update to allow for direct access
    #[inline]
    pub unsafe fn get_current_value(&self, index: usize) -> Option<StorageValueT> {
        debug_assert!(self.initialized);
        if self.column_peeks[index] {
            return None;
        }

        let scan = self.input_trie_scan.get_scan(index).unwrap();

        unsafe { &*scan.get() }.current()
    }

    /// Gets the next output value for a column.
    ///
    /// This is forwarded to `current()` of the underlying input column if the column has been peeked with the column peek being reset, otherwise to `next()`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there exists no immutable/mutable references to the column scan at `index` and thus the value inside the [`UnsafeCell`] is safe to mutate.
    ///
    /// TODO: Update to allow for direct access
    #[inline]
    pub unsafe fn get_next_value(&mut self, index: usize) -> Option<StorageValueT> {
        debug_assert!(self.initialized);
        if self.column_peeks[index] {
            self.column_peeks[index] = false;
            self.get_input_trie_current_value(index)
        } else {
            self.advance_at_layer(index, false);

            self.get_input_trie_current_value(index)
        }
    }

    /// TODO: Possibly update to allow for direct access
    fn advance_at_layer(
        &mut self,
        target_layer: usize,
        allow_advancements_above_target_layer: bool,
    ) -> bool {
        debug_assert!(self.initialized);

        self.go_to_layer(target_layer);

        let boundary_layer = if allow_advancements_above_target_layer {
            0
        } else {
            target_layer
        };

        // Traverse trie downwards to check if the value would actually exists in the materialized version of the trie
        // If at one layer there is no materialized value, move up, go to the next item, and move down again
        loop {
            // Go down one layer and check if there exists a item
            // If there exists no value, go upwards until either
            // a new value is found or we hit the the `boundary_layer`

            if self.input_trie_scan_current_layer > target_layer {
                self.column_peeks[self.input_trie_scan_current_layer] = true;
            }

            // SAFETY: this requires that no other references to the current layer scan exist. We currently have no way of ensuring this.
            let next_value =
                unsafe { self.get_input_trie_next_value(self.input_trie_scan_current_layer) };
            if next_value.is_none() {
                if self.input_trie_scan_current_layer == boundary_layer {
                    // Boundary layer has been reached and has no next value
                    return false;
                } else {
                    self.up();
                }
            } else if self.input_trie_scan_current_layer
                == self.input_trie_scan.get_types().len() - 1
            {
                // Lowest layer has been reached and an materialized value has been found
                return true;
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
            column_peeks: vec![false; target_types.len()],
            initialized: false,
            external_current_layer: 0,
        }));

        // Create one `ColumnScanPrune` for every input column
        for (i, target_type) in target_types.iter().enumerate() {
            // Generate code for every possible data type of the input column
            macro_rules! create_column_scan_for_datatype {
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
                StorageTypeName::U32 => create_column_scan_for_datatype!(U32, u32),
                StorageTypeName::U64 => create_column_scan_for_datatype!(U64, u64),
                StorageTypeName::Float => create_column_scan_for_datatype!(Float, Float),
                StorageTypeName::Double => create_column_scan_for_datatype!(Double, Double),
            };
        }

        Self {
            state,
            output_column_scans,
            target_types,
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
        let current_layer = unsafe { (*self.state.get()).external_current_layer };

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
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0, 1]);
        let column_snd = make_column_with_intervals_t(&[4, 5, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1, 8], &[0, 3, 4]);
        let column_fth = make_column_with_intervals_t(&[3, 7, 5, 7, 3, 4, 6, 7], &[0, 2, 4, 6, 7]);

        let trie = Trie::new(vec![column_fst, column_snd, column_trd, column_fth]);
        let scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut dict = Dict::default();
        let scan = TrieScanEnum::TrieScanSelectValue(TrieScanSelectValue::new(
            &mut dict,
            scan,
            &[
                ValueAssignment {
                    column_idx: 1,
                    value: DataValueT::U64(4),
                },
                ValueAssignment {
                    column_idx: 3,
                    value: DataValueT::U64(7),
                },
            ],
        ));
        let mut scan = TrieScanPrune::new(scan);

        assert_eq!(get_current_scan_item(&mut scan), None);
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
}
