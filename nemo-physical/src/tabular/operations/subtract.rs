//! This module defines [TrieScanSubtract] and [GeneratorSubtract].

use std::cell::UnsafeCell;

use bitvec::bitvec;

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::ColumnScanSubtract,
    },
    datatypes::{Double, Float, StorageTypeName},
    dictionary::meta_dv_dict::MetaDvDictionary,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationGenerator, OperationTable};

/// Used to create a [TrieScanSubtract]
#[derive(Debug, Clone)]
pub struct GeneratorSubtract {
    /// For each [`PartialTrieScan`] in `trie_subtract`,
    /// specifies which of its layers correspond to which layers from the "main" trie
    layer_maps: Vec<SubtractedLayerMap>,
}

impl GeneratorSubtract {
    /// Create a new [GeneratorSubtract].
    ///
    /// For every input table, every marker must appear in the output table
    /// and also in the same order
    ///
    /// # Panics
    /// Panics if the above condition is not met.
    pub fn new(output: OperationTable, inputs: Vec<OperationTable>) -> Self {
        let mut layer_maps = Vec::<SubtractedLayerMap>::new();

        for input in inputs {
            let layer_map: SubtractedLayerMap = input.into_iter().map(|input_marker| output.position(&input_marker).expect("Every column of the subtracted tables should be assigned to a column in the main trie.")).collect();
            debug_assert!(layer_map.is_sorted());

            layer_maps.push(layer_map);
        }

        Self { layer_maps }
    }
}

impl OperationGenerator for GeneratorSubtract {
    fn generate<'a>(
        &'_ self,
        mut trie_scans: Vec<TrieScanEnum<'a>>,
        _dictionary: &'a MetaDvDictionary,
    ) -> TrieScanEnum<'a> {
        debug_assert!(
            trie_scans.len() > 1,
            "Input needs to include the main trie and at least one trie that is to be subtracted"
        );

        let tries_subtract = trie_scans.split_off(1);
        let trie_main = trie_scans.remove(0);

        let mut column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>> =
            Vec::with_capacity(trie_main.arity());

        for output_layer in 0..trie_main.arity() {
            macro_rules! subtract_scan {
                ($type:ty, $scan:ident) => {{
                    let mut subtract_indices = Vec::<usize>::new();
                    let mut follow_indices = Vec::<usize>::new();

                    let input_main = &unsafe { &*trie_main.scan(output_layer).get() }.$scan;
                    let mut input_follower =
                        Vec::<Option<&'a ColumnScanCell<$type>>>::with_capacity(trie_scans.len());

                    for (subtract_index, (trie_subtract, layer_map)) in
                        (tries_subtract.iter().zip(self.layer_maps.iter())).enumerate()
                    {
                        let used_layer = layer_map.iter().position(|&layer| layer == output_layer);

                        if let Some(used_layer) = used_layer {
                            let is_last = used_layer == layer_map.len();

                            if is_last {
                                subtract_indices.push(subtract_index);
                            } else {
                                follow_indices.push(subtract_index);
                            }

                            let input_subtract =
                                &unsafe { &*trie_subtract.scan(used_layer).get() }.$scan;
                            input_follower.push(Some(input_subtract));
                        } else {
                            input_follower.push(None);
                        }
                    }

                    ColumnScanEnum::ColumnScanSubtract(ColumnScanSubtract::new(
                        input_main,
                        input_follower,
                        subtract_indices,
                        follow_indices,
                    ))
                }};
            }

            let subtract_scan_id32 = subtract_scan!(u32, scan_id32);
            let subtract_scan_id64 = subtract_scan!(u64, scan_id64);
            let subtract_scan_i64 = subtract_scan!(i64, scan_i64);
            let subtract_scan_float = subtract_scan!(Float, scan_float);
            let subtract_scan_double = subtract_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                subtract_scan_id32,
                subtract_scan_id64,
                subtract_scan_i64,
                subtract_scan_float,
                subtract_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        TrieScanEnum::TrieScanSubtract(TrieScanSubtract {
            trie_main: Box::new(trie_main),
            tries_subtract,
            layer_maps: self.layer_maps.clone(),
            column_scans,
            path_types: Vec::new(),
        })
    }

    fn is_unary_identity(&self) -> bool {
        true
    }
}

/// [`PartialTrieScan`] that subtracts from a "main" [`PartialTrieScan`] a list of [`PartialTrieScan`]s referred to as "subtract".
/// The results contains all elements that are in main but not in one of the subtract scans.
/// This can also handle subtracting tables of different arities.
#[derive(Debug)]
pub struct TrieScanSubtract<'a> {
    /// [`PartialTrieScan`] from which elements are being subtracted
    trie_main: Box<TrieScanEnum<'a>>,
    /// Elements that are subtracted
    tries_subtract: Vec<TrieScanEnum<'a>>,

    /// For each [`PartialTrieScan`] in `trie_subtract`,
    /// specifies which of its layers correspond to which layers from the "main" trie
    layer_maps: Vec<SubtractedLayerMap>,

    /// List of `ColumnScanSubtract` where every entry represents one level of the resulting trie.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
}

/// Associates each layer in a "subtract" trie with a layer in the "main" trie
type SubtractedLayerMap = Vec<usize>;

impl<'a> PartialTrieScan<'a> for TrieScanSubtract<'a> {
    fn up(&mut self) {
        let current_layer = self.current_layer().unwrap();

        for (trie_subtract, layer_map) in self.tries_subtract.iter_mut().zip(self.layer_maps.iter())
        {
            if let Some(current_layer_subtract) = trie_subtract.current_layer() {
                let used_layer = layer_map[current_layer_subtract];

                if current_layer == used_layer {
                    trie_subtract.up();
                }
            }
        }

        self.trie_main.up();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types.last();

        let next_layer = self.path_types.len();

        debug_assert!(next_layer < self.arity());

        let previous_equal = match previous_layer {
            Some(previous_layer) => self.column_scans[previous_layer]
                .get_mut()
                .subtract_get_equal(
                    *previous_type.expect("path_types is not empty previous_layer is not None"),
                ),
            None => bitvec![1; self.arity()],
        };

        for (subtract_index, (trie_subtract, layer_map)) in
            (self.tries_subtract.iter_mut().zip(self.layer_maps.iter())).enumerate()
        {
            if !previous_equal[subtract_index] {
                continue;
            }

            let next_layer_subtract = trie_subtract.current_layer().map_or(0, |layer| layer + 1);

            if layer_map[next_layer_subtract] == next_layer {
                trie_subtract.down(next_type);
            }
        }

        self.column_scans[next_layer]
            .get_mut()
            .subtract_set_active(next_type, previous_equal);
        self.column_scans[next_layer].get_mut().reset(next_type);

        self.path_types.push(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        self.trie_main.path_types()
    }

    fn arity(&self) -> usize {
        self.trie_main.arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }
}
