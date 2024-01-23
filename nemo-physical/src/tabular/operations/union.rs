//! This module defines [TrieScanUnion] and [GeneratorUnion].

use std::cell::UnsafeCell;

use bitvec::bitvec;

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::union::ColumnScanUnion,
    },
    datatypes::{Double, Float, StorageTypeName},
    dictionary::meta_dv_dict::MetaDvDictionary,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::OperationGenerator;

/// Used to create a [TrieScanUnion]
#[derive(Debug, Clone, Copy, Default)]
pub struct GeneratorUnion {}

impl GeneratorUnion {
    /// Create a new [GeneratorUnion].
    pub fn new() -> Self {
        Self {}
    }
}

impl OperationGenerator for GeneratorUnion {
    fn generate<'a>(
        &'_ self,
        trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        _dictionary: &'a MetaDvDictionary,
    ) -> Option<TrieScanEnum<'a>> {
        // We ignore any empy tables
        let trie_scans = trie_scans
            .into_iter()
            .filter_map(|scan_option| scan_option)
            .collect::<Vec<_>>();

        // We return `None` if there are no input tables left
        if trie_scans.is_empty() {
            return None;
        }

        debug_assert!(trie_scans
            .iter()
            .all(|scan| scan.arity() == trie_scans[0].arity()));

        let arity = trie_scans.first().map_or(0, |s| s.arity());
        let mut column_scans = Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(arity);

        for layer in 0..arity {
            macro_rules! union_scan {
                ($type:ty, $scan:ident) => {{
                    let mut input_scans =
                        Vec::<&'a ColumnScanCell<$type>>::with_capacity(trie_scans.len());

                    for trie_scan in &trie_scans {
                        let input_scan = &unsafe { &*trie_scan.scan(layer).get() }.$scan;
                        input_scans.push(input_scan);
                    }

                    ColumnScanEnum::ColumnScanUnion(ColumnScanUnion::new(input_scans))
                }};
            }

            let union_scan_id32 = union_scan!(u32, scan_id32);
            let union_scan_id64 = union_scan!(u64, scan_id64);
            let union_scan_i64 = union_scan!(i64, scan_i64);
            let union_scan_float = union_scan!(Float, scan_float);
            let union_scan_double = union_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                union_scan_id32,
                union_scan_id64,
                union_scan_i64,
                union_scan_float,
                union_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        Some(TrieScanEnum::TrieScanUnion(TrieScanUnion {
            trie_scans,
            column_scans,
            path_types: Vec::new(),
        }))
    }
}

/// [`PartialTrieScan`] which represents the union of a list of [`PartialTrieScan`]s
#[derive(Debug)]
pub struct TrieScanUnion<'a> {
    /// Input trie scans over of which the union is computed
    trie_scans: Vec<TrieScanEnum<'a>>,

    /// For each layer in the resulting trie contains a [`ColumnScanRainbow`]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
}

impl<'a> PartialTrieScan<'a> for TrieScanUnion<'a> {
    fn up(&mut self) {
        let current_layer = self.current_layer();

        for trie_scan in &mut self.trie_scans {
            if trie_scan.current_layer() == current_layer {
                trie_scan.up();
            }
        }

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types.last();

        let next_layer = self.path_types.len();

        debug_assert!(next_layer < self.arity());

        let previous_smallest = match previous_layer {
            Some(previous_layer) => self.column_scans[previous_layer]
                .get_mut()
                .union_get_smallest(
                    *previous_type.expect("path_types is not empty previous_layer is not None"),
                ),
            None => bitvec![1; self.trie_scans.len()],
        };

        for (scan_index, trie_scan) in self.trie_scans.iter_mut().enumerate() {
            if !previous_smallest[scan_index] {
                continue;
            }

            trie_scan.down(next_type);
        }

        self.column_scans[next_layer]
            .get_mut()
            .union_set_active(next_type, previous_smallest);
        self.column_scans[next_layer].get_mut().reset(next_type);

        self.path_types.push(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        &self.path_types
    }

    fn arity(&self) -> usize {
        self.trie_scans[0].arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }
}
