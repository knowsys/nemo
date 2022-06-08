use super::{TableSchema, TrieScan, TrieScanEnum};
use crate::physical::columns::{
    ColumnScan, RangedColumnScanCell, RangedColumnScanEnum, RangedColumnScanT, UnionScan,
};
use crate::physical::datatypes::DataTypeName;
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// Trie iterator representing a union between other trie iterators
#[derive(Debug)]
pub struct TrieUnion<'a> {
    trie_scans: Vec<TrieScanEnum<'a>>,
    layers: Vec<usize>,
    union_scans: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> TrieUnion<'a> {
    /// Construct new TrieUnion object.
    pub fn new(trie_scans: Vec<TrieScanEnum<'a>>) -> TrieUnion<'a> {
        debug_assert!(trie_scans.len() > 0);
        let layers = vec![0usize; trie_scans.len()];

        // This assumes that very schema is the same
        // TODO: Perhaps debug_assert! this
        let target_schema = trie_scans[0].get_schema();
        let mut union_scans =
            Vec::<UnsafeCell<RangedColumnScanT<'a>>>::with_capacity(target_schema.arity());

        for layer_index in 0..target_schema.arity() {
            match target_schema.get_type(layer_index) {
                DataTypeName::U64 => {
                    let column_scans =
                        Vec::<&'a RangedColumnScanCell<u64>>::with_capacity(target_schema.arity());

                    for scan in trie_scans {
                        unsafe {
                            if let RangedColumnScanT::U64(scan_enum) =
                                &*scan.get_scan(layer_index).unwrap().get()
                            {
                                column_scans.push(scan_enum);
                            } else {
                                panic!("type should match here")
                            }
                        }
                    }

                    let new_union_scan =
                        RangedColumnScanEnum::UnionScan(UnionScan::new(column_scans));

                    union_scans.push(UnsafeCell::new(RangedColumnScanT::U64(
                        RangedColumnScanCell::new(new_union_scan),
                    )));
                }
                DataTypeName::Float => {}
                DataTypeName::Double => {}
            };
        }

        TrieUnion {
            trie_scans,
            layers,
            union_scans,
            current_layer: None,
        }
    }
}

impl<'a> TrieScan<'a> for TrieUnion<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());
        let current_layer = self.current_layer.unwrap();
        for scan_index in 0..self.layers.len() {
            if self.layers[scan_index] == current_layer {
                self.trie_scans[scan_index].up();
                self.layers[scan_index] -= 1;
            }
        }
        self.current_layer = if current_layer == 0 {
            None
        } else {
            Some(current_layer - 1)
        };
    }

    fn down(&mut self) {
        let current_layer = self.current_layer.map_or(0, |v| v + 1);
        let previous_layer = current_layer - 1;
        self.current_layer = Some(current_layer);

        debug_assert!(current_layer < self.trie_scans[0].get_schema().arity());

        for scan_index in 0..self.layers.len() {
            if self.layers[scan_index] != previous_layer {
                continue;
            }

            //TODO: Don't use contains here
            if self.union_scans[current_layer]
                .get_mut()
                .get_smallest_scans()
                .contains(&scan_index)
            {
                self.trie_scans[scan_index].down();
                self.layers[scan_index] += 1;
            }
        }

        self.union_scans[current_layer].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.union_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie_scans[0].get_schema()
    }
}
