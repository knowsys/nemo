use super::{TableSchema, TrieScan, TrieScanEnum};
use crate::physical::columns::{
    ColumnScan, DifferenceScan, RangedColumnScanEnum, RangedColumnScanT,
};
use crate::physical::datatypes::DataTypeName;
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// Trie iterator representing a union between other trie iterators
#[derive(Debug)]
pub struct TrieDifference<'a> {
    trie_left: TrieScanEnum<'a>,
    trie_right: TrieScanEnum<'a>,
    layer_left: Option<usize>,
    layer_right: Option<usize>,
    difference_scans: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
}

impl<'a> TrieDifference<'a> {
    /// Construct new TrieDifference object.
    pub fn new(trie_left: TrieScanEnum<'a>, trie_right: TrieScanEnum<'a>) -> TrieDifference<'a> {
        let target_schema = trie_left.get_schema();
        let layer_count = target_schema.arity();

        let mut difference_scans =
            Vec::<UnsafeCell<RangedColumnScanT<'a>>>::with_capacity(layer_count);

        for layer_index in 0..layer_count {
            match target_schema.get_type(layer_index) {
                DataTypeName::U64 => {
                    let new_scan = UnsafeCell::new(RangedColumnScanT::U64(
                        RangedColumnScanEnum::DifferenceScan(DifferenceScan::new()),
                    ));

                    difference_scans.push(new_scan);
                }
                DataTypeName::Float => {}
                DataTypeName::Double => {}
            };
        }

        TrieDifference {
            trie_left,
            trie_right,
            layer_left: None,
            layer_right: None,
            difference_scans,
        }
    }
}

impl<'a> TrieScan<'a> for TrieDifference<'a> {
    fn up(&mut self) {
        // debug_assert!(self.current_layer.is_some());
        // let current_layer = self.current_layer.unwrap();
        // for scan_index in 0..self.layers.len() {
        //     if self.layers[scan_index] == current_layer {
        //         self.trie_scans[scan_index].up();
        //         self.layers[scan_index] -= 1;
        //     }
        // }
        // self.current_layer = if current_layer == 0 {
        //     None
        // } else {
        //     Some(current_layer - 1)
        // };
    }

    fn down(&mut self) {
        // let current_layer = self.current_layer.map_or(0, |v| v + 1);
        // let previous_layer = current_layer - 1;
        // self.current_layer = Some(current_layer);

        // debug_assert!(current_layer < self.trie_scans[0].get_schema().arity());

        // for scan_index in 0..self.layers.len() {
        //     if self.layers[scan_index] != previous_layer {
        //         continue;
        //     }

        //     //TODO: Don't use contains here
        //     if self.union_scans[current_layer]
        //         .get_mut()
        //         .get_smallest_scans()
        //         .contains(&scan_index)
        //     {
        //         self.trie_scans[scan_index].down();
        //         self.layers[scan_index] += 1;
        //     }
        // }

        // self.union_scans[current_layer].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&mut RangedColumnScanT<'a>> {
        self.get_scan(self.layer_left?)
    }

    fn get_scan(&self, index: usize) -> Option<&'a mut RangedColumnScanT<'a>> {
        unsafe { Some(&mut *self.difference_scans[index].get()) }
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie_left[0].get_schema()
    }
}
