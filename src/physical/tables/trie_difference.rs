use super::{TableSchema, TrieScan, TrieScanEnum};
use crate::physical::columns::{
    ColumnScan, DifferenceScan, MinusScan, RangedColumnScanCell, RangedColumnScanEnum,
    RangedColumnScanT,
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

        for layer_index in 0..target_schema.arity() {
            match target_schema.get_type(layer_index) {
                DataTypeName::U64 => unsafe {
                    if let RangedColumnScanT::U64(left_scan_enum) =
                        &*trie_left.get_scan(layer_index).unwrap().get()
                    {
                        if let RangedColumnScanT::U64(right_scan_enum) =
                            &*trie_right.get_scan(layer_index).unwrap().get()
                        {
                            let new_scan = if layer_index == target_schema.arity() - 1 {
                                RangedColumnScanEnum::MinusScan(MinusScan::new(
                                    left_scan_enum,
                                    right_scan_enum,
                                ))
                            } else {
                                RangedColumnScanEnum::DifferenceScan(DifferenceScan::new(
                                    left_scan_enum,
                                    right_scan_enum,
                                ))
                            };

                            difference_scans.push(UnsafeCell::new(RangedColumnScanT::U64(
                                RangedColumnScanCell::new(new_scan),
                            )));
                        } else {
                            panic!("type should match here")
                        }
                    } else {
                        panic!("type should match here")
                    }
                },
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
        debug_assert!(self.layer_left.is_some());

        self.layer_left = if self.layer_left.unwrap() == 0 {
            None
        } else {
            Some(self.layer_left.unwrap() - 1)
        };

        self.trie_left.up();

        if self.layer_right.is_some() && self.layer_right > self.layer_left {
            self.layer_right = self.layer_left;

            self.trie_right.up();
        }
    }

    fn down(&mut self) {
        let next_layer = self.layer_left.map_or(0, |v| v + 1);
        self.layer_left = Some(next_layer);

        debug_assert!(next_layer < self.trie_left.get_schema().arity());

        if self.layer_left == self.layer_right {
            if self.difference_scans[next_layer].get_mut().is_equal() {
                self.trie_right.down();
                self.layer_right = self.layer_left;
            }
        }

        self.difference_scans[next_layer].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        debug_assert!(self.layer_left.is_some());

        self.get_scan(self.layer_left?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.difference_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie_left.get_schema()
    }
}
