use crate::physical::{
    columnar::{
        operations::{ColumnScanFollow, ColumnScanMinus, ColumnScanPass},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::DataTypeName,
    tabular::traits::{
        table_schema::TableSchema,
        triescan::{TrieScan, TrieScanEnum},
    },
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// Trie iterator representing the difference between other trie iterators
#[derive(Debug)]
pub struct TrieScanMinus<'a> {
    trie_left: Box<TrieScanEnum<'a>>,
    trie_right: Box<TrieScanEnum<'a>>,
    layer_left: Option<usize>,
    layer_right: Option<usize>,
    difference_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanMinus<'a> {
    /// Construct new TrieScanMinus object.
    pub fn new(trie_left: TrieScanEnum<'a>, trie_right: TrieScanEnum<'a>) -> TrieScanMinus<'a> {
        let target_schema = trie_left.get_schema();
        let layer_count = target_schema.arity();

        let mut difference_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(layer_count);

        for layer_index in 0..target_schema.arity() {
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        if let ColumnScanT::$variant(left_scan_enum) =
                            &*trie_left.get_scan(layer_index).unwrap().get()
                        {
                            if let ColumnScanT::$variant(right_scan_enum) =
                                &*trie_right.get_scan(layer_index).unwrap().get()
                            {
                                let new_scan = if layer_index == target_schema.arity() - 1 {
                                    ColumnScanEnum::ColumnScanMinus(ColumnScanMinus::new(
                                        left_scan_enum,
                                        right_scan_enum,
                                    ))
                                } else {
                                    ColumnScanEnum::ColumnScanFollow(ColumnScanFollow::new(
                                        left_scan_enum,
                                        right_scan_enum,
                                    ))
                                };

                                difference_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                                    ColumnScanCell::new(new_scan),
                                )));
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($variant));
                            }
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    }
                };
            }

            match target_schema.get_type(layer_index) {
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
            };
        }

        TrieScanMinus {
            trie_left: Box::new(trie_left),
            trie_right: Box::new(trie_right),
            layer_left: None,
            layer_right: None,
            difference_scans,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanMinus<'a> {
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
        debug_assert!(next_layer < self.get_schema().arity());

        if next_layer > 0 {
            let current_layer = next_layer - 1;
            if self.layer_left == self.layer_right
                && self.difference_scans[current_layer].get_mut().is_equal()
            {
                self.trie_right.down();
                self.layer_right = Some(next_layer);
            }
        } else {
            self.layer_right = Some(next_layer);
            self.trie_right.down();
        }

        self.trie_left.down();
        self.layer_left = Some(next_layer);

        if next_layer == self.get_schema().arity() - 1 {
            macro_rules! down_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        if let ColumnScanT::$variant(left_scan_enum) =
                            &*self.trie_left.get_scan(next_layer).unwrap().get()
                        {
                            if let ColumnScanT::$variant(right_scan_enum) =
                                &*self.trie_right.get_scan(next_layer).unwrap().get()
                            {
                                if self.layer_left == self.layer_right {
                                    self.difference_scans[next_layer] = UnsafeCell::new(
                                        ColumnScanT::$variant(ColumnScanCell::new(
                                            ColumnScanEnum::ColumnScanMinus(ColumnScanMinus::new(
                                                left_scan_enum,
                                                right_scan_enum,
                                            )),
                                        )),
                                    );
                                } else {
                                    self.difference_scans[next_layer] =
                                        UnsafeCell::new(ColumnScanT::$variant(
                                            ColumnScanCell::new(ColumnScanEnum::ColumnScanPass(
                                                ColumnScanPass::new(left_scan_enum),
                                            )),
                                        ));
                                }
                            }
                        }
                    }
                };
            }

            match self.get_schema().get_type(next_layer) {
                DataTypeName::U64 => down_for_datatype!(U64),
                DataTypeName::Float => down_for_datatype!(Float),
                DataTypeName::Double => down_for_datatype!(Double),
            }
        } else {
            self.difference_scans[next_layer].get_mut().reset();
        }
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        self.get_scan(self.layer_left?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.difference_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie_left.get_schema()
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanMinus;
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tabular::table_types::trie::{
        Trie, TrieScanGeneric, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn diff_next(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &*diff_scan.current_scan()?.get() } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn diff_current(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &*diff_scan.current_scan()?.get() } {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_trie_difference() {
        let column_left_x = make_gict(&[1, 2, 3], &[0]);
        let column_left_y = make_gict(&[3, 6, 8, 2, 7, 5], &[0, 3, 5]);
        let column_right_x = make_gict(&[1, 3, 4], &[0]);
        let column_right_y = make_gict(&[2, 6, 9, 2, 5, 8], &[0, 3, 5]);

        let schema_left = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_right = schema_left.clone();

        let trie_left = Trie::new(schema_left, vec![column_left_x, column_left_y]);
        let trie_right = Trie::new(schema_right, vec![column_right_x, column_right_y]);

        let trie_left_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_left));
        let trie_right_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_right));

        let mut diff_iter = TrieScanMinus::new(trie_left_iter, trie_right_iter);
        assert!(diff_iter.current_scan().is_none());

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(3));
        assert_eq!(diff_current(&mut diff_iter), Some(3));
        assert_eq!(diff_next(&mut diff_iter), Some(8));
        assert_eq!(diff_current(&mut diff_iter), Some(8));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), Some(7));
        assert_eq!(diff_current(&mut diff_iter), Some(7));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(3));
        assert_eq!(diff_current(&mut diff_iter), Some(3));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);
    }

    #[test]
    fn test_trie_difference_2() {
        let column_left_x = make_gict(&[4, 7, 8, 9, 10], &[0]);
        let column_left_y = make_gict(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);

        let column_right_x = make_gict(&[2, 3, 4, 5, 7, 8, 9, 10], &[0]);
        let column_right_y = make_gict(
            &[1, 1, 2, 1, 2, 7, 5, 7, 1, 2, 7],
            &[0, 1, 2, 3, 4, 5, 6, 8],
        );

        let schema_left = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_right = schema_left.clone();

        let trie_left = Trie::new(schema_left, vec![column_left_x, column_left_y]);
        let trie_right = Trie::new(schema_right, vec![column_right_x, column_right_y]);

        let trie_left_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_left));
        let trie_right_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_right));

        let mut diff_iter = TrieScanMinus::new(trie_left_iter, trie_right_iter);
        assert!(diff_iter.current_scan().is_none());

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(4));
        assert_eq!(diff_current(&mut diff_iter), Some(4));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(7));
        assert_eq!(diff_current(&mut diff_iter), Some(7));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(8));
        assert_eq!(diff_current(&mut diff_iter), Some(8));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(9));
        assert_eq!(diff_current(&mut diff_iter), Some(9));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(10));
        assert_eq!(diff_current(&mut diff_iter), Some(10));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);
    }
}
