use crate::physical::{
    columnar::{
        operations::{ColumnScanFollow, ColumnScanMinus, ColumnScanPass},
        traits::columnscan::{
            ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanRc, ColumnScanT,
        },
    },
    datatypes::DataTypeName,
    tabular::{
        table_types::trie::TrieSchema,
        traits::{
            table_schema::TableSchema,
            triescan::{TrieScan, TrieScanEnum},
        },
    },
};
use std::fmt::Debug;

/// Trie iterator representing the difference between other trie iterators
#[derive(Debug)]
pub struct TrieScanMinus<'a> {
    trie_left: Box<TrieScanEnum<'a>>,
    trie_right: Box<TrieScanEnum<'a>>,
    layer_left: Option<usize>,
    layer_right: Option<usize>,
    difference_scans: Vec<ColumnScanT<'a>>,
}

impl<'a> TrieScanMinus<'a> {
    /// Construct new TrieScanMinus object.
    pub fn new(
        mut trie_left: TrieScanEnum<'a>,
        mut trie_right: TrieScanEnum<'a>,
    ) -> TrieScanMinus<'a> {
        let layer_count = trie_left.get_schema().arity();

        let mut difference_scans = Vec::<ColumnScanT<'a>>::with_capacity(layer_count);

        for layer_index in 0..layer_count {
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    if let ColumnScanT::$variant(left_scan_rc) =
                        trie_left.get_scan(layer_index).unwrap()
                    {
                        if let ColumnScanT::$variant(right_scan_rc) =
                            trie_right.get_scan(layer_index).unwrap()
                        {
                            let new_scan = if layer_index == layer_count - 1 {
                                ColumnScanEnum::ColumnScanMinus(ColumnScanMinus::new(
                                    left_scan_rc.clone(),
                                    right_scan_rc.clone(),
                                ))
                            } else {
                                ColumnScanEnum::ColumnScanFollow(ColumnScanFollow::new(
                                    left_scan_rc.clone(),
                                    right_scan_rc.clone(),
                                ))
                            };

                            difference_scans.push(ColumnScanT::$variant(ColumnScanRc::new(
                                ColumnScanCell::new(new_scan),
                            )));
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($variant));
                    }
                };
            }

            match trie_left.get_schema().get_type(layer_index) {
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
                && self.difference_scans[current_layer].is_equal()
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
                    if let ColumnScanT::$variant(left_scan_rc) =
                        self.trie_left.get_scan(next_layer).unwrap()
                    {
                        if let ColumnScanT::$variant(right_scan_rc) =
                            self.trie_right.get_scan(next_layer).unwrap()
                        {
                            if self.layer_left == self.layer_right {
                                self.difference_scans[next_layer] =
                                    ColumnScanT::$variant(ColumnScanRc::new(ColumnScanCell::new(
                                        ColumnScanEnum::ColumnScanMinus(ColumnScanMinus::new(
                                            left_scan_rc.clone(),
                                            right_scan_rc.clone(),
                                        )),
                                    )));
                            } else {
                                self.difference_scans[next_layer] =
                                    ColumnScanT::$variant(ColumnScanRc::new(ColumnScanCell::new(
                                        ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(
                                            left_scan_rc.clone(),
                                        )),
                                    )));
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
            self.difference_scans[next_layer].reset();
        }
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        self.get_scan(self.layer_left?)
    }

    fn get_scan(&mut self, index: usize) -> Option<&mut ColumnScanT<'a>> {
        Some(&mut self.difference_scans[index])
    }

    fn get_schema(&self) -> TrieSchema {
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
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn diff_next(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = diff_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn diff_current(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = diff_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_trie_difference() {
        let column_left_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_left_y = make_column_with_intervals_t(&[3, 6, 8, 2, 7, 5], &[0, 3, 5]);
        let column_right_x = make_column_with_intervals_t(&[1, 3, 4], &[0]);
        let column_right_y = make_column_with_intervals_t(&[2, 6, 9, 2, 5, 8], &[0, 3, 5]);

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
        let column_left_x = make_column_with_intervals_t(&[4, 7, 8, 9, 10], &[0]);
        let column_left_y = make_column_with_intervals_t(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);

        let column_right_x = make_column_with_intervals_t(&[2, 3, 4, 5, 7, 8, 9, 10], &[0]);
        let column_right_y = make_column_with_intervals_t(
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
