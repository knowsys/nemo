use super::super::{
    table_types::trie::TrieSchema,
    traits::{
        table_schema::TableSchema,
        triescan::{TrieScan, TrieScanEnum},
    },
};
use crate::physical::columnar::operations::ColumnScanWithTrieLookahead;
use crate::physical::columnar::traits::columnscan::{
    ColumnScanCell, ColumnScanEnum, ColumnScanRc, ColumnScanT,
};

use crate::physical::datatypes::{DataTypeName, Double, Float};
use std::{cell::RefCell, fmt::Debug, rc::Rc};

/// Iterator which turns an underlying 'partial' iterator into a full one,
/// meaining that it only returns values that would be present were the iterator to be materialized
#[derive(Debug)]
pub struct TrieFull<'a> {
    trie_scan: Rc<RefCell<TrieScanEnum<'a>>>,
    current_col_scan: Option<ColumnScanT<'a>>,
    current_layer: Option<usize>,
}

impl<'a> TrieFull<'a> {
    /// Construct new TrieFull object.
    pub fn new(trie_scan: TrieScanEnum<'a>) -> TrieFull<'a> {
        TrieFull {
            trie_scan: Rc::new(RefCell::new(trie_scan)),
            current_col_scan: None,
            current_layer: None,
        }
    }

    fn set_current_col_scan(&mut self) {
        if self.current_layer.is_none() {
            self.current_col_scan = None;
        } else {
            let current_layer = self.current_layer.unwrap();
            let current_type = self
                .trie_scan
                .borrow()
                .get_schema()
                .get_type(self.current_layer.unwrap());

            macro_rules! init_scans_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let col_scan = if let ColumnScanT::$variant(cs) =
                        self.trie_scan.borrow_mut().current_scan().unwrap()
                    {
                        cs.clone()
                    } else {
                        panic!("expected a column scan of type {}", stringify!($type));
                    };

                    let lookahead_scan = ColumnScanWithTrieLookahead::<$type>::new(
                        self.trie_scan.clone(),
                        current_layer,
                        col_scan,
                    );

                    self.current_col_scan = Some(ColumnScanT::$variant(ColumnScanRc::new(
                        ColumnScanCell::new(ColumnScanEnum::ColumnScanWithTrieLookahead(
                            lookahead_scan,
                        )),
                    )));
                }};
            }

            match current_type {
                DataTypeName::U64 => init_scans_for_datatype!(U64, u64),
                DataTypeName::Float => init_scans_for_datatype!(Float, Float),
                DataTypeName::Double => init_scans_for_datatype!(Double, Double),
            };
        }
    }
}

impl<'a> TrieScan<'a> for TrieFull<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());
        let up_layer = if self.current_layer.unwrap() == 0 {
            None
        } else {
            Some(self.current_layer.unwrap() - 1)
        };

        self.current_layer = up_layer;

        self.trie_scan.borrow_mut().up();

        self.set_current_col_scan();
    }

    fn down(&mut self) {
        let next_layer = self.current_layer.map_or(0, |v| v + 1);
        self.current_layer = Some(next_layer);

        debug_assert!(self.current_layer.unwrap() < self.get_schema().arity());

        self.trie_scan.borrow_mut().down();

        self.set_current_col_scan();
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        self.current_col_scan.as_mut()
    }

    fn get_scan(&mut self, _index: usize) -> Option<&mut ColumnScanT<'a>> {
        panic!("get_scan not possible for Full iterators.")
    }

    fn get_schema(&self) -> TrieSchema {
        self.trie_scan.borrow().get_schema()
    }
}

#[cfg(test)]
mod test {
    use super::TrieFull;
    use crate::physical::{
        columnar::traits::columnscan::ColumnScanT,
        datatypes::DataTypeName,
        tabular::{
            operations::TrieScanJoin,
            table_types::trie::{Trie, TrieScanGeneric, TrieSchema, TrieSchemaEntry},
            traits::triescan::{TrieScan, TrieScanEnum},
        },
        util::make_column_with_intervals_t,
    };
    use test_log::test;

    fn full_next(scan: &mut TrieFull) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn full_current(scan: &mut TrieFull) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_u64() {
        // Same setup as in test_trie_join
        let column_a_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_a_y = make_column_with_intervals_t(&[2, 3, 4, 5, 6, 7], &[0, 3, 4]);
        let column_b_y = make_column_with_intervals_t(&[1, 2, 3, 6], &[0]);
        let column_b_z = make_column_with_intervals_t(&[1, 8, 9, 10, 11, 12], &[0, 1, 3, 4]);

        let schema_a = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);
        let schema_b = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_target = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_a = Trie::new(schema_a, vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(schema_b, vec![column_b_y, column_b_z]);

        let join_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
        ));

        let mut full_iter = TrieFull::new(join_iter);

        full_iter.down();
        assert_eq!(full_current(&mut full_iter), None);
        assert_eq!(full_next(&mut full_iter), Some(1));
        assert_eq!(full_current(&mut full_iter), Some(1));

        full_iter.down();
        assert_eq!(full_current(&mut full_iter), None);
        assert_eq!(full_next(&mut full_iter), Some(2));
        assert_eq!(full_current(&mut full_iter), Some(2));

        full_iter.down();
        assert_eq!(full_current(&mut full_iter), None);
        assert_eq!(full_next(&mut full_iter), Some(8));
        assert_eq!(full_current(&mut full_iter), Some(8));
        assert_eq!(full_next(&mut full_iter), Some(9));
        assert_eq!(full_current(&mut full_iter), Some(9));
        assert_eq!(full_next(&mut full_iter), None);
        assert_eq!(full_current(&mut full_iter), None);

        full_iter.up();
        assert_eq!(full_next(&mut full_iter), Some(3));
        assert_eq!(full_current(&mut full_iter), Some(3));
        assert_eq!(full_next(&mut full_iter), None);
        assert_eq!(full_current(&mut full_iter), None);

        // full_iter.up();
        // assert_eq!(full_next(&mut full_iter), Some(2));
        // assert_eq!(full_current(&mut full_iter), Some(2));

        // full_iter.down();
        // assert_eq!(full_current(&mut full_iter), None);
        // assert_eq!(full_next(&mut full_iter), None);
        // assert_eq!(full_current(&mut full_iter), None);

        full_iter.up();
        assert_eq!(full_next(&mut full_iter), Some(3));
        assert_eq!(full_current(&mut full_iter), Some(3));

        full_iter.down();
        assert_eq!(full_current(&mut full_iter), None);
        assert_eq!(full_next(&mut full_iter), Some(6));
        assert_eq!(full_current(&mut full_iter), Some(6));

        full_iter.down();
        assert_eq!(full_current(&mut full_iter), None);
        assert_eq!(full_next(&mut full_iter), Some(11));
        assert_eq!(full_current(&mut full_iter), Some(11));
        assert_eq!(full_next(&mut full_iter), Some(12));
        assert_eq!(full_current(&mut full_iter), Some(12));
        assert_eq!(full_next(&mut full_iter), None);
        assert_eq!(full_current(&mut full_iter), None);

        full_iter.up();
        assert_eq!(full_next(&mut full_iter), None);
        assert_eq!(full_current(&mut full_iter), None);

        full_iter.up();
        assert_eq!(full_next(&mut full_iter), None);
        assert_eq!(full_current(&mut full_iter), None);
    }
}
