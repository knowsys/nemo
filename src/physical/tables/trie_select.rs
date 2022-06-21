use super::{TableSchema, TrieScan, TrieScanEnum};
use crate::physical::columns::{
    EqualColumnScan, PassScan, RangedColumnScanCell, RangedColumnScanEnum, RangedColumnScanT,
};
use crate::physical::datatypes::DataTypeName;
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// Trie iterator representing a union between other trie iterators
#[derive(Debug)]
pub struct TrieSelectEqual<'a> {
    base_trie: TrieScanEnum<'a>,
    select_scans: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> TrieSelectEqual<'a> {
    /// Construct new TrieSelectEqual object.
    /// Assumes that each vector in eq_classes is sorted.
    /// Also the outer vector should be sorted my the minimum values of the inner vectors.
    pub fn new(base_trie: TrieScanEnum<'a>, eq_classes: Vec<Vec<usize>>) -> Self {
        let target_schema = base_trie.get_schema();
        let arity = target_schema.arity();
        let mut select_scans = Vec::<UnsafeCell<RangedColumnScanT<'a>>>::with_capacity(arity);

        for col_index in 0..arity {
            // TODO: Other types
            match target_schema.get_type(col_index) {
                DataTypeName::U64 => {
                    unsafe {
                        // base_trie.get_scan(col_index).unwrap().get_mut()

                        let base_scan_cell = if let RangedColumnScanT::U64(base_scan) =
                            &*base_trie.get_scan(col_index).unwrap().get()
                        {
                            base_scan
                        } else {
                            panic!("");
                        };
                        let next_scan = RangedColumnScanCell::new(RangedColumnScanEnum::PassScan(
                            PassScan::new(base_scan_cell),
                        ));

                        select_scans.push(UnsafeCell::new(RangedColumnScanT::U64(next_scan)));
                    }
                }
                DataTypeName::Float => {}
                DataTypeName::Double => {}
            }
        }

        for class in eq_classes {
            for member_index in 1..class.len() {
                let prev_member_idx = class[member_index - 1];
                let current_member_idx = class[member_index];

                // TODO: Other types
                match target_schema.get_type(current_member_idx) {
                    DataTypeName::U64 => unsafe {
                        let reference_scan_enum = if let RangedColumnScanT::U64(ref_scan) =
                            &*base_trie.get_scan(prev_member_idx).unwrap().get()
                        {
                            ref_scan
                        } else {
                            panic!("");
                        };
                        let value_scan_enum = if let RangedColumnScanT::U64(value_scan) =
                            &*base_trie.get_scan(current_member_idx).unwrap().get()
                        {
                            value_scan
                        } else {
                            panic!("");
                        };

                        let next_scan =
                            RangedColumnScanCell::new(RangedColumnScanEnum::EqualColumnScan(
                                EqualColumnScan::new(reference_scan_enum, value_scan_enum),
                            ));

                        select_scans[current_member_idx] =
                            UnsafeCell::new(RangedColumnScanT::U64(next_scan));
                    },
                    DataTypeName::Float => {}
                    DataTypeName::Double => {}
                }
            }
        }

        Self {
            base_trie,
            select_scans,
            current_layer: None,
        }
    }
}

impl<'a> TrieScan<'a> for TrieSelectEqual<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());

        self.current_layer = self
            .current_layer
            .and_then(|index| (index > 0).then(|| index - 1));

        self.base_trie.up();
    }

    fn down(&mut self) {
        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));
        debug_assert!(self.current_layer.unwrap() < self.get_schema().arity());

        self.base_trie.down();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.select_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.base_trie.get_schema()
    }
}

#[cfg(test)]
mod test {
    use super::TrieSelectEqual;
    use crate::physical::columns::RangedColumnScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tables::{
        IntervalTrieScan, Trie, TrieScan, TrieScanEnum, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn select_eq_next(scan: &mut TrieSelectEqual) -> Option<u64> {
        if let RangedColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn select_eq_current(scan: &mut TrieSelectEqual) -> Option<u64> {
        if let RangedColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_select_equal() {
        let column_fst = make_gict(&[1], &[0]);
        let column_snd = make_gict(&[4, 5], &[0]);
        let column_trd = make_gict(&[0, 1, 2, 1], &[0, 3]);
        let column_fth = make_gict(&[0, 4, 5, 3, 4, 6], &[0, 1, 3, 5]);
        let schema = TrieSchema::new(vec![
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
            TrieSchemaEntry {
                label: 3,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_fst, column_snd, column_trd, column_fth]);
        let trie_iter = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie));

        let mut select_iter = TrieSelectEqual::new(trie_iter, vec![vec![0, 2], vec![1, 3]]);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(4));
        assert_eq!(select_eq_current(&mut select_iter), Some(4));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(4));
        assert_eq!(select_eq_current(&mut select_iter), Some(4));
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), Some(5));
        assert_eq!(select_eq_current(&mut select_iter), Some(5));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
    }
}
