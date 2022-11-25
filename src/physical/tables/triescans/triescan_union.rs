use crate::physical::{
    columns::colscans::{ColScan, ColScanCell, ColScanEnum, ColScanT, ColScanUnion},
    datatypes::{DataTypeName, Double, Float},
    tables::tables::TableSchema,
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

use super::{TrieScan, TrieScanEnum};

/// Trie iterator representing a union between other trie iterators
#[derive(Debug)]
pub struct TrieUnion<'a> {
    trie_scans: Vec<TrieScanEnum<'a>>,
    layers: Vec<Option<usize>>,
    union_scans: Vec<UnsafeCell<ColScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> TrieUnion<'a> {
    /// Construct new TrieUnion object.
    pub fn new(trie_scans: Vec<TrieScanEnum<'a>>) -> TrieUnion<'a> {
        debug_assert!(!trie_scans.is_empty());

        // This assumes that every schema is the same
        // TODO: Perhaps debug_assert! this
        let target_schema = trie_scans[0].get_schema();
        let mut union_scans = Vec::<UnsafeCell<ColScanT<'a>>>::with_capacity(target_schema.arity());

        for layer_index in 0..target_schema.arity() {
            macro_rules! init_scans_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let mut column_scans =
                        Vec::<&'a ColScanCell<$type>>::with_capacity(target_schema.arity());

                    for scan in &trie_scans {
                        unsafe {
                            if let ColScanT::$variant(scan_enum) =
                                &*scan.get_scan(layer_index).unwrap().get()
                            {
                                column_scans.push(scan_enum);
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($type));
                            }
                        }
                    }

                    let new_union_scan = ColScanEnum::ColScanUnion(ColScanUnion::new(column_scans));

                    union_scans.push(UnsafeCell::new(ColScanT::$variant(ColScanCell::new(
                        new_union_scan,
                    ))));
                }};
            }

            match target_schema.get_type(layer_index) {
                DataTypeName::U64 => init_scans_for_datatype!(U64, u64),
                DataTypeName::Float => init_scans_for_datatype!(Float, Float),
                DataTypeName::Double => init_scans_for_datatype!(Double, Double),
            };
        }

        let layers = vec![None; trie_scans.len()];

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
        let up_layer = if self.current_layer.unwrap() == 0 {
            None
        } else {
            Some(self.current_layer.unwrap() - 1)
        };

        for scan_index in 0..self.layers.len() {
            if self.layers[scan_index] == self.current_layer {
                self.trie_scans[scan_index].up();
                self.layers[scan_index] = up_layer;
            }
        }
        self.current_layer = up_layer;
    }

    fn down(&mut self) {
        let previous_layer = self.current_layer;
        let next_layer = self.current_layer.map_or(0, |v| v + 1);
        self.current_layer = Some(next_layer);

        debug_assert!(self.current_layer.unwrap() < self.trie_scans[0].get_schema().arity());

        let mut active_scans = Vec::<usize>::new();

        for scan_index in 0..self.layers.len() {
            if self.layers[scan_index] != previous_layer {
                continue;
            }

            if previous_layer.is_none()
                || self.union_scans[previous_layer.unwrap()]
                    .get_mut()
                    .get_smallest_scans()[scan_index]
            {
                active_scans.push(scan_index);

                self.trie_scans[scan_index].down();
                self.layers[scan_index] = self.current_layer;
            }
        }

        self.union_scans[next_layer]
            .get_mut()
            .set_active_scans(active_scans);
        self.union_scans[next_layer].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColScanT<'a>>> {
        Some(&self.union_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie_scans[0].get_schema()
    }
}

#[cfg(test)]
mod test {
    use super::TrieUnion;
    use crate::physical::columns::colscans::ColScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tables::tries::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::tables::triescans::{IntervalTrieScan, TrieJoin, TrieScan, TrieScanEnum};
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn union_next(union_scan: &mut TrieUnion) -> Option<u64> {
        if let ColScanT::U64(rcs) = unsafe { &*union_scan.current_scan()?.get() } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn union_current(union_scan: &mut TrieUnion) -> Option<u64> {
        if let ColScanT::U64(rcs) = unsafe { &*union_scan.current_scan()?.get() } {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_union() {
        let column_fst_x = make_gict(&[1, 2, 4], &[0]);
        let column_fst_y = make_gict(&[2, 5, 4], &[0, 1, 2]);
        let column_snd_x = make_gict(&[1, 2, 7], &[0]);
        let column_snd_y = make_gict(&[2, 4, 4, 8, 9], &[0, 2, 3]);
        let column_trd_x = make_gict(&[1, 3, 7], &[0]);
        let column_trd_y = make_gict(&[1, 4, 6, 3, 7, 8], &[0, 3, 4]);

        let schema_fst = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_snd = schema_fst.clone();
        let schema_trd = schema_fst.clone();

        let trie_fst = Trie::new(schema_fst, vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(schema_snd, vec![column_snd_x, column_snd_y]);
        let trie_trd = Trie::new(schema_trd, vec![column_trd_x, column_trd_y]);

        let trie_iter_fst = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_snd));
        let trie_iter_trd = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_trd));
        let mut union_iter = TrieUnion::new(vec![trie_iter_fst, trie_iter_snd, trie_iter_trd]);
        assert_eq!(union_current(&mut union_iter), None);
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), Some(6));
        assert_eq!(union_current(&mut union_iter), Some(6));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), Some(5));
        assert_eq!(union_current(&mut union_iter), Some(5));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(3));
        assert_eq!(union_current(&mut union_iter), Some(3));
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(3));
        assert_eq!(union_current(&mut union_iter), Some(3));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(7));
        assert_eq!(union_current(&mut union_iter), Some(7));
        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(7));
        assert_eq!(union_current(&mut union_iter), Some(7));
        assert_eq!(union_next(&mut union_iter), Some(8));
        assert_eq!(union_current(&mut union_iter), Some(8));
        assert_eq!(union_next(&mut union_iter), Some(9));
        assert_eq!(union_current(&mut union_iter), Some(9));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
    }

    #[test]
    fn test_union_close() {
        let column_fst_x = make_gict(&[1, 2], &[0]);
        let column_fst_y = make_gict(&[3, 4, 5], &[0, 2]);
        let column_snd_x = make_gict(&[2], &[0]);
        let column_snd_y = make_gict(&[5, 6], &[0]);
        let schema_fst = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_snd = schema_fst.clone();

        let trie_fst = Trie::new(schema_fst, vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(schema_snd, vec![column_snd_x, column_snd_y]);
        let trie_iter_fst = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_snd));

        let mut union_iter = TrieUnion::new(vec![trie_iter_fst, trie_iter_snd]);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(3));
        assert_eq!(union_current(&mut union_iter), Some(3));
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(5));
        assert_eq!(union_current(&mut union_iter), Some(5));
        assert_eq!(union_next(&mut union_iter), Some(6));
        assert_eq!(union_current(&mut union_iter), Some(6));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);
    }

    #[test]
    fn another_test() {
        let column_fst_x = make_gict(&[1, 2, 5, 7], &[0]);
        let column_fst_y = make_gict(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);
        let column_snd_x = make_gict(&[4, 7, 8, 9, 10], &[0]);
        let column_snd_y = make_gict(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);
        let schema_fst = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_snd = schema_fst.clone();

        let trie_fst = Trie::new(schema_fst, vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(schema_snd, vec![column_snd_x, column_snd_y]);
        let trie_iter_fst = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_snd));

        let mut union_iter = TrieUnion::new(vec![trie_iter_fst, trie_iter_snd]);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), Some(3));
        assert_eq!(union_current(&mut union_iter), Some(3));
        assert_eq!(union_next(&mut union_iter), Some(5));
        assert_eq!(union_current(&mut union_iter), Some(5));
        assert_eq!(union_next(&mut union_iter), Some(10));
        assert_eq!(union_current(&mut union_iter), Some(10));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), Some(7));
        assert_eq!(union_current(&mut union_iter), Some(7));
        assert_eq!(union_next(&mut union_iter), Some(10));
        assert_eq!(union_current(&mut union_iter), Some(10));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(5));
        assert_eq!(union_current(&mut union_iter), Some(5));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(9));
        assert_eq!(union_current(&mut union_iter), Some(9));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(7));
        assert_eq!(union_current(&mut union_iter), Some(7));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), Some(8));
        assert_eq!(union_current(&mut union_iter), Some(8));
        assert_eq!(union_next(&mut union_iter), Some(9));
        assert_eq!(union_current(&mut union_iter), Some(9));
        assert_eq!(union_next(&mut union_iter), Some(10));
        assert_eq!(union_current(&mut union_iter), Some(10));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(8));
        assert_eq!(union_current(&mut union_iter), Some(8));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(9));
        assert_eq!(union_current(&mut union_iter), Some(9));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(10));
        assert_eq!(union_current(&mut union_iter), Some(10));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
    }

    #[test]
    fn another_test_2() {
        let column_fst_x = make_gict(&[4], &[0]);
        let column_fst_y = make_gict(&[1], &[0]);
        let column_fst_z = make_gict(&[2], &[0]);

        let column_snd_x = make_gict(&[1, 2, 4], &[0]);
        let column_snd_y = make_gict(&[4, 4, 1], &[0, 1, 2]);
        let column_snd_z = make_gict(&[1, 1, 4], &[0, 1, 2]);
        let schema_fst = TrieSchema::new(vec![
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

        let schema_snd = schema_fst.clone();

        let trie_fst = Trie::new(schema_fst, vec![column_fst_x, column_fst_y, column_fst_z]);
        let trie_snd = Trie::new(schema_snd, vec![column_snd_x, column_snd_y, column_snd_z]);
        let trie_iter_fst = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_snd));

        let mut union_iter = TrieUnion::new(vec![trie_iter_fst, trie_iter_snd]);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
    }

    #[test]
    fn union_of_joins() {
        let column_a_x = make_gict(&[1, 4], &[0]);
        let column_a_y = make_gict(&[4, 1], &[0, 1]);
        let column_b_y = make_gict(&[1, 2], &[0]);
        let column_b_z = make_gict(&[2, 4], &[0, 1]);

        let column_c_x = make_gict(&[1, 2, 4], &[0]);
        let column_c_y = make_gict(&[2, 4, 4, 1], &[0, 2, 3]);
        let column_d_y = make_gict(&[1, 4], &[0]);
        let column_d_z = make_gict(&[4, 1], &[0, 1]);

        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_target = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 100,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 101,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 102,
                datatype: DataTypeName::U64,
            },
        ]);
        let trie_a = Trie::new(schema.clone(), vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(schema.clone(), vec![column_b_y, column_b_z]);

        let trie_c = Trie::new(schema.clone(), vec![column_c_x, column_c_y]);
        let trie_d = Trie::new(schema, vec![column_d_y, column_d_z]);

        let join_ab_iter = TrieScanEnum::TrieJoin(TrieJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target.clone(),
        ));

        let join_cd_iter = TrieScanEnum::TrieJoin(TrieJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_c)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_d)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
        ));

        let mut union_iter = TrieUnion::new(vec![join_ab_iter, join_cd_iter]);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(1));
        assert_eq!(union_current(&mut union_iter), Some(1));

        union_iter.down();
        assert_eq!(union_current(&mut union_iter), None);
        assert_eq!(union_next(&mut union_iter), Some(2));
        assert_eq!(union_current(&mut union_iter), Some(2));
        assert_eq!(union_next(&mut union_iter), Some(4));
        assert_eq!(union_current(&mut union_iter), Some(4));
        assert_eq!(union_next(&mut union_iter), None);
        assert_eq!(union_current(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);

        union_iter.up();
        assert_eq!(union_next(&mut union_iter), None);
    }
}
