use crate::physical::{
    columnar::{
        operations::ColumnScanUnion,
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{Double, Float, StorageTypeName},
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// [`TrieScan`] representing a union of other trie iterators
#[derive(Debug)]
pub struct TrieScanUnion<'a> {
    /// Trie scans for which the join is computed
    trie_scans: Vec<TrieScanEnum<'a>>,

    /// For each trie scan contains its current layer
    layers: Vec<Option<usize>>,

    /// For each layer in the resulting trie, contains a [`ColumnScanUnion`] for the union
    /// of the relevant scans in the sub tries.
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`]
    union_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,
}

impl<'a> TrieScanUnion<'a> {
    /// Construct new [`TrieScanUnion`] object.
    pub fn new(trie_scans: Vec<TrieScanEnum<'a>>) -> TrieScanUnion<'a> {
        debug_assert!(!trie_scans.is_empty());
        debug_assert!(trie_scans
            .iter()
            .all(|scan| scan.get_types().len() == trie_scans[0].get_types().len()));

        let column_types = trie_scans[0].get_types();
        let mut union_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(column_types.len());

        for layer_index in 0..column_types.len() {
            macro_rules! init_scans_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let mut column_scans =
                        Vec::<&'a ColumnScanCell<$type>>::with_capacity(column_types.len());

                    for scan in &trie_scans {
                        unsafe {
                            if let ColumnScanT::$variant(scan_enum) =
                                &*scan.get_scan(layer_index).unwrap().get()
                            {
                                column_scans.push(scan_enum);
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($type));
                            }
                        }
                    }

                    let new_union_scan =
                        ColumnScanEnum::ColumnScanUnion(ColumnScanUnion::new(column_scans));

                    union_scans.push(UnsafeCell::new(ColumnScanT::$variant(ColumnScanCell::new(
                        new_union_scan,
                    ))));
                }};
            }

            match column_types[layer_index] {
                StorageTypeName::U32 => init_scans_for_datatype!(U32, u32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64, u64),
                StorageTypeName::I64 => init_scans_for_datatype!(I64, i64),
                StorageTypeName::Float => init_scans_for_datatype!(Float, Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double, Double),
            };
        }

        let layers = vec![None; trie_scans.len()];

        TrieScanUnion {
            trie_scans,
            layers,
            union_scans,
            current_layer: None,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanUnion<'a> {
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

        debug_assert!(self.current_layer.unwrap() < self.trie_scans[0].get_types().len());

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

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.union_scans[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.union_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        self.trie_scans[0].get_types()
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanUnion;
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::tabular::operations::{JoinBindings, TrieScanJoin};
    use crate::physical::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn union_next(union_scan: &mut TrieScanUnion) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = union_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn union_current(union_scan: &mut TrieScanUnion) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = union_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_union() {
        let column_fst_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
        let column_fst_y = make_column_with_intervals_t(&[2, 5, 4], &[0, 1, 2]);
        let column_snd_x = make_column_with_intervals_t(&[1, 2, 7], &[0]);
        let column_snd_y = make_column_with_intervals_t(&[2, 4, 4, 8, 9], &[0, 2, 3]);
        let column_trd_x = make_column_with_intervals_t(&[1, 3, 7], &[0]);
        let column_trd_y = make_column_with_intervals_t(&[1, 4, 6, 3, 7, 8], &[0, 3, 4]);

        let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
        let trie_trd = Trie::new(vec![column_trd_x, column_trd_y]);

        let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));
        let trie_iter_trd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_trd));
        let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd, trie_iter_trd]);
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
        let column_fst_x = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_fst_y = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);
        let column_snd_x = make_column_with_intervals_t(&[2], &[0]);
        let column_snd_y = make_column_with_intervals_t(&[5, 6], &[0]);

        let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
        let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

        let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
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
        let column_fst_x = make_column_with_intervals_t(&[1, 2, 5, 7], &[0]);
        let column_fst_y =
            make_column_with_intervals_t(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);
        let column_snd_x = make_column_with_intervals_t(&[4, 7, 8, 9, 10], &[0]);
        let column_snd_y = make_column_with_intervals_t(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);

        let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
        let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
        let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

        let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
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
        let column_fst_x = make_column_with_intervals_t(&[4], &[0]);
        let column_fst_y = make_column_with_intervals_t(&[1], &[0]);
        let column_fst_z = make_column_with_intervals_t(&[2], &[0]);

        let column_snd_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
        let column_snd_y = make_column_with_intervals_t(&[4, 4, 1], &[0, 1, 2]);
        let column_snd_z = make_column_with_intervals_t(&[1, 1, 4], &[0, 1, 2]);

        let trie_fst = Trie::new(vec![column_fst_x, column_fst_y, column_fst_z]);
        let trie_snd = Trie::new(vec![column_snd_x, column_snd_y, column_snd_z]);
        let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
        let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

        let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
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
        let column_a_x = make_column_with_intervals_t(&[1, 4], &[0]);
        let column_a_y = make_column_with_intervals_t(&[4, 1], &[0, 1]);
        let column_b_y = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_b_z = make_column_with_intervals_t(&[2, 4], &[0, 1]);

        let column_c_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
        let column_c_y = make_column_with_intervals_t(&[2, 4, 4, 1], &[0, 2, 3]);
        let column_d_y = make_column_with_intervals_t(&[1, 4], &[0]);
        let column_d_z = make_column_with_intervals_t(&[4, 1], &[0, 1]);

        let trie_a = Trie::new(vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(vec![column_b_y, column_b_z]);

        let trie_c = Trie::new(vec![column_c_x, column_c_y]);
        let trie_d = Trie::new(vec![column_d_y, column_d_z]);

        let join_ab_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        ));

        let join_cd_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_c)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_d)),
            ],
            &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        ));

        let mut union_iter = TrieScanUnion::new(vec![join_ab_iter, join_cd_iter]);
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
