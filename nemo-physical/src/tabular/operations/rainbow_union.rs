use crate::{
    columnar::{
        operations::ColumnScanUnion,
        traits::columnscan::{ColumnScanCell, ColumnScanEnum},
    },
    datatypes::{Double, StorageTypeName},
    tabular::{
        table_types::trie_rainbow::{ColumnScanRainbow, PartialTrieScanRainbow},
        traits::partial_trie_scan::TrieScanRainbowEnum,
    },
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// [`PartialTrieScan`] representing a union of other trie iterators
#[derive(Debug)]
pub struct TrieScanUnionRainbow<'a> {
    /// Trie scans for which the join is computed
    trie_scans: Vec<TrieScanRainbowEnum<'a>>,

    /// For each layer in the resulting trie contains a [`ColumnScanRainbow`]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,
}

impl<'a> TrieScanUnionRainbow<'a> {
    /// Construct new [`TrieScanUnion`] object.
    pub fn new(trie_scans: Vec<TrieScanRainbowEnum<'a>>) -> Self {
        debug_assert!(trie_scans
            .iter()
            .all(|scan| scan.arity() == trie_scans[0].arity()));

        let arity = trie_scans.first().map_or(0, |s| s.arity());
        let mut column_scans = Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(arity);

        for layer in 0..arity {
            let mut column_scans_keys =
                Vec::<&'a ColumnScanCell<u64>>::with_capacity(trie_scans.len());
            let mut column_scans_integers =
                Vec::<&'a ColumnScanCell<i64>>::with_capacity(trie_scans.len());
            let mut column_scans_doubles =
                Vec::<&'a ColumnScanCell<Double>>::with_capacity(trie_scans.len());

            for trie_scan in &trie_scans {
                let scan_keys = &unsafe {
                    &*trie_scan
                        .scan(layer)
                        .expect("Every trie scan contains less then arity elements")
                        .get()
                }
                .scan_keys;
                column_scans_keys.push(scan_keys);

                let scan_integers = &unsafe {
                    &*trie_scan
                        .scan(layer)
                        .expect("Every trie scan contains less then arity elements")
                        .get()
                }
                .scan_integers;
                column_scans_integers.push(scan_integers);

                let scan_doubles = &unsafe {
                    &*trie_scan
                        .scan(layer)
                        .expect("Every trie scan contains less then arity elements")
                        .get()
                }
                .scan_doubles;
                column_scans_doubles.push(scan_doubles);
            }

            let new_union_scan_keys =
                ColumnScanEnum::ColumnScanUnion(ColumnScanUnion::new(column_scans_keys));
            let new_union_scan_integers =
                ColumnScanEnum::ColumnScanUnion(ColumnScanUnion::new(column_scans_integers));
            let new_union_scan_doubles =
                ColumnScanEnum::ColumnScanUnion(ColumnScanUnion::new(column_scans_doubles));

            let new_scan = ColumnScanRainbow::new(
                new_union_scan_keys,
                new_union_scan_integers,
                new_union_scan_doubles,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        Self {
            trie_scans,
            column_scans,
            current_layer: None,
        }
    }
}

impl<'a> PartialTrieScanRainbow<'a> for TrieScanUnionRainbow<'a> {
    fn up(&mut self) {
        let up_layer = self
            .current_layer
            .expect("calling up only allowed after calling down")
            .checked_sub(1);

        for trie_scan in &mut self.trie_scans {
            if trie_scan.current_layer() == self.current_layer {
                trie_scan.up();
            }
        }
        self.current_layer = up_layer;
    }

    fn down(&mut self, storage_type: StorageTypeName) {
        let previous_layer = self.current_layer;
        let next_layer = self.current_layer.map_or(0, |v| v + 1);
        self.current_layer = Some(next_layer);

        debug_assert!(self.current_layer.unwrap() < self.arity());

        let mut active_scans = Vec::<usize>::with_capacity(self.arity());
        for (scan_index, trie_scan) in self.trie_scans.iter_mut().enumerate() {
            if trie_scan.current_layer() != previous_layer {
                continue;
            }

            if previous_layer.is_none()
                || self.column_scans[previous_layer.unwrap()]
                    .get_mut()
                    .get_smallest_scans(storage_type)[scan_index]
            {
                active_scans.push(scan_index);

                trie_scan.down(storage_type);
            }
        }

        self.column_scans[next_layer]
            .get_mut()
            .set_active_scans(storage_type, active_scans);
        self.column_scans[next_layer].get_mut().reset(storage_type);
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanRainbow<'a>> {
        Some(self.column_scans[self.current_layer?].get_mut())
    }

    fn scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>> {
        Some(&self.column_scans[index])
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }

    fn arity(&self) -> usize {
        self.trie_scans.first().map_or(0, |s| s.arity())
    }
}

// #[cfg(test)]
// mod test {
//     use super::TrieScanUnion;
//     use crate::columnar::traits::columnscan::ColumnScanT;
//     use crate::tabular::operations::{JoinBindings, TrieScanJoin};
//     use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
//     use crate::tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum};
//     use crate::util::test_util::make_column_with_intervals_t;
//     use test_log::test;

//     fn union_next(union_scan: &mut TrieScanUnion) -> Option<u64> {
//         if let ColumnScanT::U64(rcs) = union_scan.current_scan()? {
//             rcs.next()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     fn union_current(union_scan: &mut TrieScanUnion) -> Option<u64> {
//         if let ColumnScanT::U64(rcs) = union_scan.current_scan()? {
//             rcs.current()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     #[test]
//     fn test_union() {
//         let column_fst_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
//         let column_fst_y = make_column_with_intervals_t(&[2, 5, 4], &[0, 1, 2]);
//         let column_snd_x = make_column_with_intervals_t(&[1, 2, 7], &[0]);
//         let column_snd_y = make_column_with_intervals_t(&[2, 4, 4, 8, 9], &[0, 2, 3]);
//         let column_trd_x = make_column_with_intervals_t(&[1, 3, 7], &[0]);
//         let column_trd_y = make_column_with_intervals_t(&[1, 4, 6, 3, 7, 8], &[0, 3, 4]);

//         let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
//         let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
//         let trie_trd = Trie::new(vec![column_trd_x, column_trd_y]);

//         let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
//         let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));
//         let trie_iter_trd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_trd));
//         let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd, trie_iter_trd]);
//         assert_eq!(union_current(&mut union_iter), None);
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), Some(6));
//         assert_eq!(union_current(&mut union_iter), Some(6));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), Some(5));
//         assert_eq!(union_current(&mut union_iter), Some(5));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(3));
//         assert_eq!(union_current(&mut union_iter), Some(3));
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(3));
//         assert_eq!(union_current(&mut union_iter), Some(3));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(7));
//         assert_eq!(union_current(&mut union_iter), Some(7));
//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(7));
//         assert_eq!(union_current(&mut union_iter), Some(7));
//         assert_eq!(union_next(&mut union_iter), Some(8));
//         assert_eq!(union_current(&mut union_iter), Some(8));
//         assert_eq!(union_next(&mut union_iter), Some(9));
//         assert_eq!(union_current(&mut union_iter), Some(9));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//     }

//     #[test]
//     fn test_union_close() {
//         let column_fst_x = make_column_with_intervals_t(&[1, 2], &[0]);
//         let column_fst_y = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);
//         let column_snd_x = make_column_with_intervals_t(&[2], &[0]);
//         let column_snd_y = make_column_with_intervals_t(&[5, 6], &[0]);

//         let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
//         let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
//         let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
//         let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

//         let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(3));
//         assert_eq!(union_current(&mut union_iter), Some(3));
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(5));
//         assert_eq!(union_current(&mut union_iter), Some(5));
//         assert_eq!(union_next(&mut union_iter), Some(6));
//         assert_eq!(union_current(&mut union_iter), Some(6));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);
//     }

//     #[test]
//     fn another_test() {
//         let column_fst_x = make_column_with_intervals_t(&[1, 2, 5, 7], &[0]);
//         let column_fst_y =
//             make_column_with_intervals_t(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);
//         let column_snd_x = make_column_with_intervals_t(&[4, 7, 8, 9, 10], &[0]);
//         let column_snd_y = make_column_with_intervals_t(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);

//         let trie_fst = Trie::new(vec![column_fst_x, column_fst_y]);
//         let trie_snd = Trie::new(vec![column_snd_x, column_snd_y]);
//         let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
//         let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

//         let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), Some(3));
//         assert_eq!(union_current(&mut union_iter), Some(3));
//         assert_eq!(union_next(&mut union_iter), Some(5));
//         assert_eq!(union_current(&mut union_iter), Some(5));
//         assert_eq!(union_next(&mut union_iter), Some(10));
//         assert_eq!(union_current(&mut union_iter), Some(10));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), Some(7));
//         assert_eq!(union_current(&mut union_iter), Some(7));
//         assert_eq!(union_next(&mut union_iter), Some(10));
//         assert_eq!(union_current(&mut union_iter), Some(10));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(5));
//         assert_eq!(union_current(&mut union_iter), Some(5));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(9));
//         assert_eq!(union_current(&mut union_iter), Some(9));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(7));
//         assert_eq!(union_current(&mut union_iter), Some(7));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), Some(8));
//         assert_eq!(union_current(&mut union_iter), Some(8));
//         assert_eq!(union_next(&mut union_iter), Some(9));
//         assert_eq!(union_current(&mut union_iter), Some(9));
//         assert_eq!(union_next(&mut union_iter), Some(10));
//         assert_eq!(union_current(&mut union_iter), Some(10));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(8));
//         assert_eq!(union_current(&mut union_iter), Some(8));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(9));
//         assert_eq!(union_current(&mut union_iter), Some(9));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(10));
//         assert_eq!(union_current(&mut union_iter), Some(10));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//     }

//     #[test]
//     fn another_test_2() {
//         let column_fst_x = make_column_with_intervals_t(&[4], &[0]);
//         let column_fst_y = make_column_with_intervals_t(&[1], &[0]);
//         let column_fst_z = make_column_with_intervals_t(&[2], &[0]);

//         let column_snd_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
//         let column_snd_y = make_column_with_intervals_t(&[4, 4, 1], &[0, 1, 2]);
//         let column_snd_z = make_column_with_intervals_t(&[1, 1, 4], &[0, 1, 2]);

//         let trie_fst = Trie::new(vec![column_fst_x, column_fst_y, column_fst_z]);
//         let trie_snd = Trie::new(vec![column_snd_x, column_snd_y, column_snd_z]);
//         let trie_iter_fst = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_fst));
//         let trie_iter_snd = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_snd));

//         let mut union_iter = TrieScanUnion::new(vec![trie_iter_fst, trie_iter_snd]);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//     }

//     #[test]
//     fn union_of_joins() {
//         let column_a_x = make_column_with_intervals_t(&[1, 4], &[0]);
//         let column_a_y = make_column_with_intervals_t(&[4, 1], &[0, 1]);
//         let column_b_y = make_column_with_intervals_t(&[1, 2], &[0]);
//         let column_b_z = make_column_with_intervals_t(&[2, 4], &[0, 1]);

//         let column_c_x = make_column_with_intervals_t(&[1, 2, 4], &[0]);
//         let column_c_y = make_column_with_intervals_t(&[2, 4, 4, 1], &[0, 2, 3]);
//         let column_d_y = make_column_with_intervals_t(&[1, 4], &[0]);
//         let column_d_z = make_column_with_intervals_t(&[4, 1], &[0, 1]);

//         let trie_a = Trie::new(vec![column_a_x, column_a_y]);
//         let trie_b = Trie::new(vec![column_b_y, column_b_z]);

//         let trie_c = Trie::new(vec![column_c_x, column_c_y]);
//         let trie_d = Trie::new(vec![column_d_y, column_d_z]);

//         let join_ab_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
//             vec![
//                 TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
//                 TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
//             ],
//             &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
//         ));

//         let join_cd_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
//             vec![
//                 TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_c)),
//                 TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_d)),
//             ],
//             &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
//         ));

//         let mut union_iter = TrieScanUnion::new(vec![join_ab_iter, join_cd_iter]);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(1));
//         assert_eq!(union_current(&mut union_iter), Some(1));

//         union_iter.down();
//         assert_eq!(union_current(&mut union_iter), None);
//         assert_eq!(union_next(&mut union_iter), Some(2));
//         assert_eq!(union_current(&mut union_iter), Some(2));
//         assert_eq!(union_next(&mut union_iter), Some(4));
//         assert_eq!(union_current(&mut union_iter), Some(4));
//         assert_eq!(union_next(&mut union_iter), None);
//         assert_eq!(union_current(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);

//         union_iter.up();
//         assert_eq!(union_next(&mut union_iter), None);
//     }
// }
