use crate::physical::{
    columnar::{
        operations::{ColumnScanJoin, ColumnScanPass},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{DataTypeName, Double, Float},
    tabular::traits::{
        table_schema::TableColumnTypes,
        triescan::{TrieScan, TrieScanEnum},
    },
};

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::iter::repeat;

/// A [`JoinBinding`] is a vector of `<Vec<usize>` where binding[i]
/// contains which layer of the ith subscan is bound to which variable
/// (Variables are represented by their index in the variable order)
/// So the join R(a, b) S(b, c) T(a, c) with variable order [a, b, c] is represented
/// with the binding [[0, 1], [1, 2], [0, 2]] (assuming trie_scans = [R, S, T])
pub type JoinBinding = Vec<Vec<usize>>;

/// [`TrieScan`] which represents the result from joining a set of tries (given as [`TrieScan`]s),
#[derive(Debug)]
pub struct TrieScanJoin<'a> {
    /// Trie scans of which the join is computed
    trie_scans: Vec<TrieScanEnum<'a>>,

    /// Types of the resulting trie
    target_types: TableColumnTypes,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,

    /// Indices of the sub tries which are assosiated with a given layer in the result
    /// E.g., given the join R(a, b), S(b, c), T(a, c) with variable order a, b, c
    /// we have layers_to_scans = [[R, T], [R, S], [S, T]]
    /// or rather layers_to_scans = [[0, 2], [0, 1], [1, 2]] if trie_scans = [R, S, T]
    layer_to_scans: Vec<Vec<usize>>,

    /// For each layer in the resulting trie, contains a [`ColumnScan`] for the intersection
    /// of the relevant scans in the sub tries.
    /// Note: We're keeping an [`UnsafeCell`] here since the
    /// [`ColumnScanT`] are actually borrowed from within
    /// `trie_scans`. We're not actually modifying through these
    /// references (since there's another layer of Cells hidden in
    /// [`ColumnScanT`], we're just using this satisfy the
    /// borrow checker.  TODO: find a nicer solution for this that
    /// doesn't expose [`UnsafeCell`] as part of the API.
    merge_joins: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanJoin<'a> {
    /// Construct new [`TrieScanJoin`] object.
    /// Assumes that each entry in `bindings``is sorted and does not contain duplicates
    pub fn new(trie_scans: Vec<TrieScanEnum<'a>>, bindings: &JoinBinding) -> Self {
        debug_assert!(bindings.len() == trie_scans.len());
        debug_assert!(trie_scans
            .iter()
            .enumerate()
            .all(
                |(scan_index, scan_enum)| bindings[scan_index].len() == scan_enum.get_types().len()
            ));
        debug_assert!(bindings
            .iter()
            .all(|binding| !binding.is_empty() && binding.is_sorted()));

        // Calculate the arity of the result trie
        let target_arity = 1 + bindings.iter().fold(0, |acc, v| {
            acc.max(*v.iter().max().expect("Each binding should be non-empty."))
        });

        // Indices of the sub tries which are assosiated with a given layer in the result
        let mut layer_to_scans = repeat(Vec::new()).take(target_arity).collect::<Vec<_>>();

        // For each layer in the result trie, contains
        let mut merge_join_indices: Vec<Vec<_>> =
            repeat(Vec::new()).take(target_arity).collect::<Vec<_>>();

        // Types of the columns in the result trie
        let mut target_types = vec![DataTypeName::U64; target_arity];

        // Continuing the example from above we obtain
        // layers_to_scans = [[0, 2], [0, 1], [1, 2]] and
        // merge_join_indices = [[0, 0], [1, 0], [1, 1]]
        // Combining this information gives you e.g. that for the second layer in the result
        // you need to join the second column of the first scan with the first column of the second scan
        for (scan_index, binding) in bindings.iter().enumerate() {
            for (col_index, &var_index) in binding.iter().enumerate() {
                layer_to_scans[var_index].push(scan_index);
                merge_join_indices[var_index].push(col_index);

                target_types[var_index] = trie_scans[scan_index].get_types()[col_index];
            }
        }

        let mut merge_joins: Vec<UnsafeCell<ColumnScanT<'a>>> = Vec::new();

        // This loop builds the [`ColumnScanJoin`] as suggested above
        for (var_index, scan_indices) in layer_to_scans.iter().enumerate() {
            macro_rules! merge_join_for_datatype {
                ($variant:ident, $type:ty) => {{
                    if scan_indices.len() > 1 {
                        let mut scans = Vec::<&ColumnScanCell<$type>>::new();
                        for (index, &scan_index) in scan_indices.iter().enumerate() {
                            let column_index = merge_join_indices[var_index][index];
                            unsafe {
                                let column_scan =
                                    &*trie_scans[scan_index].get_scan(column_index).unwrap().get();

                                if let ColumnScanT::$variant(cs) = column_scan {
                                    scans.push(cs);
                                } else {
                                    panic!("Expected a column scan of type {}", stringify!($type));
                                }
                            }
                        }

                        merge_joins.push(UnsafeCell::new(ColumnScanT::$variant(ColumnScanCell::new(
                            ColumnScanEnum::ColumnScanJoin(ColumnScanJoin::new(scans)),
                        ))))
                    } else {
                        // If we have only one column then no join is neccessary and we use a [`ColumnScanPass`]
                        let scan_index = scan_indices[0];
                        let column_index = merge_join_indices[var_index][0];

                        unsafe {
                            let column_scan =
                                &*trie_scans[scan_index].get_scan(column_index).unwrap().get();

                            if let ColumnScanT::$variant(cs) = column_scan {
                                merge_joins.push(UnsafeCell::new(ColumnScanT::$variant(ColumnScanCell::new(
                                    ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(cs))
                                ))));
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($type));
                            }
                        }
                    }
                }};
            }

            match target_types[var_index] {
                DataTypeName::U32 => merge_join_for_datatype!(U32, u32),
                DataTypeName::U64 => merge_join_for_datatype!(U64, u64),
                DataTypeName::Float => merge_join_for_datatype!(Float, Float),
                DataTypeName::Double => merge_join_for_datatype!(Double, Double),
            }
        }

        Self {
            trie_scans,
            target_types,
            current_layer: None,
            layer_to_scans,
            merge_joins,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanJoin<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());
        let current_layer = self.current_layer.unwrap();
        let current_scans = &self.layer_to_scans[current_layer];

        for &scan_index in current_scans {
            self.trie_scans[scan_index].up();
        }

        self.current_layer = if current_layer == 0 {
            None
        } else {
            Some(current_layer - 1)
        };
    }

    fn down(&mut self) {
        let current_layer = self.current_layer.map_or(0, |v| v + 1);
        self.current_layer = Some(current_layer);

        debug_assert!(current_layer < self.target_types.len());

        let current_scans = &self.layer_to_scans[current_layer];

        for &scan_index in current_scans {
            self.trie_scans[scan_index].down();
        }

        // The above down call has changed the sub scans of the current layer
        // Hence, we need to reset its state
        self.merge_joins[current_layer].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        debug_assert!(self.current_layer.is_some());

        Some(&self.merge_joins[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.merge_joins[index])
    }

    fn get_types(&self) -> &TableColumnTypes {
        &self.target_types
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanJoin;
    use crate::physical::columnar::adaptive_column_builder::ColumnBuilderAdaptive;
    use crate::physical::columnar::column_types::{
        interval::{ColumnWithIntervals, ColumnWithIntervalsT},
        vector::ColumnVector,
    };
    use crate::physical::columnar::traits::{
        column::{Column, ColumnEnum},
        columnbuilder::ColumnBuilder,
        columnscan::ColumnScanT,
    };
    use crate::physical::tabular::operations::materialize;
    use crate::physical::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};

    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn join_next(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            if let ColumnScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.next()
            } else {
                panic!("type should be u64");
            }
        }
    }

    fn join_current(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            if let ColumnScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_trie_join() {
        let column_a_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_a_y = make_column_with_intervals_t(&[2, 3, 4, 5, 6, 7], &[0, 3, 4]);
        let column_b_y = make_column_with_intervals_t(&[1, 2, 3, 6], &[0]);
        let column_b_z = make_column_with_intervals_t(&[1, 8, 9, 10, 11, 12], &[0, 1, 3, 4]);

        //TODO: Maybe schema schould just be copyable
        let trie_a = Trie::new(vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(vec![column_b_y, column_b_z]);

        let mut join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
        );

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(1));
        assert_eq!(join_current(&mut join_iter), Some(1));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(2));
        assert_eq!(join_current(&mut join_iter), Some(2));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(8));
        assert_eq!(join_current(&mut join_iter), Some(8));
        assert_eq!(join_next(&mut join_iter), Some(9));
        assert_eq!(join_current(&mut join_iter), Some(9));
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(3));
        assert_eq!(join_current(&mut join_iter), Some(3));
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(2));
        assert_eq!(join_current(&mut join_iter), Some(2));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(3));
        assert_eq!(join_current(&mut join_iter), Some(3));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(6));
        assert_eq!(join_current(&mut join_iter), Some(6));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(11));
        assert_eq!(join_current(&mut join_iter), Some(11));
        assert_eq!(join_next(&mut join_iter), Some(12));
        assert_eq!(join_current(&mut join_iter), Some(12));
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        let column_c_x = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_c_y = make_column_with_intervals_t(&[2, 8], &[0, 1]);

        let trie_c = Trie::new(vec![column_c_x, column_c_y]);
        let join_iter_ab = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
        );

        let mut join_iter_abc = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanJoin(join_iter_ab),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_c)),
            ],
            &vec![vec![0, 1, 2], vec![0, 1]],
        );

        join_iter_abc.down();
        assert_eq!(join_current(&mut join_iter_abc), None);
        assert_eq!(join_next(&mut join_iter_abc), Some(1));
        assert_eq!(join_current(&mut join_iter_abc), Some(1));

        join_iter_abc.down();
        assert_eq!(join_current(&mut join_iter_abc), None);
        assert_eq!(join_next(&mut join_iter_abc), Some(2));
        assert_eq!(join_current(&mut join_iter_abc), Some(2));

        join_iter_abc.down();
        assert_eq!(join_current(&mut join_iter_abc), None);
        assert_eq!(join_next(&mut join_iter_abc), Some(8));
        assert_eq!(join_current(&mut join_iter_abc), Some(8));
        assert_eq!(join_next(&mut join_iter_abc), Some(9));
        assert_eq!(join_current(&mut join_iter_abc), Some(9));
        assert_eq!(join_next(&mut join_iter_abc), None);
        assert_eq!(join_current(&mut join_iter_abc), None);

        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), None);
        assert_eq!(join_current(&mut join_iter_abc), None);

        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), Some(2));
        assert_eq!(join_current(&mut join_iter_abc), Some(2));

        join_iter_abc.down();
        assert_eq!(join_current(&mut join_iter_abc), None);
        assert_eq!(join_next(&mut join_iter_abc), None);
        assert_eq!(join_current(&mut join_iter_abc), None);

        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), None);
        assert_eq!(join_current(&mut join_iter_abc), None);
    }

    #[test]
    fn test_self_join() {
        let column_x = make_column_with_intervals_t(&[1, 2, 5, 7], &[0]);
        let column_y =
            make_column_with_intervals_t(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);

        let trie = Trie::new(vec![column_x, column_y]);

        let mut join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
        );

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(1));
        assert_eq!(join_current(&mut join_iter), Some(1));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(2));
        assert_eq!(join_current(&mut join_iter), Some(2));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(4));
        assert_eq!(join_current(&mut join_iter), Some(4));
        assert_eq!(join_next(&mut join_iter), Some(7));
        assert_eq!(join_current(&mut join_iter), Some(7));
        assert_eq!(join_next(&mut join_iter), Some(10));
        assert_eq!(join_current(&mut join_iter), Some(10));
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(5));
        assert_eq!(join_current(&mut join_iter), Some(5));

        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(9));
        assert_eq!(join_current(&mut join_iter), Some(9));

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);

        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(2));
        assert_eq!(join_current(&mut join_iter), Some(2));

        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(7));
        assert_eq!(join_current(&mut join_iter), Some(7));

        join_iter.down();
        assert_eq!(join_current(&mut join_iter), None);
        assert_eq!(join_next(&mut join_iter), Some(8));
        assert_eq!(join_current(&mut join_iter), Some(8));
        assert_eq!(join_next(&mut join_iter), Some(9));
        assert_eq!(join_current(&mut join_iter), Some(9));
        assert_eq!(join_next(&mut join_iter), Some(10));
        assert_eq!(join_current(&mut join_iter), Some(10));
        assert_eq!(join_next(&mut join_iter), None);
        assert_eq!(join_current(&mut join_iter), None);
    }

    #[test]
    fn test_self_join_inverse() {
        let column_x = make_column_with_intervals_t(&[1, 2, 5, 7], &[0]);
        let column_y =
            make_column_with_intervals_t(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);
        let column_inv_x = make_column_with_intervals_t(&[2, 3, 4, 5, 7, 8, 9, 10], &[0]);
        let column_inv_y = make_column_with_intervals_t(
            &[1, 1, 2, 1, 2, 7, 5, 7, 1, 2, 7],
            &[0, 1, 2, 3, 4, 5, 6, 8],
        );

        let trie = Trie::new(vec![column_x, column_y]);

        let trie_inv = Trie::new(vec![column_inv_x, column_inv_y]);

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_inv)),
            ],
            &vec![vec![0, 2], vec![1, 2]],
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter)).unwrap();

        let join_col_fst = join_trie.get_column(0).as_u64().unwrap();

        let join_col_snd = join_trie.get_column(1).as_u64().unwrap();

        let join_col_trd = join_trie.get_column(2).as_u64().unwrap();

        assert_eq!(
            join_col_fst.get_data_column().iter().collect::<Vec<u64>>(),
            vec![1, 2]
        );

        assert_eq!(
            join_col_snd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![4, 7, 9, 10, 8, 9, 10]
        );

        assert_eq!(
            join_col_trd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![2, 2, 5, 2, 7, 7, 7]
        );
    }

    #[test]
    fn test_self_join_2() {
        let column_new_x = make_column_with_intervals_t(&[1, 2, 4, 7, 8, 9, 10], &[0]);
        let column_new_y = make_column_with_intervals_t(
            &[4, 7, 9, 8, 9, 1, 1, 2, 1, 2, 1, 2],
            &[0, 3, 5, 6, 7, 8, 10],
        );

        let column_old_x = make_column_with_intervals_t(&[1, 2, 4, 5, 7, 8, 9, 10], &[0]);
        let column_old_y = make_column_with_intervals_t(
            &[
                2, 3, 4, 5, 7, 10, 4, 7, 8, 9, 10, 1, 9, 1, 8, 9, 10, 2, 1, 2, 1, 2,
            ],
            &[0, 6, 11, 12, 13, 17, 18, 20],
        );

        let trie_new = Trie::new(vec![column_new_x, column_new_y]);
        let trie_old = Trie::new(vec![column_old_x, column_old_y]);

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_new)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_old)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter)).unwrap();

        let join_col_fst = join_trie.get_column(0).as_u64().unwrap();

        let join_col_snd = join_trie.get_column(1).as_u64().unwrap();

        let join_col_trd = join_trie.get_column(2).as_u64().unwrap();

        assert_eq!(
            join_col_fst.get_data_column().iter().collect::<Vec<u64>>(),
            vec![1, 2, 4, 7, 8, 9, 10]
        );
        assert_eq!(
            join_col_fst.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            join_col_snd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![4, 7, 9, 8, 9, 1, 1, 2, 1, 2, 1, 2]
        );
        assert_eq!(
            join_col_snd.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0, 3, 5, 6, 7, 8, 10]
        );

        assert_eq!(
            join_col_trd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![
                1, 1, 8, 9, 10, 1, 2, 2, 1, 2, 2, 3, 4, 5, 7, 10, 2, 3, 4, 5, 7, 10, 4, 7, 8, 9,
                10, 2, 3, 4, 5, 7, 10, 4, 7, 8, 9, 10, 2, 3, 4, 5, 7, 10, 4, 7, 8, 9, 10
            ]
        );
        assert_eq!(
            join_col_trd.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0, 1, 5, 7, 8, 10, 16, 22, 27, 33, 38, 44]
        );
    }

    #[test]
    fn another_test() {
        let column_a_x = make_column_with_intervals_t(&[1, 2, 3, 4, 5], &[0]);
        let column_a_y =
            make_column_with_intervals_t(&[2, 3, 1, 2, 4, 2, 3, 2, 4, 4], &[0, 2, 5, 7, 9]);

        let column_b_x = make_column_with_intervals_t(&[2, 3], &[0]);
        let column_b_y = make_column_with_intervals_t(&[2, 3, 4, 1, 2, 4], &[0, 3]);

        let trie_a = Trie::new(vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(vec![column_b_x, column_b_y]);

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &vec![vec![1, 2], vec![0, 1]],
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter)).unwrap();

        let join_col_fst = join_trie.get_column(0).as_u64().unwrap();

        let join_col_snd = join_trie.get_column(1).as_u64().unwrap();

        let join_col_trd = join_trie.get_column(2).as_u64().unwrap();

        assert_eq!(
            join_col_fst.get_data_column().iter().collect::<Vec<u64>>(),
            vec![2, 3]
        );
        assert_eq!(
            join_col_fst.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            join_col_snd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![2, 3, 4, 1, 2, 4]
        );
        assert_eq!(
            join_col_snd.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0, 3]
        );

        assert_eq!(
            join_col_trd.get_data_column().iter().collect::<Vec<u64>>(),
            vec![1, 2, 4, 2, 3, 2, 4, 2, 3, 1, 2, 4, 2, 4]
        );
        assert_eq!(
            join_col_trd.get_int_column().iter().collect::<Vec<usize>>(),
            vec![0, 3, 5, 7, 9, 12]
        );
    }

    #[test]
    fn test_dynamic() {
        let mut builder = ColumnBuilderAdaptive::<u64>::default();
        builder.add(2);
        builder.add(3);
        builder.add(4);

        let built_interval_col = ColumnWithIntervals::<u64>::new(
            builder.finalize(),
            ColumnEnum::ColumnVector(ColumnVector::new(vec![0])),
        );

        let my_trie = Trie::new(vec![ColumnWithIntervalsT::U64(built_interval_col)]);

        let mut my_join_iter = TrieScanJoin::new(
            vec![TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
                &my_trie,
            ))],
            &vec![vec![0]],
        );

        my_join_iter.down();
        assert_eq!(join_next(&mut my_join_iter), Some(2));
        assert_eq!(join_next(&mut my_join_iter), Some(3));
        assert_eq!(join_next(&mut my_join_iter), Some(4));
        assert_eq!(join_next(&mut my_join_iter), None);
    }
}
