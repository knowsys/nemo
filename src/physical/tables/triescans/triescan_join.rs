use crate::physical::{
    columns::colscans::{ColScan, ColScanCell, ColScanEnum, ColScanJoin, ColScanT},
    datatypes::{DataTypeName, Double, Float},
    tables::tables::TableSchema,
    tables::tries::TrieSchema,
};

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::iter::repeat;

use super::{TrieScan, TrieScanEnum};

/// Structure resulting from joining a set of tries (given as TrieScanJoins),
/// which itself is a TrieScanJoin that can be used in such a join
#[derive(Debug)]
pub struct TrieScanJoin<'a> {
    trie_scans: Vec<TrieScanEnum<'a>>,
    target_schema: TrieSchema,

    current_variable: Option<usize>,

    variable_to_scan: Vec<Vec<usize>>,

    /// We're keeping an [`UnsafeCell`] here since the
    /// [`ColScanT`] are actually borrowed from within
    /// `trie_scans`. We're not actually modifying through these
    /// references (since there's another layer of Cells hidden in
    /// [`ColScanT`], we're just using this satisfy the
    /// borrow checker.  TODO: find a nicer solution for this that
    /// doesn't expose [`UnsafeCell`] as part of the API.
    merge_joins: Vec<UnsafeCell<ColScanT<'a>>>,
}

impl<'a> TrieScanJoin<'a> {
    /// Construct new TrieScanJoin object.
    pub fn new(
        trie_scans: Vec<TrieScanEnum<'a>>,
        bindings: &[Vec<usize>],
        target_schema: TrieSchema,
    ) -> Self {
        let mut variable_to_scan = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();
        let mut merge_join_indices: Vec<Vec<_>> = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();

        let mut merge_joins: Vec<UnsafeCell<ColScanT<'a>>> = Vec::new();

        for (scan_index, binding) in bindings.iter().enumerate() {
            for (col_index, &var) in binding.iter().enumerate() {
                variable_to_scan[var].push(scan_index);
                merge_join_indices[var].push(col_index);
            }
        }

        for (variable, scan_indices) in variable_to_scan.iter().enumerate() {
            macro_rules! merge_join_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let mut scans = Vec::<&ColScanCell<$type>>::new();
                    for (index, &scan_index) in scan_indices.iter().enumerate() {
                        let column_index = merge_join_indices[variable][index];
                        unsafe {
                            let column_scan =
                                &*trie_scans[scan_index].get_scan(column_index).unwrap().get();

                            if let ColScanT::$variant(cs) = column_scan {
                                scans.push(cs);
                            } else {
                                panic!("expected a column scan of type {}", stringify!($type));
                            }
                        }
                    }

                    merge_joins.push(UnsafeCell::new(ColScanT::$variant(ColScanCell::new(
                        ColScanEnum::ColScanJoin(ColScanJoin::new(scans)),
                    ))))
                }};
            }

            match target_schema.get_type(variable) {
                DataTypeName::U64 => merge_join_for_datatype!(U64, u64),
                DataTypeName::Float => merge_join_for_datatype!(Float, Float),
                DataTypeName::Double => merge_join_for_datatype!(Double, Double),
            }
        }

        Self {
            trie_scans,
            target_schema,
            current_variable: None,
            variable_to_scan,
            merge_joins,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanJoin<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_variable.is_some());
        let current_variable = self.current_variable.unwrap();
        let current_scans = &self.variable_to_scan[current_variable];

        for &scan_index in current_scans {
            self.trie_scans[scan_index].up();
        }

        self.current_variable = if current_variable == 0 {
            None
        } else {
            Some(current_variable - 1)
        };
    }

    fn down(&mut self) {
        let current_variable = self.current_variable.map_or(0, |v| v + 1);
        self.current_variable = Some(current_variable);

        debug_assert!(current_variable < self.target_schema.arity());

        let current_scans = &self.variable_to_scan[current_variable];

        for &scan_index in current_scans {
            self.trie_scans[scan_index].down();
        }

        self.merge_joins[current_variable].get_mut().reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColScanT<'a>>> {
        debug_assert!(self.current_variable.is_some());

        match self.target_schema.get_type(self.current_variable?) {
            DataTypeName::U64 | DataTypeName::Float | DataTypeName::Double => {
                Some(&self.merge_joins[self.current_variable?])
            }
        }
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColScanT<'a>>> {
        Some(&self.merge_joins[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.target_schema
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanJoin;
    use crate::physical::columns::builders::{ColBuilder, ColBuilderAdaptive};
    use crate::physical::columns::columns::{
        Column, ColumnEnum, ColumnVector, IntervalColumnEnum, IntervalColumnGeneric,
        IntervalColumnT,
    };
    use crate::physical::tables::tries::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::tables::triescans::{
        materialize, TrieScan, TrieScanEnum, TrieScanGeneric,
    };
    use crate::physical::{columns::colscans::ColScanT, datatypes::DataTypeName};

    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn join_next(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            if let ColScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.next()
            } else {
                panic!("type should be u64");
            }
        }
    }

    fn join_current(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            if let ColScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_trie_join() {
        let column_a_x = make_gict(&[1, 2, 3], &[0]);
        let column_a_y = make_gict(&[2, 3, 4, 5, 6, 7], &[0, 3, 4]);
        let column_b_y = make_gict(&[1, 2, 3, 6], &[0]);
        let column_b_z = make_gict(&[1, 8, 9, 10, 11, 12], &[0, 1, 3, 4]);

        let schema_a = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
        ]);
        let schema_b = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 12,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 13,
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
        //TODO: Maybe schema schould just be copyable
        let schema_target_clone = schema_target.clone();
        let schema_target_clone_2 = schema_target.clone();

        let trie_a = Trie::new(schema_a, vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(schema_b, vec![column_b_y, column_b_z]);

        let mut join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
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

        let column_c_x = make_gict(&[1, 2], &[0]);
        let column_c_y = make_gict(&[2, 8], &[0, 1]);

        let schema_c = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 20,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 21,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_c = Trie::new(schema_c, vec![column_c_x, column_c_y]);
        let join_iter_ab = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target_clone,
        );

        let mut join_iter_abc = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanJoin(join_iter_ab),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_c)),
            ],
            &[vec![0, 1, 2], vec![0, 1]],
            schema_target_clone_2,
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
        let column_x = make_gict(&[1, 2, 5, 7], &[0]);
        let column_y = make_gict(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);

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

        let trie = Trie::new(schema, vec![column_x, column_y]);

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

        let mut join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
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
        let column_x = make_gict(&[1, 2, 5, 7], &[0]);
        let column_y = make_gict(&[2, 3, 5, 10, 4, 7, 10, 9, 8, 9, 10], &[0, 4, 7, 8]);
        let column_inv_x = make_gict(&[2, 3, 4, 5, 7, 8, 9, 10], &[0]);
        let column_inv_y = make_gict(
            &[1, 1, 2, 1, 2, 7, 5, 7, 1, 2, 7],
            &[0, 1, 2, 3, 4, 5, 6, 8],
        );

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

        let trie = Trie::new(schema, vec![column_x, column_y]);

        let schema_inv = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 20,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 21,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_inv = Trie::new(schema_inv, vec![column_inv_x, column_inv_y]);

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

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_inv)),
            ],
            &[vec![0, 2], vec![1, 2]],
            schema_target,
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter));

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
        let column_new_x = make_gict(&[1, 2, 4, 7, 8, 9, 10], &[0]);
        let column_new_y = make_gict(
            &[4, 7, 9, 8, 9, 1, 1, 2, 1, 2, 1, 2],
            &[0, 3, 5, 6, 7, 8, 10],
        );

        let column_old_x = make_gict(&[1, 2, 4, 5, 7, 8, 9, 10], &[0]);
        let column_old_y = make_gict(
            &[
                2, 3, 4, 5, 7, 10, 4, 7, 8, 9, 10, 1, 9, 1, 8, 9, 10, 2, 1, 2, 1, 2,
            ],
            &[0, 6, 11, 12, 13, 17, 18, 20],
        );

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

        let trie_new = Trie::new(schema, vec![column_new_x, column_new_y]);

        let schema_inv = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 20,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 21,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_old = Trie::new(schema_inv, vec![column_old_x, column_old_y]);

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

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_new)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_old)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter));

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
        let column_a_x = make_gict(&[1, 2, 3, 4, 5], &[0]);
        let column_a_y = make_gict(&[2, 3, 1, 2, 4, 2, 3, 2, 4, 4], &[0, 2, 5, 7, 9]);

        let column_b_x = make_gict(&[2, 3], &[0]);
        let column_b_y = make_gict(&[2, 3, 4, 1, 2, 4], &[0, 3]);

        let schema_a = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_a = Trie::new(schema_a, vec![column_a_x, column_a_y]);

        let schema_b = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 20,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 21,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_b = Trie::new(schema_b, vec![column_b_x, column_b_y]);

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

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &[vec![1, 2], vec![0, 1]],
            schema_target,
        );

        let join_trie = materialize(&mut TrieScanEnum::TrieScanJoin(join_iter));

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
        let mut builder = ColBuilderAdaptive::<u64>::new();
        builder.add(2);
        builder.add(3);
        builder.add(4);

        let schema = TrieSchema::new(vec![TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        }]);

        let schema_target = TrieSchema::new(vec![TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        }]);

        let built_interval_col = IntervalColumnGeneric::<u64>::new(
            builder.finalize(),
            ColumnEnum::ColumnVector(ColumnVector::new(vec![0])),
        );

        let my_trie = Trie::new(
            schema,
            vec![IntervalColumnT::U64(
                IntervalColumnEnum::IntervalColumnGeneric(built_interval_col),
            )],
        );

        let mut my_join_iter = TrieScanJoin::new(
            vec![TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
                &my_trie,
            ))],
            &[vec![0]],
            schema_target,
        );

        my_join_iter.down();
        assert_eq!(join_next(&mut my_join_iter), Some(2));
        assert_eq!(join_next(&mut my_join_iter), Some(3));
        assert_eq!(join_next(&mut my_join_iter), Some(4));
        assert_eq!(join_next(&mut my_join_iter), None);
    }
}
