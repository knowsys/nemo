use super::{TableSchema, TrieScan, TrieScanEnum, TrieSchema};
use crate::physical::columns::{
    ColumnScan, OrderedMergeJoin, RangedColumnScanCell, RangedColumnScanEnum, RangedColumnScanT,
};
use crate::physical::datatypes::{DataTypeName, Double, Float};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::iter::repeat;

/// Structure resulting from joining a set of tries (given as TrieJoins),
/// which itself is a TrieJoin that can be used in such a join
#[derive(Debug)]
pub struct TrieJoin<'a> {
    trie_scans: Vec<TrieScanEnum<'a>>,
    target_schema: TrieSchema,

    current_variable: Option<usize>,

    variable_to_scan: Vec<Vec<usize>>,

    /// We're keeping an [`UnsafeCell`] here since the
    /// [`RangedColumnScanT`] are actually borrowed from within
    /// `trie_scans`. We're not actually modifying through these
    /// references (since there's another layer of Cells hidden in
    /// [`RangedColumnScanT`], we're just using this satisfy the
    /// borrow checker.  TODO: find a nicer solution for this that
    /// doesn't expose [`UnsafeCell`] as part of the API.
    merge_joins: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
}

impl<'a> TrieJoin<'a> {
    /// Construct new TrieJoin object.
    pub fn new(
        trie_scans: Vec<TrieScanEnum<'a>>,
        bindings: &Vec<Vec<usize>>,
        target_schema: TrieSchema,
    ) -> Self {
        let mut variable_to_scan = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();
        let mut merge_join_indices: Vec<Vec<_>> = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();

        let mut merge_joins: Vec<UnsafeCell<RangedColumnScanT<'a>>> = Vec::new();

        for (scan_index, binding) in bindings.iter().enumerate() {
            for (col_index, &var) in binding.iter().enumerate() {
                variable_to_scan[var].push(scan_index);
                merge_join_indices[var].push(col_index);
            }
        }

        for (variable, scan_indices) in variable_to_scan.iter().enumerate() {
            macro_rules! merge_join_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let mut scans = Vec::<&RangedColumnScanCell<$type>>::new();
                    for (index, &scan_index) in scan_indices.iter().enumerate() {
                        let column_index = merge_join_indices[variable][index];
                        unsafe {
                            let column_scan =
                                &*trie_scans[scan_index].get_scan(column_index).unwrap().get();

                            if let RangedColumnScanT::$variant(cs) = column_scan {
                                scans.push(cs);
                            } else {
                                panic!("expected a column scan of type {}", stringify!($type));
                            }
                        }
                    }

                    merge_joins.push(UnsafeCell::new(RangedColumnScanT::$variant(
                        RangedColumnScanCell::new(RangedColumnScanEnum::OrderedMergeJoin(
                            OrderedMergeJoin::new(scans),
                        )),
                    )))
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

impl<'a> TrieScan<'a> for TrieJoin<'a> {
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

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        debug_assert!(self.current_variable.is_some());

        match self.target_schema.get_type(self.current_variable?) {
            DataTypeName::U64 | DataTypeName::Float | DataTypeName::Double => {
                Some(&self.merge_joins[self.current_variable?])
            }
        }
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.merge_joins[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.target_schema
    }
}

#[cfg(test)]
mod test {
    use super::TrieJoin;
    use crate::physical::columns::{
        ColumnEnum, GenericIntervalColumn, IntervalColumnEnum, RangedColumnScanT,
    };
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tables::{
        IntervalTrieScan, Trie, TrieScan, TrieScanEnum, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn join_next(join_scan: &mut TrieJoin) -> Option<u64> {
        unsafe {
            if let RangedColumnScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.next()
            } else {
                panic!("type should be u64");
            }
        }
    }

    fn join_current(join_scan: &mut TrieJoin) -> Option<u64> {
        unsafe {
            if let RangedColumnScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
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

        let mut join_iter = TrieJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
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
        let join_iter_ab = TrieJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
            ],
            &vec![vec![0, 1], vec![1, 2]],
            schema_target_clone,
        );

        let mut join_iter_abc = TrieJoin::new(
            vec![
                TrieScanEnum::TrieJoin(join_iter_ab),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_c)),
            ],
            &vec![vec![0, 1, 2], vec![0, 1]],
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

    use crate::physical::columns::{
        AdaptiveColumnBuilder, ColumnBuilder, IntervalColumnT, VectorColumn,
    };

    #[test]
    fn test_dynamic() {
        let mut builder = AdaptiveColumnBuilder::<u64>::new();
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

        let built_interval_col = GenericIntervalColumn::<u64>::new(
            builder.finalize(),
            ColumnEnum::VectorColumn(VectorColumn::new(vec![0])),
        );

        let my_trie = Trie::new(
            schema,
            vec![IntervalColumnT::U64(
                IntervalColumnEnum::GenericIntervalColumn(built_interval_col),
            )],
        );

        let mut my_join_iter = TrieJoin::new(
            vec![TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(
                &my_trie,
            ))],
            &vec![vec![0]],
            schema_target,
        );

        my_join_iter.down();
        assert_eq!(join_next(&mut my_join_iter), Some(2));
        assert_eq!(join_next(&mut my_join_iter), Some(3));
        assert_eq!(join_next(&mut my_join_iter), Some(4));
        assert_eq!(join_next(&mut my_join_iter), None);
    }
}
