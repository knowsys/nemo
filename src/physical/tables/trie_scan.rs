use super::{Table, TableSchema, Trie, TrieSchema};
use crate::generate_forwarder;
use crate::physical::columns::{
    Column, ColumnScan, IntervalColumn, OrderedMergeJoin, RangedColumnScan, RangedColumnScanCell,
    RangedColumnScanEnum, RangedColumnScanT,
};
use crate::physical::datatypes::{DataTypeName, Double, Float};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::iter::repeat;

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a RangedColumnScanEnum object.
pub trait TrieScan<'a>: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a ranged [`ColumnScan`].
    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>>;

    /// Return the underlying [`RangedColumnScan`] object given an index.
    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>>;

    /// Return the underlying [`TrieSchema`].
    fn get_schema(&self) -> &dyn TableSchema;
}

/// Implementation of TrieScan for Trie with IntervalColumns
#[derive(Debug)]
pub struct IntervalTrieScan<'a> {
    trie: &'a Trie<'a>,
    layers: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> IntervalTrieScan<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let mut layers = Vec::<UnsafeCell<RangedColumnScanT<'a>>>::new();

        for column_t in trie.columns() {
            let new_scan = column_t.iter();
            layers.push(UnsafeCell::new(new_scan));
        }

        Self {
            trie,
            layers,
            current_layer: None,
        }
    }
}

impl<'a> TrieScan<'a> for IntervalTrieScan<'a> {
    fn up(&mut self) {
        self.current_layer = self
            .current_layer
            .and_then(|index| (index > 0).then(|| index - 1));
    }

    fn down(&mut self) {
        match self.current_layer {
            None => self.current_layer = Some(0),
            Some(index) => {
                debug_assert!(
                    index < self.layers.len(),
                    "Called down while on the last layer"
                );

                let current_position = self.layers[index].get_mut().pos().unwrap();

                let next_index = index + 1;
                let next_layer_range = self
                    .trie
                    .get_column(next_index)
                    .int_bounds(current_position);

                self.layers[next_index].get_mut().narrow(next_layer_range);

                self.current_layer = Some(next_index);
            }
        }
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.layers[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.layers[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie.schema()
    }
}

/// Structure resulting from joining a set of tries (given as TrieJoins),
/// which itself is a TrieJoin that can be used in such a join
#[derive(Debug)]
pub struct TrieScanJoin<'a> {
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

impl<'a> TrieScanJoin<'a> {
    /// Construct new TrieScanJoin object.
    pub fn new(trie_scans: Vec<TrieScanEnum<'a>>, target_schema: TrieSchema) -> Self {
        let mut variable_to_scan = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();
        let mut merge_join_indices: Vec<Vec<_>> = repeat(Vec::new())
            .take(target_schema.arity())
            .collect::<Vec<_>>();

        let mut merge_joins: Vec<UnsafeCell<RangedColumnScanT<'a>>> = Vec::new();

        for (scan_index, scan) in trie_scans.iter().enumerate() {
            let current_schema = scan.get_schema();
            for entry_index in 0..current_schema.arity() {
                variable_to_scan[current_schema.get_label(entry_index)].push(scan_index);
            }
        }

        for (target_label_index, merge_join_index) in merge_join_indices.iter_mut().enumerate() {
            for scan in &trie_scans {
                merge_join_index.push(
                    scan.get_schema()
                        .find_index(target_schema.get_label(target_label_index)),
                );
            }
        }

        for (variable_index, merge_join_index) in merge_join_indices.iter_mut().enumerate() {
            macro_rules! merge_join_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let mut scans = Vec::<&RangedColumnScanCell<$type>>::new();
                    for (scan_index, scan) in merge_join_index.iter().enumerate() {
                        match scan {
                            Some(label_index) => unsafe {
                                let column_scan =
                                    &*trie_scans[scan_index].get_scan(*label_index).unwrap().get();

                                if let RangedColumnScanT::$variant(cs) = column_scan {
                                    scans.push(cs);
                                } else {
                                    panic!("expected a column scan of type {}", stringify!($type));
                                }
                            },
                            None => {}
                        }
                    }

                    merge_joins.push(UnsafeCell::new(RangedColumnScanT::$variant(
                        RangedColumnScanCell::new(RangedColumnScanEnum::OrderedMergeJoin(
                            OrderedMergeJoin::new(scans),
                        )),
                    )))
                }};
            }

            match target_schema.get_type(variable_index) {
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

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        debug_assert!(self.current_variable.is_some());

        match self.target_schema.get_type(self.current_variable?) {
            DataTypeName::U64 => Some(&self.merge_joins[self.current_variable?]),
            DataTypeName::Float => None,
            DataTypeName::Double => None,
        }
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.merge_joins[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.target_schema
    }
}

/// Enum for TrieScan Variants
#[derive(Debug)]
pub enum TrieScanEnum<'a> {
    /// Case IntervalTrieScan
    IntervalTrieScan(IntervalTrieScan<'a>),
    /// Case TrieScanJoin
    TrieScanJoin(TrieScanJoin<'a>),
}

generate_forwarder!(forward_to_scan; IntervalTrieScan, TrieScanJoin);

impl<'a> TrieScan<'a> for TrieScanEnum<'a> {
    fn up(&mut self) {
        forward_to_scan!(self, up)
    }

    fn down(&mut self) {
        forward_to_scan!(self, down)
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        forward_to_scan!(self, current_scan)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        forward_to_scan!(self, get_scan(index))
    }

    fn get_schema(&self) -> &dyn TableSchema {
        forward_to_scan!(self, get_schema)
    }
}

#[cfg(test)]
mod test {
    use std::cell::UnsafeCell;

    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::{IntervalTrieScan, TrieScan, TrieScanEnum, TrieScanJoin};
    use crate::physical::columns::{
        ColumnEnum, GenericIntervalColumnEnum, IntervalColumnEnum, RangedColumnScan,
        RangedColumnScanT,
    };
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn seek_scan(scan: &UnsafeCell<RangedColumnScanT>, value: u64) -> Option<u64> {
        unsafe {
            if let RangedColumnScanT::U64(rcs) = &(*scan.get()) {
                rcs.seek(value)
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_trie_iter() {
        let column_fst = make_gict(&[1, 2, 3], &[0]);
        let column_snd = make_gict(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_gict(&[3, 4, 5, 7, 2, 1], &[0, 2, 3, 4, 5]);

        let column_vec = vec![column_fst, column_snd, column_trd];

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
        ]);

        let trie = Trie::new(schema, column_vec);
        let mut trie_iter = IntervalTrieScan::new(&trie);

        assert!(trie_iter.current_scan().is_none());

        trie_iter.up();
        assert!(trie_iter.current_scan().is_none());

        trie_iter.down();

        let scan = trie_iter.current_scan().unwrap();
        seek_scan(scan, 1);
        assert_eq!(unsafe { (*scan.get()).pos() }, Some(0));
        trie_iter.down();
        let scan = trie_iter.current_scan().unwrap();
        seek_scan(scan, 2);
        assert_eq!(unsafe { (*scan.get()).pos() }, Some(0));

        //TODO: Further tests this once GenericColumnScan is fixed
    }

    fn join_next(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            if let RangedColumnScanT::U64(rcs) = &(*join_scan.current_scan()?.get()) {
                rcs.next()
            } else {
                panic!("type should be u64");
            }
        }
    }

    fn _join_current(join_scan: &mut TrieScanJoin) -> Option<u64> {
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
        //TODO: Maybe schema schould just be copyable
        let schema_target_clone = schema_target.clone();
        let schema_target_clone_2 = schema_target.clone();

        let trie_a = Trie::new(schema_a, vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(schema_b, vec![column_b_y, column_b_z]);

        let mut join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
            ],
            schema_target,
        );

        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(1));
        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(2));
        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(8));
        assert_eq!(join_next(&mut join_iter), Some(9));
        assert_eq!(join_next(&mut join_iter), None);
        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(3));
        assert_eq!(join_next(&mut join_iter), None);
        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(2));
        join_iter.down();
        assert_eq!(join_next(&mut join_iter), None);
        join_iter.up();
        assert_eq!(join_next(&mut join_iter), Some(3));
        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(6));
        join_iter.down();
        assert_eq!(join_next(&mut join_iter), Some(11));
        assert_eq!(join_next(&mut join_iter), Some(12));
        assert_eq!(join_next(&mut join_iter), None);
        join_iter.up();
        assert_eq!(join_next(&mut join_iter), None);
        join_iter.up();
        assert_eq!(join_next(&mut join_iter), None);

        let column_c_x = make_gict(&[1, 2], &[0]);
        let column_c_y = make_gict(&[2, 8], &[0, 1]);

        let schema_c = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie_c = Trie::new(schema_c, vec![column_c_x, column_c_y]);
        let join_iter_ab = TrieScanJoin::new(
            vec![
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_a)),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_b)),
            ],
            schema_target_clone,
        );

        let mut join_iter_abc = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanJoin(join_iter_ab),
                TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(&trie_c)),
            ],
            schema_target_clone_2,
        );

        join_iter_abc.down();
        assert_eq!(join_next(&mut join_iter_abc), Some(1));
        join_iter_abc.down();
        assert_eq!(join_next(&mut join_iter_abc), Some(2));
        join_iter_abc.down();
        assert_eq!(join_next(&mut join_iter_abc), Some(8));
        assert_eq!(join_next(&mut join_iter_abc), Some(9));
        assert_eq!(join_next(&mut join_iter_abc), None);
        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), None);
        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), Some(2));
        join_iter_abc.down();
        assert_eq!(join_next(&mut join_iter_abc), None);
        join_iter_abc.up();
        assert_eq!(join_next(&mut join_iter_abc), None);
    }

    use crate::physical::columns::{
        AdaptiveColumnBuilder, ColumnBuilder, GenericIntervalColumn, IntervalColumnT, VectorColumn,
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

        let built_interval_col =
            GenericIntervalColumn::<u64, ColumnEnum<u64>, VectorColumn<usize>>::new(
                builder.finalize(),
                VectorColumn::new(vec![0]),
            );

        let my_trie = Trie::new(
            schema,
            vec![IntervalColumnT::U64(
                IntervalColumnEnum::GenericIntervalColumn(
                    GenericIntervalColumnEnum::ColumnEnumWithVecStarts(built_interval_col),
                ),
            )],
        );

        let mut my_join_iter = TrieScanJoin::new(
            vec![TrieScanEnum::IntervalTrieScan(IntervalTrieScan::new(
                &my_trie,
            ))],
            schema_target,
        );

        my_join_iter.down();
        assert_eq!(join_next(&mut my_join_iter), Some(2));
        assert_eq!(join_next(&mut my_join_iter), Some(3));
        assert_eq!(join_next(&mut my_join_iter), Some(4));
        assert_eq!(join_next(&mut my_join_iter), None);
    }
}
