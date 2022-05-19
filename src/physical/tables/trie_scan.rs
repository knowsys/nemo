use super::{Table, TableSchema, Trie, TrieSchema};
use crate::physical::columns::{
    IntervalColumnT, OrderedMergeJoin, RangedColumnScan, RangedColumnScanT,
};
use crate::physical::datatypes::DataTypeName;
use std::fmt::Debug;

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a RangedColumnScanT object.
pub trait TrieScan<'a>: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a ranged [`ColumnScan`].
    fn current_scan(&mut self) -> Option<*mut RangedColumnScanT<'a>>;

    /// Return the underlying [`TrieSchema`].
    fn get_schema(&self) -> &dyn TableSchema;
}

/// Implementation of TrieScan for Trie with IntervalColumns
#[derive(Debug)]
pub struct IntervalTrieScan<'a> {
    trie: &'a Trie,
    layers: Vec<RangedColumnScanT<'a>>,
    current_layer: Option<usize>,
}

impl<'a> IntervalTrieScan<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let mut layers = Vec::<RangedColumnScanT<'a>>::new();

        for column_t in trie.columns() {
            let new_scan = match column_t {
                IntervalColumnT::IntervalColumnU64(column) => {
                    RangedColumnScanT::RangedColumnScanU64(column.iter())
                }
                IntervalColumnT::IntervalColumnFloat(column) => {
                    RangedColumnScanT::RangedColumnScanFloat(column.iter())
                }
                IntervalColumnT::IntervalColumnDouble(column) => {
                    RangedColumnScanT::RangedColumnScanDouble(column.iter())
                }
            };

            layers.push(new_scan);
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

                let current_position = self.layers[index].pos().unwrap();

                let next_index = index + 1;
                let next_layer_range = self
                    .trie
                    .get_column(next_index)
                    .int_bounds(current_position);

                self.layers[next_index].narrow(next_layer_range);

                self.current_layer = Some(next_index);
            }
        }
    }

    fn current_scan(&mut self) -> Option<*mut RangedColumnScanT<'a>> {
        Some(&mut self.layers[self.current_layer?])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie.schema()
    }
}

/// Function computing trie join on concrete tries
/// Probably useless
/*
pub fn material_join(
    scans: &mut Vec<&mut IntervalTrieScan>,
    target_schema: &TrieSchema,
    current_variable_index: Option<usize>,
) {
    let current_variable_index = current_variable_index.map_or(0, |v| v + 1);
    let current_label = target_schema.get_label(current_variable_index);

    let mut current_trie_scans: Vec<usize> = vec![];

    for index in 0..scans.len() {
        if scans[index]
            .get_schema()
            .find_index(current_label)
            .is_some()
        {
            current_trie_scans.push(index);
            scans[index].down();
        }
    }

    match target_schema.get_type(current_variable_index) {
        DataTypeName::U64 => {
            let mut current_column_scans: Vec<&mut dyn RangedColumnScan<Item = u64>> = vec![];
            for index in current_trie_scans {
                current_column_scans.push(
                    scans[index]
                        .current_scan()
                        .unwrap()
                        .to_colum_scan_u64()
                        .unwrap(),
                );
            }
        }
        DataTypeName::Float => {}
        DataTypeName::Double => {}
    }

    material_join(scans, target_schema, Some(current_variable_index));
}
*/

/// Structure resulting from joining a set of tries (given as TrieJoins),
/// which itself is a TrieJoin that can be used in such a join
#[derive(Debug)]
pub struct TrieScanJoin<'a> {
    trie_scans: Vec<Box<dyn TrieScan<'a> + 'a>>,
    target_schema: TrieSchema,

    current_variable: Option<usize>,

    variable_to_scan: Vec<Vec<usize>>,

    //Will replace variable_to_scan
    merge_joins: Vec<RangedColumnScanT<'a>>,

    column_scan_cache: Option<RangedColumnScanT<'a>>,
}

impl<'a> TrieScanJoin<'a> {
    /// Construct new TrieScanJoin object.
    pub fn new(trie_scans: Vec<Box<dyn TrieScan<'a> + 'a>>, target_schema: TrieSchema) -> Self {
        let mut variable_to_scan: Vec<Vec<usize>> = vec![];
        variable_to_scan.resize(target_schema.arity(), vec![]);
        let mut merge_joins: Vec<RangedColumnScanT<'a>>;

        for scan_index in 0..trie_scans.len() {
            let current_schema = trie_scans[scan_index].get_schema();
            for entry_index in 0..current_schema.arity() {
                variable_to_scan[current_schema.get_label(entry_index)].push(scan_index);
            }
        }

        Self {
            trie_scans,
            target_schema,
            current_variable: None,
            variable_to_scan,
            merge_joins: vec![],
            column_scan_cache: None,
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
    }

    fn current_scan(&mut self) -> Option<*mut RangedColumnScanT<'a>> {
        debug_assert!(self.current_variable.is_some());

        match self.target_schema.get_type(self.current_variable?) {
            DataTypeName::U64 => {
                if self.variable_to_scan[self.current_variable.unwrap()].len() == 1 {
                    let scan_index = self.variable_to_scan[self.current_variable.unwrap()][0];

                    return self.trie_scans[scan_index].current_scan();
                }

                let mut rest = self.trie_scans.as_mut_slice();

                let mut trie_scans: Vec<&mut Box<dyn TrieScan>> = vec![];

                // TODO: make sure that scan indices are sorted; debug_assert probably
                for _ in &self.variable_to_scan[self.current_variable.unwrap()] {
                    // let (_left, middle) = rest.split_at_mut(*scan_index);
                    // let (target, right) = middle.split_at_mut(1);

                    // rest = right;
                    let (_left, middle) = rest.split_at_mut(0);
                    let (target, right) = middle.split_at_mut(1);

                    rest = right;
                    trie_scans.push(target.get_mut(0).unwrap());
                }

                let mut column_scans = Vec::<&mut dyn RangedColumnScan<Item = u64>>::new();

                unsafe {
                    for ts in trie_scans {
                        column_scans.push((*ts.current_scan()?).to_colum_scan_u64().unwrap());
                    }
                }
                let omj = OrderedMergeJoin::new(column_scans);

                let rcst = RangedColumnScanT::RangedColumnScanU64(Box::new(omj));

                self.column_scan_cache = Some(rcst);

                Some(self.column_scan_cache.as_mut().unwrap())
            }
            DataTypeName::Float => None,
            DataTypeName::Double => None,
        }
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.target_schema
    }
}

#[cfg(test)]
mod test {
    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::{IntervalTrieScan, TrieScan, TrieScanJoin};
    use crate::physical::columns::RangedColumnScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn seek_scan(scan: &mut RangedColumnScanT, value: u64) {
        scan.to_colum_scan_u64().unwrap().seek(value);
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
        let mut scan;

        unsafe {
            scan = &mut (*trie_iter.current_scan().unwrap());
            seek_scan(scan, 1);
            assert_eq!(scan.pos(), Some(0));
        }

        trie_iter.down();
        unsafe {
            scan = &mut (*trie_iter.current_scan().unwrap());
            seek_scan(scan, 2);
            assert_eq!(scan.pos(), Some(0));
        }

        //TODO: Further tests this once GenericColumnScan is fixed
    }

    fn join_next(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            (*join_scan.current_scan()?)
                .to_colum_scan_u64()
                .unwrap()
                .next()
        }
    }

    fn join_current(join_scan: &mut TrieScanJoin) -> Option<u64> {
        unsafe {
            (*join_scan.current_scan()?)
                .to_colum_scan_u64()
                .unwrap()
                .current()
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
        let schema_a_clone = schema_a.clone();

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
        let schema_b_clone = schema_b.clone();

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
                Box::new(IntervalTrieScan::new(&trie_a)),
                Box::new(IntervalTrieScan::new(&trie_b)),
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
                Box::new(IntervalTrieScan::new(&trie_a)),
                Box::new(IntervalTrieScan::new(&trie_b)),
            ],
            schema_target_clone,
        );

        let mut join_iter_abc = TrieScanJoin::new(
            vec![
                Box::new(join_iter_ab),
                Box::new(IntervalTrieScan::new(&trie_c)),
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

        let built_interval_col = GenericIntervalColumn::<u64>::new(
            builder.finalize(),
            Box::new(VectorColumn::new(vec![0])),
        );

        let my_trie = Trie::new(
            schema,
            vec![IntervalColumnT::IntervalColumnU64(Box::new(
                built_interval_col,
            ))],
        );

        let mut my_join_iter = TrieScanJoin::new(
            vec![Box::new(IntervalTrieScan::new(&my_trie))],
            schema_target,
        );

        my_join_iter.down();
        assert_eq!(join_next(&mut my_join_iter), Some(2));
        assert_eq!(join_next(&mut my_join_iter), Some(3));
        assert_eq!(join_next(&mut my_join_iter), Some(4));
        assert_eq!(join_next(&mut my_join_iter), None);
    }
}
