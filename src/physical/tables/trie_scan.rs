use super::{Table, TableSchema, Trie, TrieSchema};
use crate::physical::columns::{ColumnScan, IntervalColumnT, RangedColumnScan, RangedColumnScanT};
use crate::physical::datatypes::DataTypeName;
use std::fmt::Debug;

/// Iterator for the a Trie datastructure.to_colum_scan_u64
/// Allows for vertical traversal through the tree and can return
/// its current position as a RangedColumnScanT object.
pub trait TrieScan: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a ranged [`ColumnScan`].
    fn current_scan(&mut self) -> Option<&mut RangedColumnScanT>;

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

impl<'a> TrieScan for IntervalTrieScan<'a> {
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

    fn current_scan(&mut self) -> Option<&'a mut RangedColumnScanT> {
        Some(&mut self.layers[self.current_layer?])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie.schema()
    }
}

/// Function computing trie join on concrete tries
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

/// Structure resulting from joining a set of tries (given as TrieJoins),
/// which itself is a TrieJoin that can be used in such a join
#[derive(Debug)]
pub struct TrieScanJoin {
    trie_scans: Vec<Box<dyn TrieScan>>,
    schema: TrieSchema,

    current_variable: Option<usize>,

    variable_to_scan: Vec<Vec<usize>>,
}

impl TrieScanJoin {
    /// Construct new TrieScanJoin object.
    pub fn new(trie_scans: Vec<Box<dyn TrieScan>>, schema: TrieSchema) -> Self {
        let mut variable_to_scan: Vec<Vec<usize>> = vec![];
        variable_to_scan.resize(schema.arity(), vec![]);
        for scan_index in 0..trie_scans.len() {
            let current_schema = trie_scans[scan_index].get_schema();
            for entry_index in 0..current_schema.arity() {
                variable_to_scan[current_schema.get_label(entry_index)].push(scan_index);
            }
        }

        Self {
            trie_scans,
            schema,
            current_variable: None,
            variable_to_scan,
        }
    }
}

impl TrieScan for TrieScanJoin {
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

        debug_assert!(current_variable < self.schema.arity());

        let current_scans = &self.variable_to_scan[current_variable];

        for &scan_index in current_scans {
            self.trie_scans[scan_index].down();
        }
    }

    fn current_scan(&mut self) -> Option<&mut RangedColumnScanT> {
        None
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::{IntervalTrieScan, TrieScan};
    use crate::physical::columns::RangedColumnScanT;
    use crate::physical::datatypes::{DataTypeName, DataValueT};
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn seek_scan(scan: &mut RangedColumnScanT, value: u64) {
        scan.seek(DataValueT::U64(value));
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
        let mut scan = trie_iter.current_scan().unwrap();
        seek_scan(scan, 1);
        assert_eq!(scan.pos(), Some(0));

        trie_iter.down();
        scan = trie_iter.current_scan().unwrap();
        seek_scan(scan, 2);
        assert_eq!(scan.pos(), Some(0));

        //TODO: Further tests this once GenericColumnScan is fixed
    }
}
