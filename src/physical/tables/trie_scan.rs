use super::Trie;
use crate::physical::columns::{IntervalColumnT, MaterialColumnScanT};
use std::fmt::Debug;

/// Iterator for the a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a MaterialColumnScanT object.
pub trait TrieScan: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a ranged [`ColumnScan`].
    fn current_scan(&mut self) -> Option<&mut MaterialColumnScanT>;
}

/// Implementation of TrieScan for Trie with IntervalColumns
#[derive(Debug)]
pub struct IntervalTrieScan<'a> {
    trie: &'a Trie,
    layers: Vec<MaterialColumnScanT<'a>>,
    current_layer: Option<usize>,
}

impl<'a> IntervalTrieScan<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let mut layers = Vec::<MaterialColumnScanT<'a>>::new();

        for column_t in trie.columns() {
            let new_scan = match column_t {
                IntervalColumnT::IntervalColumnU64(column) => {
                    MaterialColumnScanT::MaterialColumnScanU64(column.iter())
                }
                IntervalColumnT::IntervalColumnFloat(column) => {
                    MaterialColumnScanT::MaterialColumnScanFloat(column.iter())
                }
                IntervalColumnT::IntervalColumnDouble(column) => {
                    MaterialColumnScanT::MaterialColumnScanDouble(column.iter())
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
        match self.current_layer {
            None => self.current_layer = None,
            Some(index) => {
                if index == 0 {
                    self.current_layer = None;
                } else {
                    self.current_layer = Some(index - 1);
                }
            }
        };
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

                println!(
                    "Next_layer_range: {}, {}",
                    next_layer_range.start, next_layer_range.end
                );

                self.layers[next_index].narrow(next_layer_range);

                self.current_layer = Some(next_index);
            }
        }
    }

    fn current_scan(&mut self) -> Option<&'a mut MaterialColumnScanT> {
        Some(&mut self.layers[self.current_layer?])
    }
}

#[cfg(test)]
mod test {
    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::{IntervalTrieScan, TrieScan};
    use crate::physical::columns::{
        GenericIntervalColumn, IntervalColumnT, MaterialColumnScanT, VectorColumn,
    };
    use crate::physical::datatypes::DataTypeName;
    use test_log::test;

    fn make_gic(values: &[u64], ints: &[usize]) -> GenericIntervalColumn<u64> {
        GenericIntervalColumn::new(
            Box::new(VectorColumn::new(values.to_vec())),
            Box::new(VectorColumn::new(ints.to_vec())),
        )
    }

    fn make_gict(values: &[u64], ints: &[usize]) -> IntervalColumnT {
        IntervalColumnT::IntervalColumnU64(Box::new(make_gic(values, ints)))
    }

    fn seek_scan(scan: &mut MaterialColumnScanT, value: u64) {
        if let MaterialColumnScanT::MaterialColumnScanU64(column) = scan {
            column.seek(value);
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
