use super::Trie;
use crate::physical::columns::{IntervalColumnT, RangedColumnScanT};
use std::fmt::Debug;

/// Iterator for the a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a RangedColumnScanT object.
pub trait TrieScan: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a ranged [`ColumnScan`].
    fn current_scan(&mut self) -> Option<&mut RangedColumnScanT>;
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
}

#[cfg(test)]
mod test {
    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::{IntervalTrieScan, TrieScan};
    use crate::physical::columns::RangedColumnScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn seek_scan(scan: &mut RangedColumnScanT, value: u64) {
        if let RangedColumnScanT::RangedColumnScanU64(column) = scan {
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
