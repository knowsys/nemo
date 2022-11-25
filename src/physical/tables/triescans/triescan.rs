use crate::physical::columns::colscans::{ColScan, ColScanT};
use crate::physical::columns::columns::IntervalColumn;
use crate::physical::tables::tables::{Table, TableSchema};
use crate::physical::tables::tries::Trie;
use crate::{generate_forwarder, physical::columns::columns::Column};
use std::cell::UnsafeCell;
use std::fmt::Debug;

use super::{TrieDifference, TrieJoin, TrieProject, TrieSelectEqual, TrieSelectValue, TrieUnion};

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a [`ColScanT`] object.
pub trait TrieScan<'a>: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a [`ColScanT`].
    fn current_scan(&self) -> Option<&UnsafeCell<ColScanT<'a>>>;

    /// Return the underlying [`ColScanT`] object given an index.
    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColScanT<'a>>>;

    /// Return the underlying [`TableSchema`].
    fn get_schema(&self) -> &dyn TableSchema;
}

/// Implementation of TrieScan for Trie with IntervalColumns
#[derive(Debug)]
pub struct IntervalTrieScan<'a> {
    trie: &'a Trie,
    layers: Vec<UnsafeCell<ColScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> IntervalTrieScan<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let mut layers = Vec::<UnsafeCell<ColScanT<'a>>>::new();

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
        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));
    }

    fn down(&mut self) {
        match self.current_layer {
            None => {
                self.layers[0].get_mut().reset();
                self.current_layer = Some(0);
            }
            Some(index) => {
                debug_assert!(
                    index < self.layers.len(),
                    "Called down while on the last layer"
                );

                let current_position = self.layers[index]
                    .get_mut()
                    .pos()
                    .expect("Going down is only allowed when on an element.");

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

    fn current_scan(&self) -> Option<&UnsafeCell<ColScanT<'a>>> {
        Some(&self.layers[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColScanT<'a>>> {
        Some(&self.layers[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        self.trie.schema()
    }
}

/// Enum for TrieScan Variants
#[derive(Debug)]
pub enum TrieScanEnum<'a> {
    /// Case IntervalTrieScan
    IntervalTrieScan(IntervalTrieScan<'a>),
    /// Case TrieJoin
    TrieJoin(TrieJoin<'a>),
    /// Case TrieProject
    TrieProject(TrieProject<'a>),
    /// Case TrieDifference
    TrieDifference(TrieDifference<'a>),
    /// Case TrieUnion
    TrieUnion(TrieUnion<'a>),
    /// Case TrieSelectEqual
    TrieSelectEqual(TrieSelectEqual<'a>),
    /// Case TrieSelectValue
    TrieSelectValue(TrieSelectValue<'a>),
}

generate_forwarder!(forward_to_scan; IntervalTrieScan, TrieJoin, TrieProject, TrieDifference, TrieSelectEqual, TrieSelectValue, TrieUnion);

impl<'a> TrieScan<'a> for TrieScanEnum<'a> {
    fn up(&mut self) {
        forward_to_scan!(self, up)
    }

    fn down(&mut self) {
        forward_to_scan!(self, down)
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColScanT<'a>>> {
        forward_to_scan!(self, current_scan)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColScanT<'a>>> {
        forward_to_scan!(self, get_scan(index))
    }

    fn get_schema(&self) -> &dyn TableSchema {
        forward_to_scan!(self, get_schema)
    }
}

#[cfg(test)]
mod test {
    use super::{IntervalTrieScan, TrieScan};
    use crate::physical::columns::colscans::ColScanT;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tables::tries::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    fn scan_seek(int_scan: &mut IntervalTrieScan, value: u64) -> Option<u64> {
        if let ColScanT::U64(rcs) = unsafe { &(*int_scan.current_scan()?.get()) } {
            rcs.seek(value)
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_next(int_scan: &mut IntervalTrieScan) -> Option<u64> {
        if let ColScanT::U64(rcs) = unsafe { &(*int_scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut IntervalTrieScan) -> Option<u64> {
        unsafe {
            if let ColScanT::U64(rcs) = &(*int_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_trie_iter() {
        let column_fst = make_gict(&[1, 2, 3], &[0]);
        let column_snd = make_gict(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_gict(&[3, 4, 5, 7, 8, 7, 2, 1], &[0, 2, 5, 6, 7]);

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

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 3), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 6), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));
        assert!(scan_next(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 2), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());
    }
}
