//! This module defines [TrieScanTrim].

use crate::{
    columnar::columnscan::ColumnScanRainbow,
    datatypes::{StorageTypeName, StorageValueT},
    tabular::triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
};

///
#[derive(Debug)]
pub struct TrieScanTrim<'a> {
    trie_scan: TrieScanEnum<'a>,

    possible_types: Vec<Vec<StorageTypeName>>,
    type_indices: Vec<usize>,

    empty: bool,
}

impl<'a> TrieScanTrim<'a> {
    ///
    pub fn new(trie_scan: TrieScanEnum<'a>) -> Self {
        let arity = trie_scan.arity();

        let possible_types = (0..arity)
            .map(|layer| trie_scan.possible_types(layer).storage_types())
            .collect::<Vec<_>>();

        let empty = arity == 0 || possible_types.iter().any(|types| types.is_empty());

        Self {
            trie_scan,
            possible_types,
            type_indices: vec![0; arity],
            empty,
        }
    }

    fn column_scan(&mut self, layer: usize) -> &mut ColumnScanRainbow<'a> {
        unsafe { &mut *self.trie_scan.scan(layer).get() }
    }

    fn move_next(&mut self) -> bool {
        let current_layer = self
            .trie_scan
            .current_layer()
            .expect("Function assumes that trie is initialized");

        let mut current_type_index = self.type_indices[current_layer];
        let mut current_type = self.possible_types[current_layer][current_type_index];

        loop {
            if self.column_scan(current_layer).next(current_type).is_some() {
                self.type_indices[current_layer] = current_type_index;
                return true;
            } else {
                current_type_index += 1;

                if let Some(&next_type) = self.possible_types[current_layer].get(current_type_index)
                {
                    self.trie_scan.up();
                    self.trie_scan.down(next_type);

                    current_type = next_type;
                } else {
                    return false;
                }
            }
        }
    }

    fn move_up(&mut self) {
        let current_layer = self
            .trie_scan
            .current_layer()
            .expect("Cannot call up in the top layer");

        self.trie_scan.up();
        self.type_indices[current_layer] = 0;
    }

    fn move_down(&mut self) {
        let next_layer = self.trie_scan.current_layer().map_or(0, |layer| layer + 1);
        let first_type = self.possible_types[next_layer][0];

        self.trie_scan.down(first_type);
    }

    fn repair_state(&mut self) -> Option<usize> {
        let mut current_layer = self
            .trie_scan
            .current_layer()
            .expect("We expect the trie to be initialized");

        let mut highest_changed_layer = current_layer;

        loop {
            let is_last_layer = current_layer == self.trie_scan.arity() - 1;

            if self.move_next() {
                if is_last_layer {
                    return Some(highest_changed_layer);
                } else {
                    self.move_down();
                    current_layer += 1;
                }
            } else {
                if current_layer == 0 {
                    return None;
                }

                self.move_up();
                current_layer -= 1;

                if current_layer < highest_changed_layer {
                    highest_changed_layer = current_layer;
                }
            }
        }
    }
}

impl<'a> TrieScan for TrieScanTrim<'a> {
    fn num_columns(&self) -> usize {
        self.trie_scan.arity()
    }

    fn advance_on_layer(&mut self, target_layer: usize) -> Option<usize> {
        if self.empty {
            return None;
        }

        if self.trie_scan.current_layer().is_none() {
            self.move_down();
        } else {
            for _ in target_layer..(self.trie_scan.arity() - 1) {
                self.move_up();
            }
        }

        self.repair_state()
    }

    fn current_value(&mut self, layer: usize) -> StorageValueT {
        debug_assert!(!self.empty);

        let type_index = self.type_indices[layer];
        let storage_type = self.possible_types[layer][type_index];

        self.column_scan(layer)
            .current(storage_type)
            .expect("Function assumes that advance_on_layer has been called at least once")
    }
}

#[cfg(test)]
mod test {
    use crate::{
        tabular::{trie::Trie, triescan::TrieScanEnum},
        util::test_util::test::trie_int64,
    };

    use super::TrieScanTrim;

    #[test]
    fn triescan_trim_basic() {}
}
