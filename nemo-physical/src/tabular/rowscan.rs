//! This module defines [RowScan].

use std::marker::PhantomData;

use streaming_iterator::StreamingIterator;

use crate::datatypes::{StorageTypeName, StorageValueT};

use super::triescan::PartialTrieScan;

/// A [StreamingIterator] for a [PartialTrieScan]
#[derive(Debug)]
pub struct RowScan<'a, Scan: PartialTrieScan<'a>> {
    /// Using 'a in the trait bound doesn't count
    _phantom: PhantomData<&'a usize>,

    /// [PartialTrieScan] whose rows will be enumerated
    trie_scan: Scan,

    /// Whether it can be known a priori that this will return no rows
    empty: bool,
    /// For each layer,
    /// holds the possible [StorageTypeName] of that column in `trie_scan`
    possible_types: Vec<Vec<StorageTypeName>>,

    /// For each layer, an index into the inner list of `possible_types`
    current_types: Vec<usize>,
    /// A buffer containing the current row
    current_row: Vec<StorageValueT>,

    /// The highest layer that has been changed by the most recent call to `next`
    changed_layers: usize,
}

impl<'a, Scan: PartialTrieScan<'a>> RowScan<'a, Scan> {
    /// Create a new [RowScan].
    pub fn new(trie_scan: Scan, cut: usize) -> Self {
        let arity = trie_scan.arity();
        let used_columns = arity - cut;

        let possible_types = (0..arity)
            .map(|layer| trie_scan.possible_types(layer).storage_types())
            .collect::<Vec<_>>();

        let empty = arity == 0 || possible_types.iter().any(|types| types.is_empty());

        Self {
            _phantom: PhantomData::default(),
            trie_scan,
            empty,
            possible_types,
            current_types: vec![0; arity],
            current_row: vec![StorageValueT::Id32(0); used_columns],
            changed_layers: 0,
        }
    }

    /// Advance the column scan of the current layer for the given [StorageTypeName]
    /// to the next value.
    ///
    /// Assumes that `self.trie_scan` is at some layer.
    fn column_scan_next(&mut self, storage_type: StorageTypeName) -> Option<StorageValueT> {
        unsafe {
            &mut *self
                .trie_scan
                .current_scan()
                .expect("This function assumes that trie_scan is at some layer")
                .get()
        }
        .next(storage_type)
    }
}

impl<'a, Scan: PartialTrieScan<'a>> StreamingIterator for RowScan<'a, Scan> {
    type Item = [StorageValueT];

    fn advance(&mut self) {
        if self.empty {
            return;
        }

        if self.trie_scan.current_layer().is_none() {
            let first_type = self.possible_types[0][0];
            self.trie_scan.down(first_type);
        };

        self.changed_layers = self.trie_scan.arity() - 1;

        while let Some(current_layer) = self.trie_scan.current_layer() {
            let is_last_layer = current_layer == self.trie_scan.arity() - 1;
            let is_used_layer = current_layer < self.current_row.len();

            let current_type_index = self.current_types[current_layer];
            let current_type = self.possible_types[current_layer][current_type_index];

            if current_layer < self.changed_layers {
                self.changed_layers = current_layer;
            }

            if let Some(next_value) = self.column_scan_next(current_type) {
                if is_used_layer {
                    self.current_row[current_layer] = next_value;
                }

                if is_last_layer {
                    return;
                } else {
                    let next_layer = current_layer + 1;
                    let next_layer_first_type = self.possible_types[next_layer][0];

                    self.trie_scan.down(next_layer_first_type);
                }
            } else {
                let next_type_index = &mut self.current_types[current_layer];
                *next_type_index += 1;

                self.trie_scan.up();

                if let Some(next_type) = self.possible_types[current_layer].get(*next_type_index) {
                    self.trie_scan.down(*next_type);
                } else {
                    self.current_types[current_layer] = 0;
                }
            }
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.trie_scan.current_layer().is_some() {
            Some(&self.current_row[self.changed_layers..])
        } else {
            None
        }
    }
}

impl<'a, Scan: PartialTrieScan<'a>> Iterator for RowScan<'a, Scan> {
    type Item = Vec<StorageValueT>;

    fn next(&mut self) -> Option<Self::Item> {
        StreamingIterator::advance(self);

        if self.trie_scan.current_layer().is_some() {
            Some(self.current_row.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::StorageValueT,
        management::database::Dict,
        tabular::{
            operations::{union::GeneratorUnion, OperationGenerator},
            trie::Trie,
            triescan::TrieScanEnum,
        },
    };

    use super::RowScan;

    #[test]
    fn rowscan_basic() {
        let dictionary = RefCell::new(Dict::default());

        let trie_a = Trie::from_rows(vec![
            vec![StorageValueT::Id32(0), StorageValueT::Int64(10)],
            vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
            vec![StorageValueT::Int64(2), StorageValueT::Int64(4)],
        ]);

        let trie_b = Trie::from_rows(vec![
            vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
            vec![StorageValueT::Id32(2), StorageValueT::Int64(-5)],
            vec![StorageValueT::Int64(2), StorageValueT::Id32(3)],
        ]);

        let trie_scan_a = TrieScanEnum::TrieScanGeneric(trie_a.partial_iterator());
        let trie_scan_b = TrieScanEnum::TrieScanGeneric(trie_b.partial_iterator());

        let union_generator = GeneratorUnion::new();
        let trie_scan = union_generator
            .generate(vec![Some(trie_scan_a), Some(trie_scan_b)], &dictionary)
            .unwrap();

        let expected = vec![
            vec![StorageValueT::Id32(0), StorageValueT::Int64(10)],
            vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
            vec![StorageValueT::Id32(2), StorageValueT::Int64(-5)],
            vec![StorageValueT::Int64(2), StorageValueT::Id32(3)],
            vec![StorageValueT::Int64(2), StorageValueT::Int64(4)],
        ];

        let row_scan = RowScan::new(trie_scan, 0);
        let result = row_scan.collect::<Vec<_>>();

        assert_eq!(result, expected);
    }
}
