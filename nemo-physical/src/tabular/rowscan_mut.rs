//! This module defines [RowScanMut].

use std::marker::PhantomData;

use streaming_iterator::StreamingIterator;

use crate::datatypes::{StorageTypeName, StorageValueT};

use super::{
    trie::{Trie, TrieScanGenericMut},
    triescan::PartialTrieScan,
};

#[derive(Debug)]
struct TypeIndex {
    start: usize,
    used: usize,
}

/// Stores the possible [StorageTypeName] for each layer,
/// and which of those are currently in use
#[derive(Debug)]
struct PossibleTypes {
    /// All possible storage types
    storage_types: Vec<StorageTypeName>,
    /// For each layer, contains a [TypeIndex]
    used_types: Vec<TypeIndex>,
}

impl PossibleTypes {
    pub fn new(input_types: Vec<Vec<StorageTypeName>>) -> Self {
        let mut storage_types = Vec::<StorageTypeName>::new();
        let mut used_types = Vec::<TypeIndex>::new();

        for types_layer in input_types {
            let new_index = TypeIndex {
                start: storage_types.len(),
                used: storage_types.len(),
            };

            used_types.push(new_index);

            if types_layer.is_empty() {
                return Self {
                    storage_types: Vec::new(),
                    used_types: Vec::new(),
                };
            }

            for storage_type in types_layer {
                storage_types.push(storage_type);
            }
        }

        used_types.push(TypeIndex {
            start: storage_types.len(),
            used: storage_types.len(),
        });

        Self {
            storage_types,
            used_types,
        }
    }

    pub fn next_type(&mut self, layer: usize) -> Option<StorageTypeName> {
        let next_index = self.used_types[layer].used + 1;

        if next_index == self.used_types[layer + 1].start {
            self.used_types[layer].used = self.used_types[layer].start;
            None
        } else {
            self.used_types[layer].used = next_index;
            Some(self.storage_types[next_index])
        }
    }

    pub fn first_type(&mut self, layer: usize) -> StorageTypeName {
        let start = self.used_types[layer].start;

        self.used_types[layer].used = start;
        self.storage_types[start]
    }

    pub fn current_type(&self, layer: usize) -> StorageTypeName {
        self.storage_types[self.used_types[layer].used]
    }
}

/// A row returned by [RowScanMut].
/// Additionally also contains the first row index which differes from the last call to `next`.
///
/// TODO: It would be nice if the next operation could return `(usize, &[StorageValueT])` instead of &Row
#[derive(Debug)]
pub(crate) struct Row {
    /// Row as a vector of [StorageValueT]
    pub row: Vec<StorageValueT>,
    /// First index of the row that differs from the last call to `next`
    pub change: usize,
}

/// A [StreamingIterator] for a [PartialTrieScan]
#[derive(Debug)]
pub(crate) struct RowScanMut<'a> {
    /// [Trie] whose rows will be iterated
    trie_scan: TrieScanGenericMut<'a>,

    /// Whether it can be known a priori that this will return no rows
    empty: bool,
    /// For each layer, holds the possible [StorageTypeName]s of that column in `trie_scan`
    possible_types: PossibleTypes,

    /// The current row
    /// and the first column index that has been changed by the most recent call to `next`
    current_row: Row,
}

impl<'a> RowScanMut<'a> {
    /// Create a new [RowScanMut].
    pub(crate) fn new(trie_scan: TrieScanGenericMut<'a>, cut: usize) -> Self {
        let arity = trie_scan.arity();
        let used_columns = arity - cut;

        let possible_types = (0..arity)
            .map(|layer| trie_scan.possible_types(layer).storage_types())
            .collect::<Vec<_>>();

        let empty = arity == 0 || possible_types.iter().any(|types| types.is_empty());

        Self {
            trie_scan,
            empty,
            possible_types: PossibleTypes::new(possible_types),
            current_row: Row {
                row: vec![StorageValueT::Id32(0); used_columns],
                change: 0,
            },
        }
    }

    pub(crate) fn delete_current_row(&mut self) {
        let last_type = self.current_row.row.last().expect("").get_type();
        let last_index = self.current_row.row.len() - 1;

        unsafe { &mut *self.trie_scan.scan(last_index).get() }.set_deleted(last_type);
    }

    /// Advance the column scan of the current layer for the given [StorageTypeName]
    /// to the next value.
    ///
    /// # Panics
    /// Panics if `self.trie_scan` is not at some layer.
    fn column_scan_next(&mut self, storage_type: StorageTypeName) -> Option<StorageValueT> {
        let iter = unsafe {
            &mut *self
                .trie_scan
                .current_scan()
                .expect("This function assumes that trie_scan is at some layer")
                .get()
        };
        iter.next(storage_type);
        iter.current(storage_type)
    }

    /// Return the current value of the of the trie on a given layer for a given storage type.
    ///
    /// # Panics
    /// Panics if there is no value at the given loaction
    fn column_scan_current(&self, layer: usize, storage_type: StorageTypeName) -> StorageValueT {
        unsafe { &mut *self.trie_scan.scan(layer).get() }
            .current(storage_type)
            .expect("Function assumes that columnscan points to some value")
    }
}

impl<'a> StreamingIterator for RowScanMut<'a> {
    type Item = Row;

    fn advance(&mut self) {
        if self.empty {
            return;
        }

        if self.trie_scan.current_layer().is_none() {
            let first_type = self.possible_types.first_type(0);
            self.trie_scan.down(first_type);
        };

        self.current_row.change = self.current_row.row.len() - 1;

        while let Some(current_layer) = self.trie_scan.current_layer() {
            let is_last_layer = current_layer == self.trie_scan.arity() - 1;
            let current_type = self.possible_types.current_type(current_layer);

            if current_layer < self.current_row.change {
                self.current_row.change = current_layer;
            }

            if let Some(_next_value) = self.column_scan_next(current_type) {
                if is_last_layer {
                    for layer in 0..self.current_row.row.len() {
                        let layer_type = self.possible_types.current_type(layer);
                        let value = self.column_scan_current(layer, layer_type);
                        self.current_row.row[layer] = value;
                    }

                    for _ in self.current_row.row.len()..=current_layer {
                        self.trie_scan.up();
                    }

                    return;
                } else {
                    let next_layer = current_layer + 1;
                    let next_layer_first_type = self.possible_types.first_type(next_layer);

                    self.trie_scan.down(next_layer_first_type);
                }
            } else {
                self.trie_scan.up();

                if let Some(next_type) = self.possible_types.next_type(current_layer) {
                    self.trie_scan.down(next_type);
                }
            }
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.trie_scan.current_layer().is_some() {
            Some(&self.current_row)
        } else {
            None
        }
    }
}

impl<'a> Iterator for RowScanMut<'a> {
    type Item = Vec<StorageValueT>;

    fn next(&mut self) -> Option<Self::Item> {
        StreamingIterator::advance(self);

        if self.trie_scan.current_layer().is_some() {
            Some(self.current_row.row.clone())
        } else {
            None
        }
    }
}

// #[cfg(test)]
// mod test {
//     use std::cell::RefCell;

//     use crate::{
//         datatypes::StorageValueT,
//         management::database::Dict,
//         tabular::{
//             operations::{union::GeneratorUnion, OperationGenerator},
//             trie::Trie,
//             triescan::TrieScanEnum,
//         },
//     };

//     use super::RowScanMut;

//     #[test]
//     fn RowScanMut_basic() {
//         let dictionary = RefCell::new(Dict::default());

//         let trie_a = Trie::from_rows(vec![
//             vec![StorageValueT::Id32(0), StorageValueT::Int64(10)],
//             vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
//             vec![StorageValueT::Int64(2), StorageValueT::Int64(4)],
//         ]);

//         let trie_b = Trie::from_rows(vec![
//             vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
//             vec![StorageValueT::Id32(2), StorageValueT::Int64(-5)],
//             vec![StorageValueT::Int64(2), StorageValueT::Id32(3)],
//         ]);

//         let trie_scan_a = TrieScanEnum::Generic(trie_a.partial_iterator());
//         let trie_scan_b = TrieScanEnum::Generic(trie_b.partial_iterator());

//         let union_generator = GeneratorUnion::new();
//         let trie_scan = union_generator
//             .generate(vec![Some(trie_scan_a), Some(trie_scan_b)], &dictionary)
//             .unwrap();

//         let expected = vec![
//             vec![StorageValueT::Id32(0), StorageValueT::Int64(10)],
//             vec![StorageValueT::Id32(1), StorageValueT::Int64(-10)],
//             vec![StorageValueT::Id32(2), StorageValueT::Int64(-5)],
//             vec![StorageValueT::Int64(2), StorageValueT::Id32(3)],
//             vec![StorageValueT::Int64(2), StorageValueT::Int64(4)],
//         ];

//         let row_scan = RowScanMut::new(trie_scan, 0);
//         let result = row_scan.collect::<Vec<_>>();

//         assert_eq!(result, expected);
//     }

//     #[test]
//     fn project_across_data_types() {
//         let trie = Trie::from_rows(vec![
//             vec![StorageValueT::Id32(0), StorageValueT::Id32(1)],
//             vec![StorageValueT::Int64(0), StorageValueT::Int64(42)],
//             vec![StorageValueT::Int64(1), StorageValueT::Int64(42)],
//             vec![StorageValueT::Int64(2), StorageValueT::Int64(42)],
//             vec![StorageValueT::Int64(3), StorageValueT::Int64(42)],
//             vec![StorageValueT::Int64(4), StorageValueT::Int64(42)],
//             vec![StorageValueT::Int64(5), StorageValueT::Int64(42)],
//         ]);

//         let project_1 = RowScanMut::new(trie.partial_iterator(), 1);

//         let expected = vec![
//             vec![StorageValueT::Id32(0)],
//             vec![StorageValueT::Int64(0)],
//             vec![StorageValueT::Int64(1)],
//             vec![StorageValueT::Int64(2)],
//             vec![StorageValueT::Int64(3)],
//             vec![StorageValueT::Int64(4)],
//             vec![StorageValueT::Int64(5)],
//         ];

//         let result: Vec<_> = project_1.collect();

//         assert_eq!(result, expected);
//     }
// }
