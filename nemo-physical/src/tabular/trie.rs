//! This module implements [Trie]
//! as well as its iterator [TrieScanGeneric].

use std::cell::UnsafeCell;

use crate::{
    columnar::{
        columnscan::ColumnScanRainbow,
        intervalcolumn::{
            interval_lookup::lookup_column_single::IntervalLookupColumnSingle, IntervalColumnT,
            IntervalColumnTBuilder,
        },
    },
    datasources::SortedTupleBuffer,
    datatypes::StorageTypeName,
};

use super::triescan::PartialTrieScan;

/// Defines the lookup method used in [IntervalColumnT]
type IntervalLookupMethod = IntervalLookupColumnSingle;

/// A tree like data structure for storing tabular data
///
/// A path in the tree from root to leaf corresponds
/// to a row in the represented table.
#[derive(Debug, Clone)]
pub struct Trie {
    /// Each [IntervalColumnT] represents one column in the table.
    /// We refer to each such column as a layer.
    columns: Vec<IntervalColumnT<IntervalLookupMethod>>,
}

impl Trie {
    /// Return the arity, that is the number of columns, in this trie.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Return a [PartialTrieScan] over this trie.
    pub fn iter(&self) -> TrieScanGeneric<'_> {
        let column_scans = self
            .columns
            .iter()
            .map(|column| UnsafeCell::new(column.iter()))
            .collect::<Vec<_>>();

        TrieScanGeneric::new(self, column_scans)
    }
}

impl Trie {
    /// Create a new [Trie] from a [SortedTupleBuffer].
    pub fn from_tuple_buffer(buffer: SortedTupleBuffer) -> Self {
        let mut intervalcolumn_builders = (0..buffer.column_number())
            .map(|_| IntervalColumnTBuilder::<IntervalLookupMethod>::default())
            .collect::<Vec<_>>();

        let mut last_tuple_intervals = vec![buffer.size()];

        for column_index in 0..buffer.column_number() {
            let current_builder = &mut intervalcolumn_builders[column_index];
            let mut current_tuple_intervals = Vec::<usize>::new();

            let mut predecessor_index = 0;
            for (tuple_index, value) in buffer.get_column(column_index).enumerate() {
                if tuple_index == last_tuple_intervals[predecessor_index] {
                    current_builder.finish_interval();
                    predecessor_index += 1;
                }

                let new_value = current_builder.add_value(value);

                if new_value && tuple_index > 0 {
                    current_tuple_intervals.push(tuple_index);
                }
            }

            current_builder.finish_interval();

            last_tuple_intervals = current_tuple_intervals;
        }

        Self {
            columns: intervalcolumn_builders
                .into_iter()
                .map(|builder| builder.finalize())
                .collect(),
        }
    }
}

/// Implementation of [PartialTrieScan] for a [Trie]
#[derive(Debug)]
pub struct TrieScanGeneric<'a> {
    /// Underlying [Trie] over which we are iterating
    trie: &'a Trie,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,

    /// [ColumnScan] for each layer in the [PartialTrieScan]
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> TrieScanGeneric<'a> {
    /// Construct a new [TrieScanGeneric].
    pub fn new(trie: &'a Trie, column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>) -> Self {
        Self {
            trie,
            path_types: Vec::with_capacity(column_scans.len()),
            column_scans,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanGeneric<'a> {
    fn up(&mut self) {
        debug_assert!(
            !self.path_types.is_empty(),
            "Attempted to go up in the starting position"
        );

        self.path_types.pop();
    }

    fn down(&mut self, storage_type: StorageTypeName) {
        match self.current_layer() {
            None => {
                self.column_scans[0].get_mut().reset(storage_type);
            }
            Some(current_layer) => {
                let local_index = self.column_scans[current_layer]
                .get_mut()
                .pos(storage_type)
                .expect(
                    "Calling down on a trie is only allowed when currently pointing at an element.",
                );
                let global_index =
                    self.trie.columns[current_layer].global_index(storage_type, local_index);

                let next_layer = current_layer + 1;

                let next_interval = self.trie.columns[next_layer]
                    .interval_bounds(storage_type, global_index)
                    .unwrap_or(0..0);

                self.column_scans[next_layer]
                    .get_mut()
                    .narrow(storage_type, next_interval);
            }
        }

        self.path_types.push(storage_type);
    }

    fn arity(&self) -> usize {
        self.trie.arity()
    }

    fn current_layer(&self) -> Option<usize> {
        self.path_types.len().checked_sub(1)
    }

    fn scan<'b: 'a>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }

    fn path_types(&self) -> &[StorageTypeName] {
        &self.path_types
    }
}
