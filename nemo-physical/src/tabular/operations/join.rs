//! This module defines [TrieScanJoin] and [GeneratorJoin].

use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::ColumnScanJoin,
    },
    datatypes::{Double, Float, StorageTypeName},
    tabular::{
        operations::OperationColumnMarker,
        triescan::{PartialTrieScan, TrieScanEnum},
    },
    util::mapping::{permutation::Permutation, traits::NatMapping},
};

use super::{OperationGenerator, OperationTable};

/// Helper structure which identifies a column
/// by its (input) table index and its column index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColumnIndex {
    /// The index of the relation this column belons to.
    pub relation: usize,
    /// The index of this column within the relation.
    pub column: usize,
}

/// Used to create a [TrieScanJoin]
#[derive(Debug)]
pub struct GeneratorJoin {
    /// In the order of the output column,
    /// contains all the columns (identified by its [ColumnIndex])
    /// that should be joined
    bindings: Vec<Vec<ColumnIndex>>,
    /// The overall number of input relations
    num_input_relations: usize,
}

impl GeneratorJoin {
    /// Create a new [GeneratorJoin].
    pub fn new(output: OperationTable, input: Vec<OperationTable>) -> Self {
        let num_input_relations = input.len();

        // Associates the marker of each output column
        // with a list of columns that need to be joined
        let mut output_map = HashMap::<OperationColumnMarker, Vec<ColumnIndex>>::new();

        for (relation_index, relation_table) in input.into_iter().enumerate() {
            for (column_index, marker) in relation_table.into_iter().enumerate() {
                let output_vec = match output_map.entry(marker) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(Vec::new()),
                };

                output_vec.push(ColumnIndex {
                    relation: relation_index,
                    column: column_index,
                });
            }
        }

        let mut bindings = Vec::<Vec<ColumnIndex>>::with_capacity(output.arity());
        for marker in output.into_iter() {
            bindings.push(
                output_map.remove(&marker).expect(
                    "Every output column must be associated with at least one input column.",
                ),
            )
        }

        Self {
            bindings,
            num_input_relations,
        }
    }

    /// Change the bindings such as to take into account a reordering of the output table.
    pub fn apply_permutation(&mut self, permutation: &Permutation) {
        self.bindings = permutation.permute(&self.bindings);
    }

    fn binding_vector(&self) -> Vec<Vec<usize>> {
        let mut result = vec![vec![]; self.num_input_relations];

        for (output_index, input_columns) in self.bindings.iter().enumerate() {
            for input_column in input_columns {
                for _ in result[input_column.relation].len()..=input_column.column {
                    result[input_column.relation].push(0);
                }

                result[input_column.relation][input_column.column] = output_index;
            }
        }

        result
    }

    fn sort_input_relations(&self) -> Vec<Permutation> {
        let bindings_vector = self.binding_vector();
        bindings_vector
            .into_iter()
            .map(|v| Permutation::from_unsorted(&v))
            .collect()
    }

    /// Returns whether or not this binding is compatible with the leapfrog triejoin algorithm.
    pub fn is_leapfrog_compatible(&self) -> bool {
        self.sort_input_relations().iter().all(|p| p.is_identity())
    }

    /// This function makes sure that the join is possible to execute with the leapfrog triejoin algorithm.
    /// It returns a list of permutations where the ith entry indicates the needed reordering for the ith input table.
    pub fn comply_with_leapfrog(&mut self) -> Vec<Permutation> {
        let sort_permutations = self.sort_input_relations();

        for input_columns in self.bindings.iter_mut() {
            for input_column in input_columns {
                input_column.column =
                    sort_permutations[input_column.relation].get(input_column.column);
            }
        }

        sort_permutations
    }
}

impl OperationGenerator for GeneratorJoin {
    fn generate<'a>(&'_ self, trie_scans: Vec<TrieScanEnum<'a>>) -> TrieScanEnum<'a> {
        // `self.bindings` contains the columns used for each output index.
        // `layers_to_scans` just contains a list of the relation indices for each output.
        let layers_to_scans = self
            .bindings
            .iter()
            .map(|binding| {
                binding
                    .iter()
                    .map(|index| index.relation)
                    .collect::<Vec<usize>>()
            })
            .collect::<Vec<_>>();

        let mut column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>> =
            Vec::with_capacity(self.bindings.len());

        for output_index in 0..self.bindings.len() {
            macro_rules! join_scan {
                ($type:ty, $scan:ident) => {{
                    let mut input_scans =
                        Vec::<&'a ColumnScanCell<$type>>::with_capacity(trie_scans.len());

                    for &input_column in &self.bindings[output_index] {
                        let input_scan = &unsafe {
                            &*trie_scans[input_column.relation]
                                .scan(input_column.column)
                                .get()
                        }
                        .$scan;
                        input_scans.push(input_scan)
                    }

                    ColumnScanEnum::ColumnScanJoin(ColumnScanJoin::new(input_scans))
                }};
            }

            let join_scan_id32 = join_scan!(u32, scan_id32);
            let join_scan_id64 = join_scan!(u64, scan_id64);
            let join_scan_i64 = join_scan!(i64, scan_i64);
            let join_scan_float = join_scan!(Float, scan_float);
            let join_scan_double = join_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                join_scan_id32,
                join_scan_id64,
                join_scan_i64,
                join_scan_float,
                join_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        TrieScanEnum::TrieScanJoin(TrieScanJoin {
            trie_scans,
            layers_to_scans,
            path_types: Vec::new(),
            column_scans,
        })
    }
}

/// [`PartialTrieScan`] which represents the result from joining a list of [`PartialTrieScan`]s
#[derive(Debug)]
pub struct TrieScanJoin<'a> {
    /// Input trie scans over of which the join is computed
    trie_scans: Vec<TrieScanEnum<'a>>,

    /// Indices of the sub tries which are associated with a given layer in the result
    /// E.g., given the join R(a, b), S(b, c), T(a, c) with variable order a, b, c
    /// we have `layers_to_scans = [[R, T], [R, S], [S, T]]`
    /// or rather `layers_to_scans = [[0, 2], [0, 1], [1, 2]]` if `trie_scans = [R, S, T]`
    layers_to_scans: Vec<Vec<usize>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,

    /// For each layer in the resulting trie, contains a [`ColumnScanRainbow`],
    /// which computed the intersection of the relevant columns of the input tries.
    ///
    /// Note: We're keeping an [`UnsafeCell`] here since the
    /// [`ColumnScanRainbow`] are actually borrowed from within
    /// `trie_scans`. We're not actually modifying through these
    /// references (since there's another layer of Cells hidden in
    /// [`ColumnScanRainbow`], we're just using this satisfy the
    /// borrow checker.  
    ///
    /// TODO: find a nicer solution for this that
    /// doesn't expose [`UnsafeCell`] as part of the API.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanJoin<'a> {
    fn up(&mut self) {
        let current_layer = self.path_types.len();

        let current_scans = &self.layers_to_scans[current_layer];
        for &scan_index in current_scans {
            self.trie_scans[scan_index].up();
        }

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let next_layer = self.path_types.len();
        debug_assert!(next_layer < self.arity());

        let affected_scans = &self.layers_to_scans[next_layer];
        for &scan_index in affected_scans {
            self.trie_scans[scan_index].down(next_type);
        }

        // The above down call has changed the sub scans of the current layer.
        // Hence, we need to reset its state.
        self.column_scans[next_layer].get_mut().reset(next_type);

        self.path_types.push(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        &self.path_types
    }

    fn arity(&self) -> usize {
        self.layers_to_scans.len()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }
}
