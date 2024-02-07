//! This module defines [TrieScanJoin] and [GeneratorJoin].

use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::join::ColumnScanJoin,
    },
    datatypes::{Double, Float, StorageTypeName},
    dictionary::meta_dv_dict::MetaDvDictionary,
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
    ///
    /// Markers in every input table
    /// must appear in the same order as they appear in the output table.
    /// No marker must be repeated in an input table.
    /// Every marker in the output table must appear in some input table.
    ///
    /// # Panics
    /// Panics if any of the above conditions is not met.
    pub fn new(output: OperationTable, input: Vec<OperationTable>) -> Self {
        let num_input_relations = input.len();

        // Associates the marker of each output column
        // with a list of columns that need to be joined
        let mut output_map = HashMap::<OperationColumnMarker, Vec<ColumnIndex>>::new();

        for (relation_index, relation_table) in input.iter().enumerate() {
            for (column_index, marker) in relation_table.iter().enumerate() {
                let output_vec = match output_map.entry(marker.clone()) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(Vec::new()),
                };

                output_vec.push(ColumnIndex {
                    relation: relation_index,
                    column: column_index,
                });

                if column_index > 0 {
                    let previous_column_index = column_index - 1;
                    let previous_marker = &relation_table[previous_column_index];

                    let current_output_position = output
                        .position(&marker)
                        .expect("Every input marker must appear in the output");
                    let previous_output_position = output
                        .position(previous_marker)
                        .expect("Every input marker must appear in the output");

                    debug_assert!(current_output_position > previous_output_position)
                }
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
    fn generate<'a>(
        &'_ self,
        trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        _dictionary: &'a MetaDvDictionary,
    ) -> Option<TrieScanEnum<'a>> {
        // We return `None` if any of the input tables is `None`
        let mut trie_scans = trie_scans.into_iter().collect::<Option<Vec<_>>>()?;

        if trie_scans.is_empty() {
            // An empty join results in an empty table
            return None;
        }

        if trie_scans.len() == 1 {
            // If the join only contains one element then we an just return
            // the underlying table
            return Some(trie_scans.remove(0));
        }

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

        Some(TrieScanEnum::TrieScanJoin(TrieScanJoin {
            trie_scans,
            layers_to_scans,
            path_types: Vec::new(),
            column_scans,
        }))
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
        let current_layer = self.path_types.len() - 1;

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

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        datatypes::{StorageTypeName, StorageValueT},
        dictionary::meta_dv_dict::MetaDvDictionary,
        tabular::{
            operations::{OperationGenerator, OperationTable, OperationTableGenerator},
            triescan::TrieScanEnum,
        },
        util::test_util::test::{trie_dfs, trie_id32},
    };

    use super::GeneratorJoin;

    /// Generate a [TrieScanEnum] for a join between the provided input scans.
    pub(crate) fn generate_join_scan<'a>(
        dictionary: &'a MetaDvDictionary,
        output: Vec<&str>,
        input: Vec<(TrieScanEnum<'a>, Vec<&str>)>,
    ) -> TrieScanEnum<'a> {
        let mut marker_generator = OperationTableGenerator::new();
        for marker in output.clone() {
            marker_generator.add_marker(marker);
        }

        for marker in input.iter().flat_map(|(_, markers)| markers.iter()) {
            marker_generator.add_marker(marker);
        }

        let markers_output = marker_generator.operation_table(output.iter());
        let (input, markers_input): (Vec<Option<TrieScanEnum>>, Vec<OperationTable>) = input
            .into_iter()
            .map(|(scan, markers)| (Some(scan), marker_generator.operation_table(markers.iter())))
            .unzip();

        let join_generator = GeneratorJoin::new(markers_output, markers_input);
        join_generator.generate(input, dictionary).unwrap()
    }

    #[test]
    fn basic_trie_join() {
        let dictionary = MetaDvDictionary::default();

        let trie_a = trie_id32(vec![&[1, 2], &[1, 3], &[1, 4], &[2, 5], &[3, 6], &[3, 7]]);
        let trie_b = trie_id32(vec![
            &[1, 1],
            &[2, 8],
            &[2, 9],
            &[3, 10],
            &[6, 11],
            &[6, 12],
        ]);

        let trie_a_scan = TrieScanEnum::TrieScanGeneric(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::TrieScanGeneric(trie_b.partial_iterator());

        let mut join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y", "z"],
            vec![(trie_a_scan, vec!["x", "y"]), (trie_b_scan, vec!["y", "z"])],
        );

        trie_dfs(
            &mut join_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(3),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(6),
                StorageValueT::Id32(11),
                StorageValueT::Id32(12),
            ],
        );
    }

    #[test]
    fn self_join() {
        let dictionary = MetaDvDictionary::default();

        let trie = trie_id32(vec![
            &[1, 2],
            &[1, 3],
            &[1, 5],
            &[1, 10],
            &[2, 4],
            &[2, 7],
            &[2, 10],
            &[5, 9],
            &[7, 8],
            &[7, 9],
            &[7, 10],
        ]);

        let trie_a_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());
        let trie_b_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y", "z"],
            vec![(trie_a_scan, vec!["x", "y"]), (trie_b_scan, vec!["y", "z"])],
        );

        trie_dfs(
            &mut join_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(5),
                StorageValueT::Id32(9),
                StorageValueT::Id32(2),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
            ],
        )
    }

    #[test]
    fn self_join_inverse() {
        let dictionary = MetaDvDictionary::default();

        let trie = trie_id32(vec![
            &[1, 2],
            &[1, 3],
            &[1, 5],
            &[1, 10],
            &[2, 4],
            &[2, 7],
            &[2, 10],
            &[5, 9],
            &[7, 8],
            &[7, 9],
            &[7, 10],
        ]);

        let trie_inv = trie_id32(vec![
            &[2, 1],
            &[3, 1],
            &[4, 2],
            &[5, 1],
            &[7, 2],
            &[8, 7],
            &[9, 5],
            &[9, 7],
            &[10, 1],
            &[10, 2],
            &[10, 7],
        ]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());
        let trie_inv_scan = TrieScanEnum::TrieScanGeneric(trie_inv.partial_iterator());

        let mut join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y", "z"],
            vec![(trie_scan, vec!["x", "z"]), (trie_inv_scan, vec!["y", "z"])],
        );

        trie_dfs(
            &mut join_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(2),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(2),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(5),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(7),
                StorageValueT::Id32(9),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(7),
                StorageValueT::Id32(5), // x = 5
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(7), // x = 7
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
            ],
        )
    }

    #[test]
    fn self_join_2() {
        let dictionary = MetaDvDictionary::default();

        let trie_new = trie_id32(vec![
            &[1, 4],
            &[1, 7],
            &[1, 9],
            &[2, 8],
            &[2, 9],
            &[4, 1],
            &[7, 1],
            &[8, 2],
            &[9, 1],
            &[9, 2],
            &[10, 1],
            &[10, 2],
        ]);

        let trie_old = trie_id32(vec![
            &[1, 2],
            &[1, 3],
            &[1, 4],
            &[1, 5],
            &[1, 7],
            &[1, 10],
            &[2, 4],
            &[2, 7],
            &[2, 8],
            &[2, 9],
            &[2, 10],
            &[4, 1],
            &[5, 9],
            &[7, 1],
            &[7, 8],
            &[7, 9],
            &[7, 10],
            &[8, 2],
            &[9, 1],
            &[9, 2],
            &[10, 1],
            &[10, 2],
        ]);

        let trie_new_scan = TrieScanEnum::TrieScanGeneric(trie_new.partial_iterator());
        let trie_old_scan = TrieScanEnum::TrieScanGeneric(trie_old.partial_iterator());

        let mut join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y", "z"],
            vec![
                (trie_new_scan, vec!["x", "y"]),
                (trie_old_scan, vec!["y", "z"]),
            ],
        );

        trie_dfs(
            &mut join_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(4), // y = 4
                StorageValueT::Id32(1),
                StorageValueT::Id32(7), // y = 7
                StorageValueT::Id32(1),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(9), // y = 9
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(8), // y = 8
                StorageValueT::Id32(2),
                StorageValueT::Id32(9), // y = 9
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4), // x = 4
                StorageValueT::Id32(1), // y = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(7), // x = 7
                StorageValueT::Id32(1), // y = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(8), // x = 8
                StorageValueT::Id32(2), // y = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(9), // x = 9
                StorageValueT::Id32(1), // y = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2), // y = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(10), // x = 10
                StorageValueT::Id32(1),  // y = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2), // y = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
            ],
        )
    }

    #[test]
    fn another_join_test() {
        let dictionary = MetaDvDictionary::default();

        let trie_a = trie_id32(vec![
            &[1, 2],
            &[1, 3],
            &[2, 1],
            &[2, 2],
            &[2, 4],
            &[3, 2],
            &[3, 3],
            &[4, 2],
            &[4, 4],
            &[5, 4],
        ]);

        let trie_b = trie_id32(vec![&[2, 2], &[2, 3], &[2, 4], &[3, 1], &[3, 2], &[3, 4]]);

        let trie_a_scan = TrieScanEnum::TrieScanGeneric(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::TrieScanGeneric(trie_b.partial_iterator());

        let mut join_scan = generate_join_scan(
            &dictionary,
            vec!["x", "y", "z"],
            vec![(trie_a_scan, vec!["y", "z"]), (trie_b_scan, vec!["x", "y"])],
        );

        trie_dfs(
            &mut join_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(2), // x = 1
                StorageValueT::Id32(2), // y = 2
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
                StorageValueT::Id32(3), // y = 3
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(4), // y = 4
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
                StorageValueT::Id32(3), // x = 3
                StorageValueT::Id32(1), // y = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(2), // y = 2
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
                StorageValueT::Id32(4), // y = 4
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
            ],
        )
    }
}
