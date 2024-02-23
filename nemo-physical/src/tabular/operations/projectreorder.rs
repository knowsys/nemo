//! This module defines [GeneratorProjectReorder].

use std::collections::HashMap;

use streaming_iterator::StreamingIterator;

use crate::{
    datatypes::StorageValueT,
    management::execution_plan::ColumnOrder,
    tabular::{
        buffer::tuple_buffer::TupleBuffer, rowscan::RowScan, trie::Trie, triescan::PartialTrieScan,
    },
    util::mapping::{ordered_choice::SortedChoice, traits::NatMapping},
};

use super::OperationTable;

/// Type that represents a projection and reordering of an input table.
pub type ProjectReordering = SortedChoice;

/// Used to perform a project and reorder operation on a [TrieScan].
///
/// Note: This does not follow the usual pattern of implementing [OperationGenerator][super::OperationGenerator],
/// since this operation is not done via a [PartialTrieScan][crate::tabular::triescan::PartialTrieScan].
#[derive(Debug, Clone)]
pub(crate) struct GeneratorProjectReorder {
    /// Determines which columns of the input trie are used and in what position in the output trie
    projectreordering: ProjectReordering,
    /// Last layer of the input trie that also appears in the output
    last_used_layer: usize,
    /// Arity of the output table
    arity_output: usize,
}

impl GeneratorProjectReorder {
    /// Create a new [GeneratorProjectReorder].
    ///
    /// Every marker in the output table must appear in the input table.
    ///
    /// # Panics
    /// Panics if the above condition is not met.
    pub(crate) fn new(output: OperationTable, input: OperationTable) -> Self {
        let projectreordering = ProjectReordering::from_transformation(&input, &output);
        let arity_output = output.len();
        let mut last_used_layer: usize = 0;

        for output_marker in output.iter() {
            if let Some(input_layer) = input.position(output_marker) {
                if input_layer > last_used_layer {
                    last_used_layer = input_layer;
                }
            }
        }

        Self {
            projectreordering,
            last_used_layer,
            arity_output,
        }
    }

    /// Create a [GeneratorProjectReorder],
    /// which transforms a [Trie] with a given input [ColumnOrder]
    /// into a [Trie] with the same contents but in the output [ColumnOrder].
    pub(crate) fn from_reordering(source: ColumnOrder, target: ColumnOrder, arity: usize) -> Self {
        let mut result_map = HashMap::<usize, usize>::new();

        for input in 0..arity {
            let source_output = source.get(input);
            let target_output = target.get(input);

            result_map.insert(source_output, target_output);
        }

        Self {
            projectreordering: ProjectReordering::from_map(result_map, arity),
            last_used_layer: arity.checked_sub(1).unwrap_or(0),
            arity_output: arity,
        }
    }

    /// Apply the operation to a [PartialTrieScan].
    pub(crate) fn apply_operation_partial<'a, Scan: PartialTrieScan<'a>>(
        &self,
        trie_scan: Scan,
    ) -> Trie {
        debug_assert!(trie_scan.arity() == self.projectreordering.domain_size());
        debug_assert!(self.last_used_layer < trie_scan.arity());

        let cut = trie_scan.arity() - self.last_used_layer - 1;

        let mut rowscan = RowScan::new(trie_scan, cut);

        let mut current_tuple = vec![StorageValueT::Id32(0); self.arity_output];
        let mut tuple_buffer = TupleBuffer::new(self.arity_output);

        while let Some(current_row) = StreamingIterator::next(&mut rowscan) {
            debug_assert!(current_row.len() <= self.last_used_layer + 1);

            let start_change = self.last_used_layer + 1 - current_row.len();

            for (row_index, current_value) in current_row.into_iter().enumerate() {
                let input_layer = start_change + row_index;
                if let Some(output_layer) = self.projectreordering.get_partial(input_layer) {
                    current_tuple[output_layer] = *current_value;
                }
            }

            for value in &current_tuple {
                tuple_buffer.add_tuple_value(*value);
            }
        }

        Trie::from_tuple_buffer(tuple_buffer.finalize())
    }

    /// Return whether this operation would leave the input [Trie] unchanged.
    pub(crate) fn is_noop(&self) -> bool {
        self.projectreordering.is_identity()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datatypes::StorageValueT, tabular::operations::OperationTableGenerator,
        util::test_util::test::trie_id32,
    };

    use super::GeneratorProjectReorder;

    #[test]
    fn project_single_small_hole() {
        let trie = trie_id32(vec![
            &[1, 3, 7],
            &[1, 3, 9],
            &[1, 5, 5],
            &[1, 5, 8],
            &[2, 2, 4],
            &[2, 2, 6],
            &[2, 4, 2],
            &[2, 4, 3],
        ]);

        let trie_scan_no_first = trie.partial_iterator();
        let trie_scan_no_middle = trie.partial_iterator();
        let trie_scan_no_last = trie.partial_iterator();

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");

        let markers_trie = marker_generator.operation_table(["x", "y", "z"].iter());
        let markers_no_first = marker_generator.operation_table(["y", "z"].iter());
        let markers_no_middle = marker_generator.operation_table(["x", "z"].iter());
        let markers_no_last = marker_generator.operation_table(["x", "y"].iter());

        let generator_no_first =
            GeneratorProjectReorder::new(markers_no_first, markers_trie.clone());
        let generator_no_middle =
            GeneratorProjectReorder::new(markers_no_middle, markers_trie.clone());
        let generator_no_last = GeneratorProjectReorder::new(markers_no_last, markers_trie);

        let result_no_first = generator_no_first
            .apply_operation_partial(trie_scan_no_first)
            .row_iterator()
            .collect::<Vec<_>>();
        let result_no_middle = generator_no_middle
            .apply_operation_partial(trie_scan_no_middle)
            .row_iterator()
            .collect::<Vec<_>>();
        let result_no_last = generator_no_last
            .apply_operation_partial(trie_scan_no_last)
            .row_iterator()
            .collect::<Vec<_>>();

        let expected_no_first = vec![
            vec![StorageValueT::Id32(2), StorageValueT::Id32(4)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(6)],
            vec![StorageValueT::Id32(3), StorageValueT::Id32(7)],
            vec![StorageValueT::Id32(3), StorageValueT::Id32(9)],
            vec![StorageValueT::Id32(4), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(4), StorageValueT::Id32(3)],
            vec![StorageValueT::Id32(5), StorageValueT::Id32(5)],
            vec![StorageValueT::Id32(5), StorageValueT::Id32(8)],
        ];

        let expected_no_middle = vec![
            vec![StorageValueT::Id32(1), StorageValueT::Id32(5)],
            vec![StorageValueT::Id32(1), StorageValueT::Id32(7)],
            vec![StorageValueT::Id32(1), StorageValueT::Id32(8)],
            vec![StorageValueT::Id32(1), StorageValueT::Id32(9)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(3)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(4)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(6)],
        ];

        let expected_no_last = vec![
            vec![StorageValueT::Id32(1), StorageValueT::Id32(3)],
            vec![StorageValueT::Id32(1), StorageValueT::Id32(5)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(2), StorageValueT::Id32(4)],
        ];

        assert_eq!(result_no_first, expected_no_first);
        assert_eq!(result_no_middle, expected_no_middle);
        assert_eq!(result_no_last, expected_no_last);
    }

    #[test]
    fn project_multiple_big_holes() {
        let trie = trie_id32(vec![
            &[1, 3, 7, 5, 1, 0],
            &[1, 3, 7, 5, 2, 1],
            &[1, 3, 7, 10, 20, 2],
            &[1, 3, 7, 15, 3, 3],
            &[1, 3, 7, 15, 4, 4],
            &[1, 3, 9, 2, 21, 5],
            &[1, 3, 9, 9, 5, 6],
            &[1, 3, 9, 9, 6, 7],
            &[1, 5, 5, 6, 22, 8],
            &[1, 5, 8, 1, 7, 9],
            &[1, 5, 8, 1, 8, 10],
            &[2, 2, 4, 3, 23, 11],
            &[2, 2, 6, 7, 9, 12],
            &[2, 2, 6, 7, 10, 13],
            &[2, 2, 6, 9, 24, 14],
            &[2, 4, 2, 11, 11, 15],
            &[2, 4, 2, 11, 12, 16],
            &[2, 4, 3, 13, 25, 17],
            &[2, 4, 3, 17, 13, 18],
            &[2, 4, 3, 17, 14, 19],
        ]);

        let trie_scan = trie.partial_iterator();

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("a");
        marker_generator.add_marker("b");
        marker_generator.add_marker("c");
        marker_generator.add_marker("d");
        marker_generator.add_marker("e");
        marker_generator.add_marker("f");

        let markers_trie = marker_generator.operation_table(["a", "b", "c", "d", "e", "f"].iter());
        let markers_projected = marker_generator.operation_table(["a", "d", "f"].iter());

        let project_generator = GeneratorProjectReorder::new(markers_projected, markers_trie);

        let result = project_generator
            .apply_operation_partial(trie_scan)
            .row_iterator()
            .collect::<Vec<_>>();

        let expected = vec![
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(1),
                StorageValueT::Id32(9),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(1),
                StorageValueT::Id32(10),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(5),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Id32(0),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Id32(1),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(6),
                StorageValueT::Id32(8),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(9),
                StorageValueT::Id32(6),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(9),
                StorageValueT::Id32(7),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(15),
                StorageValueT::Id32(3),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(15),
                StorageValueT::Id32(4),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(11),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(7),
                StorageValueT::Id32(12),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(7),
                StorageValueT::Id32(13),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(9),
                StorageValueT::Id32(14),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(11),
                StorageValueT::Id32(15),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(11),
                StorageValueT::Id32(16),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(13),
                StorageValueT::Id32(17),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(17),
                StorageValueT::Id32(18),
            ],
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(17),
                StorageValueT::Id32(19),
            ],
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn reorder() {
        let trie = trie_id32(vec![
            &[1, 3, 7],
            &[1, 3, 9],
            &[1, 5, 5],
            &[1, 5, 8],
            &[2, 2, 4],
            &[2, 2, 6],
            &[2, 4, 2],
            &[2, 4, 3],
        ]);

        let trie_scan = trie.partial_iterator();

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");

        let markers_trie = marker_generator.operation_table(["x", "y", "z"].iter());
        let markers_reorder = marker_generator.operation_table(["z", "x", "y"].iter());

        let reorder_generator = GeneratorProjectReorder::new(markers_reorder, markers_trie);

        let result = reorder_generator
            .apply_operation_partial(trie_scan)
            .row_iterator()
            .collect::<Vec<_>>();

        let expected = vec![
            vec![
                StorageValueT::Id32(2),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
            ],
            vec![
                StorageValueT::Id32(3),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
            ],
            vec![
                StorageValueT::Id32(4),
                StorageValueT::Id32(2),
                StorageValueT::Id32(2),
            ],
            vec![
                StorageValueT::Id32(5),
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
            ],
            vec![
                StorageValueT::Id32(6),
                StorageValueT::Id32(2),
                StorageValueT::Id32(2),
            ],
            vec![
                StorageValueT::Id32(7),
                StorageValueT::Id32(1),
                StorageValueT::Id32(3),
            ],
            vec![
                StorageValueT::Id32(8),
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
            ],
            vec![
                StorageValueT::Id32(9),
                StorageValueT::Id32(1),
                StorageValueT::Id32(3),
            ],
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn project_and_reorder() {
        let trie = trie_id32(vec![
            &[1, 3, 7],
            &[1, 3, 9],
            &[1, 5, 5],
            &[1, 5, 8],
            &[2, 2, 4],
            &[2, 2, 6],
            &[2, 4, 2],
            &[2, 4, 3],
        ]);

        let trie_scan = trie.partial_iterator();

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");

        let markers_trie = marker_generator.operation_table(["x", "y", "z"].iter());
        let markers_reorder = marker_generator.operation_table(["z", "x"].iter());

        let reorder_generator = GeneratorProjectReorder::new(markers_reorder, markers_trie);

        let result = reorder_generator
            .apply_operation_partial(trie_scan)
            .row_iterator()
            .collect::<Vec<_>>();

        let expected = vec![
            vec![StorageValueT::Id32(2), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(3), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(4), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(5), StorageValueT::Id32(1)],
            vec![StorageValueT::Id32(6), StorageValueT::Id32(2)],
            vec![StorageValueT::Id32(7), StorageValueT::Id32(1)],
            vec![StorageValueT::Id32(8), StorageValueT::Id32(1)],
            vec![StorageValueT::Id32(9), StorageValueT::Id32(1)],
        ];

        assert_eq!(result, expected);
    }
}
