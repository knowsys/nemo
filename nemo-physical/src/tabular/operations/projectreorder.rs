//! This module defines [GeneratorProjectReorder].

use crate::{
    datasources::{SortedTupleBuffer, TupleBuffer},
    datatypes::StorageValueT,
    tabular::{trie::Trie, triescan::TrieScan},
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
pub struct GeneratorProjectReorder {
    /// Determines which columns of the input trie are used and in what position in the output trie
    projectreordering: ProjectReordering,
    /// Last layer of the input trie that also appears in the output
    last_used_layer: usize,
    /// Arity of the input table
    arity_input: usize,
    /// Arity of the output table
    arity_output: usize,
}

impl GeneratorProjectReorder {
    /// Create a new [GeneratorSubtract].
    pub fn new(output: OperationTable, input: OperationTable) -> Self {
        let projectreordering = ProjectReordering::from_transformation(&input, &output);
        let arity_input = input.len();
        let arity_output = output.len();
        let mut last_used_layer: usize = 0;

        for (output_layer, output_marker) in output.into_iter().enumerate() {
            if input.position(&output_marker).is_some() {
                last_used_layer = output_layer;
            }
        }

        Self {
            projectreordering,
            last_used_layer,
            arity_input,
            arity_output,
        }
    }

    /// Apply the operation to an input [TrieScan]
    pub fn apply_operation<Scan: TrieScan>(&self, mut trie_scan: Scan) -> Trie {
        debug_assert!(trie_scan.num_columns() == self.arity_output);

        let mut current_tuple = vec![StorageValueT::Id32(0); self.arity_output];
        // let mut tuple_buffer = TupleBuffer::new()

        while let Some(changed_layer) = trie_scan.advance_on_layer(self.last_used_layer) {
            for input_layer in changed_layer..self.arity_output {
                if let Some(output_layer) = self.projectreordering.get_partial(input_layer) {
                    current_tuple[output_layer] = trie_scan.current_value(input_layer);
                }
            }

            for value in &current_tuple {
                // tuple_buffer.add(value);
            }

            // tuple_buffer.finish_row();
        }

        // Trie::from_tuple_buffer(SortedTupleBuffer::new(tuple_buffer));
        todo!()
    }
}
