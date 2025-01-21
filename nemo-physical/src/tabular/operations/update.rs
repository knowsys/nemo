//! This module defines [GeneratorUpdate].

use std::collections::HashMap;

use streaming_iterator::StreamingIterator;

use crate::{
    function::{evaluation::StackProgram, tree::FunctionTree},
    tabular::{
        rowscan_mut::RowScanMut,
        trie::{Trie, TrieScanGenericMut},
    },
    util::mapping::permutation::Permutation,
};

use super::{Filter, OperationColumnMarker, OperationTable};

/// Used to update values of .
///
/// Note: This does not follow the usual pattern of implementing [OperationGenerator][super::OperationGenerator],
/// since this operation is done directly on [Trie]s.
#[derive(Debug, Clone)]
pub(crate) struct GeneratorUpdate {
    /// For each incoming trie, how to reorder the arguments such that it complies with `filter`
    permutations: Vec<Permutation>,

    /// Filter that determines whether to keep values
    filter: StackProgram,
}

impl GeneratorUpdate {
    /// Create a new [GeneratorUpdate].
    pub(crate) fn new(new: OperationTable, old: Vec<OperationTable>, filter: Filter) -> Self {
        // let references = function.references();
        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();
        for (counter, marker) in new.iter().enumerate() {
            reference_map.insert(marker, counter);
        }

        let stack_program = StackProgram::from_function_tree(&filter, &reference_map, None);

        Self {
            permutations: vec![],
            filter: stack_program,
        }
    }

    /// Apply the operation.
    pub(crate) fn apply_operation(&self, old: Vec<&mut Trie>, new: &mut Trie) {}

    // /// Apply the operation.
    // pub(crate) fn apply_operation(&self, old: Vec<&mut Trie>, new: &mut Trie) {
    //     let new_iter = new.partial_iterator_mut();
    //     let mut new_row_iter = RowScanMut::new(new_iter, 0);

    //     let mut row_index: usize = 0;

    //     loop {
    //         new_row_iter.advance();
    //         row_index += 1;

    //         if row_index % 2 == 0 {
    //             new_row_iter.delete_current_row();
    //         }

    //         if new_row_iter.get().is_none() {
    //             return;
    //         }
    //     }
    // }
}
