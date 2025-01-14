//! This module defines ...

use std::collections::HashMap;

use streaming_iterator::StreamingIterator;

use crate::{
    function::{evaluation::StackProgram, tree::FunctionTree},
    tabular::{
        rowscan_mut::RowScanMut,
        trie::{Trie, TrieScanGenericMut},
    },
};

use super::{OperationColumnMarker, OperationTable};

/// Used to update values of .
///
/// Note: This does not follow the usual pattern of implementing [OperationGenerator][super::OperationGenerator],
/// since this operation is done directly on [Trie]s.
#[derive(Debug, Clone, Default)]
pub(crate) struct GeneratorUpdate {
    /// Determines which columns of the input trie are used and in what position in the output trie
    projectreordering: usize,
    /// Last layer of the input trie that also appears in the output
    last_used_layer: usize,
    /// Arity of the output table
    arity_output: usize,
}

impl GeneratorUpdate {
    ///
    pub(crate) fn new(
        old: OperationTable,
        new: OperationTable,
        function: &FunctionTree<OperationColumnMarker>,
    ) -> Self {
        let references = function.references();
        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();

        let stack_program = StackProgram::from_function_tree(function, &reference_map, None);

        todo!()
    }

    ///
    pub(crate) fn apply_operation(&self, old: Vec<&mut Trie>, new: &mut Trie) {
        let new_iter = new.partial_iterator_mut();
        let mut new_row_iter = RowScanMut::new(new_iter, 0);

        let mut row_index: usize = 0;

        loop {
            new_row_iter.advance();
            row_index += 1;

            if row_index % 2 == 0 {
                new_row_iter.delete_current_row();
            }

            if new_row_iter.get().is_none() {
                return;
            }
        }
    }
}
