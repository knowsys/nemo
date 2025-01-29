//! This module defines [GeneratorUpdate].

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use streaming_iterator::StreamingIterator;

use crate::{
    datatypes::into_datavalue::IntoDataValue,
    datavalues::AnyDataValue,
    function::{evaluation::StackProgram, tree::FunctionTree},
    management::database::Dict,
    tabular::{
        rowscan::RowScan,
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
        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();
        for (counter, marker) in new.iter().enumerate() {
            reference_map.insert(marker.clone(), counter);
        }
        if let Some(table) = old.get(0) {
            for (counter, marker) in table.iter().enumerate() {
                reference_map.insert(marker.clone(), new.len() + counter);
            }
        }

        let mut permutations = Vec::with_capacity(old.len());
        for table in &old {
            let (_, permutation) = table.align(&new);
            permutations.push(permutation);
        }

        let stack_program = StackProgram::from_function_tree(&filter, &reference_map, None);

        Self {
            permutations,
            filter: stack_program,
        }
    }

    /// Apply the operation.
    pub(crate) fn apply_operation(
        &self,
        old: Vec<&mut Trie>,
        new: &mut Trie,
        dictionary: &RefCell<Dict>,
    ) {
        let mut old_scans = old
            .iter()
            .map(|trie| RowScanMut::new(trie.partial_iterator_mut(), 0))
            .collect::<Vec<_>>();
        let mut new_scan = new.row_iterator();
        let mut current_row = vec![AnyDataValue::new_boolean(false); new.arity() * 2];

        while let Some(new_row) = new_scan.next() {
            current_row
                .iter_mut()
                .zip(new_row.iter())
                .for_each(|(current, new)| {
                    *current = new.into_datavalue(&dictionary.borrow()).expect("...")
                });

            for (mut old_scan, permutation) in old_scans.iter_mut().zip(self.permutations.iter()) {
                while let Some(old_row) = Iterator::next(&mut old_scan) {
                    for (row_index, value) in old_row.into_iter().enumerate() {
                        let value = value.into_datavalue(&dictionary.borrow()).expect("...");
                        current_row[new.arity() + permutation.get(row_index)] = value;

                        if let Some(true) = self.filter.evaluate_bool(&current_row, None) {
                            old_scan.delete_current_row();
                        }
                    }
                }
            }
        }

        // TODO: This is horrible:

        let mut deleted_rows = HashSet::<usize>::new();
        let mut new_scan_a = RowScan::new(new.partial_iterator(), 0);
        let mut new_scan_b = RowScan::new(new.partial_iterator(), 0);

        while let Some(row_a) = Iterator::next(&mut new_scan_a) {
            current_row
                .iter_mut()
                .zip(row_a.iter())
                .for_each(|(current, new)| {
                    *current = new.into_datavalue(&dictionary.borrow()).expect("...")
                });

            let mut row_index: usize = 0;
            while let Some(row_b) = Iterator::next(&mut new_scan_a) {
                current_row
                    .iter_mut()
                    .skip(new.arity())
                    .zip(row_b.iter())
                    .for_each(|(current, new)| {
                        *current = new.into_datavalue(&dictionary.borrow()).expect("...")
                    });

                if let Some(true) = self.filter.evaluate_bool(&current_row, None) {
                    deleted_rows.insert(row_index);
                }

                row_index += 1;
            }
        }

        let mut new_scan = RowScanMut::new(new.partial_iterator_mut(), 0);
        let mut row_index: usize = 0;
        while let Some(row) = Iterator::next(&mut new_scan) {
            if deleted_rows.contains(&row_index) {
                new_scan.delete_current_row();
            }

            row_index += 1;
        }

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
    }
}
