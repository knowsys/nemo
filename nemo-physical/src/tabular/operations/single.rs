//! This module defines [GeneratorSingle].

use std::collections::HashSet;

use streaming_iterator::StreamingIterator;

use crate::{
    datatypes::StorageValueT,
    tabular::{
        buffer::tuple_buffer::TupleBuffer,
        rowscan::{Row, RowScan},
        trie::Trie,
        triescan::PartialTrieScan,
    },
};

use super::OperationTable;

///
///
/// Note: This does not follow the usual pattern of implementing [OperationGenerator][super::OperationGenerator],
/// since this operation is not done via a [PartialTrieScan].
#[derive(Debug, Clone)]
pub(crate) struct GeneratorSingle {
    /// Columns for which all entries will be computed
    full_columns: Vec<usize>,
    /// Last `existential` column need only one value
    existential: usize,
}

impl GeneratorSingle {
    /// Create a new [GeneratorSingle].
    ///
    /// Every marker in the output table must appear in the input table.
    ///
    /// # Panics
    /// Panics if the above condition is not met.
    pub(crate) fn new(table: OperationTable, single_columns: OperationTable) -> Self {
        let mut last_full_column: Option<usize> = None;

        for (column, marker) in table.iter().enumerate() {
            if !single_columns.iter().any(|single| single == marker) {
                last_full_column = Some(column);
            }
        }

        let existential = if let Some(last_full_column) = last_full_column {
            table.arity() - last_full_column - 1
        } else {
            table.arity()
        };

        let mut full_columns = Vec::<usize>::new();

        for (column, marker) in table.iter().enumerate() {
            if column >= table.arity() - existential {
                break;
            }

            if !single_columns.iter().any(|single| single == marker) {
                full_columns.push(column);
            }
        }

        Self {
            full_columns,
            existential,
        }
    }

    /// Apply the operation to a [PartialTrieScan].
    pub(crate) fn apply_operation<'a, Scan: PartialTrieScan<'a>>(&self, trie_scan: Scan) -> Trie {
        let arity = trie_scan.arity();

        if arity == 0 {
            Trie::zero_arity(true);
        }

        let mut rowscan = RowScan::new(trie_scan, self.existential, 0);
        let mut tuple_buffer = TupleBuffer::new(arity);

        let mut enough = HashSet::<Vec<StorageValueT>>::default();

        while let Some(Row {
            row: current_row,
            change: _,
        }) = StreamingIterator::next(&mut rowscan)
        {
            if self.full_columns.len() != arity {
                let single = self
                    .full_columns
                    .iter()
                    .map(|&column| current_row[column])
                    .collect::<Vec<_>>();

                if !enough.insert(single) {
                    continue;
                }
            }

            for value in current_row {
                tuple_buffer.add_tuple_value(*value);
            }
        }

        Trie::from_tuple_buffer(tuple_buffer.finalize())
    }
}
