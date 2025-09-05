//! This module defines [GeneratorIncrementalImport].

use std::{cell::RefCell, rc::Rc};

use crate::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    error::ReadingError,
    management::database::Dict,
    tabular::{trie::Trie, triescan::PartialTrieScan},
};

use super::OperationTable;

/// Database operation that triggers an import
/// with bindings from an input table
#[derive(Debug)]
pub(crate) struct GeneratorIncrementalImport {
    /// Handling of the import
    provider: Rc<Box<dyn TableProvider>>,

    /// Encodes for which columns of the input trie
    /// bind the values of which columns in the output trie
    _bindings: Vec<usize>,

    /// Arity of the output table
    arity_output: usize,
}

impl GeneratorIncrementalImport {
    /// Create a new [GeneratorIncrementalImport].
    ///
    /// Every marker in the output table must appear in the input table.
    ///
    /// # Panics
    /// Panics if the above condition is not met.
    pub(crate) fn new(
        output: OperationTable,
        input: OperationTable,
        provider: Rc<Box<dyn TableProvider>>,
    ) -> Self {
        let mut bindings = Vec::default();

        let arity_output = output.arity();

        for column in output {
            bindings.push(
                input.position(&column).expect(
                    "function assumes that every input column is present in the output table",
                ),
            );
        }

        Self {
            provider,
            _bindings: bindings,
            arity_output,
        }
    }

    /// Apply the operation to a [PartialTrieScan].
    pub(crate) fn apply_operation<'a, Scan: PartialTrieScan<'a>>(
        self,
        trie_scan: Scan,
        dictionary: &'a RefCell<Dict>,
    ) -> Result<Trie, ReadingError> {
        if trie_scan.arity() == 0 {
            Trie::zero_arity(true);
        }

        let mut tuple_writer = TupleWriter::new(dictionary, self.arity_output);

        if let Ok(provider) = Rc::try_unwrap(self.provider) {
            provider.provide_table_data(&mut tuple_writer)?;

            Ok(Trie::from_tuple_writer(tuple_writer))
        } else {
            panic!("invalid execution plan: import used in multiple places");
        }
    }
}
