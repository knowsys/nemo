//! This module defines [GeneratorIncrementalImport].

use std::{cell::RefCell, rc::Rc};

use crate::{
    datasources::{table_providers::TableProvider, tuple_writer::TupleWriter},
    datatypes::into_datavalue::IntoDataValue,
    error::ReadingError,
    management::database::Dict,
    tabular::{rowscan::RowScan, trie::Trie, triescan::PartialTrieScan},
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
    bound_positions: Vec<usize>,

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
        let mut bound_positions = Vec::default();

        let arity_output = output.arity();

        for column in input {
            bound_positions.push(
                output.position(&column).expect(
                    "function assumes that every input column is present in the output table",
                ),
            );
        }

        Self {
            provider,
            bound_positions,
            arity_output,
        }
    }

    /// Apply the operation to a [PartialTrieScan].
    pub(crate) fn apply_operation<'a, Scan: PartialTrieScan<'a>>(
        self,
        trie_scan: Scan,
        dictionary: &'a RefCell<Dict>,
    ) -> Result<Trie, ReadingError> {
        log::trace!("doing incremental import");

        let mut tuple_writer = TupleWriter::new(dictionary, self.arity_output);

        if let Ok(provider) = Rc::try_unwrap(self.provider) {
            let num_bindings = 0; // TODO: figure out how many bindings we have.

            if trie_scan.arity() == 0
                || !provider.should_import_with_bindings(&self.bound_positions, num_bindings)
            {
                // do ordinary import instead
                provider.provide_table_data(&mut tuple_writer)?;
            } else {
                let scan = RowScan::new_full(trie_scan);
                let bindings = scan
                    .map(|row| {
                        row.into_iter()
                            .map(|value| {
                                value
                                    .into_datavalue(
                                        &dictionary
                                            .try_borrow()
                                            .expect("should not be borrowed already"),
                                    )
                                    .expect("value is in the dictionary")
                            })
                            .collect()
                    })
                    .collect::<Vec<_>>();

                provider.provide_table_data_with_bindings(
                    &mut tuple_writer,
                    &self.bound_positions,
                    &bindings,
                    bindings.len(),
                )?;
            }

            Ok(Trie::from_tuple_writer(tuple_writer))
        } else {
            panic!("invalid execution plan: import used in multiple places");
        }
    }
}
