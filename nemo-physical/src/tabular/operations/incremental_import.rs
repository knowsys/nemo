//! This module defines [GeneratorIncrementalImport].

use std::{cell::RefCell, rc::Rc};

use crate::{
    datasources::{
        bindings::{Bindings, ProductBindings},
        table_providers::TableProvider,
        tuple_writer::TupleWriter,
    },
    datatypes::into_datavalue::IntoDataValue,
    datavalues::AnyDataValue,
    error::ReadingError,
    management::database::Dict,
    tabular::{rowscan::RowScan, trie::Trie, triescan::PartialTrieScan},
};

/// Database operation that triggers an import
/// with bindings that are derived from
/// one or more input tables
#[derive(Debug)]
pub(crate) struct GeneratorIncrementalImport {
    /// Handling of the import
    provider: Rc<Box<dyn TableProvider>>,

    /// List of indices for each input table
    /// that encode which columns of the output table
    /// are bound by the respective input table
    ///
    /// E.g. if the import results in a table with arity 4,
    /// bound_positions = [[0, 3], [1]], means that
    /// the first input table contains bindings for column 0 and 3,
    /// the second input table contains bindings for column 1,
    /// and column 2 is not bound.
    bound_positions: Vec<Vec<usize>>,

    /// Arity of the output table
    arity_output: usize,
}

impl GeneratorIncrementalImport {
    /// Create a new [GeneratorIncrementalImport].
    ///
    /// # Panics
    /// Panics if the above condition is not met.
    pub(crate) fn new(
        bound_positions: Vec<Vec<usize>>,
        arity_output: usize,
        provider: Rc<Box<dyn TableProvider>>,
    ) -> Self {
        Self {
            provider,
            bound_positions,
            arity_output,
        }
    }

    /// Materializes a [PartialTrieScan] into a list of [AnyDataValue] tuples.
    fn materialize_bindings<'a, Scan: PartialTrieScan<'a>>(
        trie_scan: Option<Scan>,
        dictionary: &'a RefCell<Dict>,
    ) -> Vec<Vec<AnyDataValue>> {
        let Some(trie_scan) = trie_scan else {
            return Vec::default();
        };

        let scan = RowScan::new_full(trie_scan);

        scan.map(|row| {
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
        .collect::<Vec<_>>()
    }

    /// Check whether a trie scan (option) has zero arity
    fn zero_arity_scan<'a, Scan: PartialTrieScan<'a>>(scan: &Option<Scan>) -> bool {
        scan.as_ref()
            .map(|scan| scan.arity() == 0)
            .unwrap_or_default()
    }

    /// Apply the operation to a [PartialTrieScan].
    pub(crate) async fn apply_operation<'a, Scan: PartialTrieScan<'a>>(
        self,
        mut trie_scans: Vec<(Option<Scan>, Option<Scan>)>,
        dictionary: &'a RefCell<Dict>,
    ) -> Result<Trie, ReadingError> {
        let Ok(provider) = Rc::try_unwrap(self.provider) else {
            panic!("invalid execution plan: import used in multiple places");
        };

        let mut tuple_writer = TupleWriter::new(dictionary, self.arity_output);
        let num_bindings = 0; // TODO: figure out how many bindings we have.

        let zero_arity_trie = trie_scans
            .iter()
            .any(|(old, new)| Self::zero_arity_scan(old) || Self::zero_arity_scan(new));
        let use_bindings =
            provider.should_import_with_bindings(&self.bound_positions, num_bindings);

        if zero_arity_trie || !use_bindings {
            log::info!("Loading data (without bindings): {provider}");
            provider.provide_table_data(&mut tuple_writer).await?
        } else if trie_scans.len() == 1 {
            let (_, trie_scan) = trie_scans.pop().expect("length is 1");
            let bound_positions = &self.bound_positions[0];

            let bindings = Self::materialize_bindings(trie_scan, dictionary);
            log::info!(
                "Loading data (with {} bindings): {provider}",
                bindings.len()
            );

            provider
                .provide_table_data_with_bindings(
                    &mut tuple_writer,
                    &ProductBindings::new(Bindings::new(bound_positions.clone(), bindings)),
                )
                .await?;
        } else {
            let mut bindings = Vec::default();

            for ((old, new), bound_positions) in
                trie_scans.into_iter().zip(self.bound_positions.iter())
            {
                let bindings_old = Self::materialize_bindings(old, dictionary);
                let bindings_new = Self::materialize_bindings(new, dictionary);

                bindings.push((
                    Bindings::new(bound_positions.clone(), bindings_old),
                    Bindings::new(bound_positions.clone(), bindings_new),
                ));
            }

            provider
                .provide_table_data_with_bindings(
                    &mut tuple_writer,
                    &ProductBindings::product(bindings),
                )
                .await?;
        }

        Ok(Trie::from_tuple_writer(tuple_writer))
    }
}
