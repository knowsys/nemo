//! Adding rows through [import_rows](crate::engine::Engine::import_rows).

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    management::{Revision, TableId},
    value::{DataValue, Type},
};

use crate::engine::{
    Engine,
    compile::translation::{Purpose, Translator},
    compile::types,
    evaluate::extensional::ExtensionalOrigin,
    imported_rows::RowBlock,
};

impl Engine {
    /// Adds rows given through [import_rows](Engine::import_rows) to a predicate:
    /// as a block for the first load before one, and as a new revision after one.
    ///
    /// The loaded program was analyzed with the types that its columns could hold.
    /// If the rows stay within the inferred types of the predicate's columns,
    /// that analysis still holds.
    /// If they widen these types, the whole program is analyzed again,
    /// and the rows are added only if the program still keeps every declaration.
    pub(in crate::engine) fn admit_rows(
        &mut self,
        predicate: &str,
        rows: impl IntoIterator<Item = Vec<DataValue>>,
    ) -> Result<(), Diagnostic> {
        if self.program.is_none() {
            let arity = self.unloaded_rows.arity(predicate);
            if let Some(block) = RowBlock::read(predicate, rows, arity)? {
                self.unloaded_rows.add(block)?;
            }

            return Ok(());
        }

        let table = self
            .written_predicate(predicate)
            .map_err(|unknown| {
                unknown.with_hint("first `extend()` the program with statements that use it")
            })?
            .table;

        // The translation checks the block's number of values against the program.
        let Some(block) = RowBlock::read(predicate, rows, self.arity_of(predicate))? else {
            return Ok(());
        };

        // The rows are checked as facts of the program are (declarations, the element types
        // that `@type` declares, encoding), against the kept translation state.
        // A refused call leaves the seeds as they were.
        let seeds_before = self.translation.seeds.get(&table).cloned();
        let state = std::mem::take(&mut self.translation);
        let context = self.loading_context();
        let (stores, setup) = self.translation_parts(Purpose::Load, context);
        let mut translator = Translator::new(stores, setup, state, Vec::new());
        let outcome = translator.imported_rows(&block);
        let (state, _) = translator.finish();
        self.translation = state;
        let rows = match outcome {
            Ok(encoded) => encoded.expect("a load reads data").1,
            Err(problem) => {
                self.restore_seeds(table, seeds_before);
                return Err(problem);
            }
        };
        if !self.seeds_within_inferred(table) {
            match self.reanalyze() {
                Ok(column_types) => {
                    if let Some(program) = &mut self.program {
                        program.column_types = column_types;
                    }
                }
                Err(problem) => {
                    self.restore_seeds(table, seeds_before);
                    return Err(problem.with_note(format!(
                        "the rows added through `import_rows` widen `{predicate}`"
                    )));
                }
            }
        }
        if let Some(program) = &self.program {
            self.assert_stored_types(&program.column_types, table, &rows);
        }

        let revision = Revision(self.step);
        self.step += 1;
        let grown = self.store_input([(table, rows)], revision, ExtensionalOrigin::ImportRows);
        if !grown.is_empty() {
            self.dirty.insert(table);
            self.fixpoint = false;
        }

        Ok(())
    }

    /// Returns whether a table's seeds lie within its inferred column types,
    /// so that the analysis the program was loaded with still holds.
    ///
    /// The seeds are the column types that the table's input rows give it.
    fn seeds_within_inferred(&self, table: TableId) -> bool {
        let (Some(program), Some(seeds)) = (&self.program, self.translation.seeds.get(&table))
        else {
            return true;
        };

        program.column_types.get(&table).is_some_and(|inferred| {
            seeds
                .iter()
                .zip(inferred)
                .all(|(seed, inferred)| seed.is_subset(inferred, &self.translation.label_types))
        })
    }

    /// Restores a table's seeds to what they were before a refused call.
    fn restore_seeds(&mut self, table: TableId, seeds: Option<Vec<Type>>) {
        match seeds {
            Some(seeds) => {
                self.translation.seeds.insert(table, seeds);
            }
            None => {
                self.translation.seeds.remove(&table);
            }
        }
    }

    /// Analyzes the loaded program again over the kept translation state.
    ///
    /// Returns the inferred column types,
    /// or the error for the first declaration that the analysis cannot show the program keeps.
    fn reanalyze(&self) -> Result<HashMap<TableId, Vec<Type>>, Diagnostic> {
        let program = self
            .program
            .as_ref()
            .expect("rows widen the types of a loaded program");
        let sources: Vec<types::RuleSourceInfo<'_>> = program
            .sources
            .iter()
            .map(|source| types::RuleSourceInfo {
                span: None,
                names: &source.names,
            })
            .collect();
        let inputs = types::AnalysisInputs {
            predicates: &self.predicates,
            seeds: &self.translation.seeds,
            declared: types::Declared {
                predicates: &self.translation.declarations,
                labels: &self.translation.label_types,
            },
            open: &self.translation.open,
            plan: &self.translation.plan,
            previous: Some(&program.column_types),
        };
        let types::Analysis {
            column_types,
            findings,
            ..
        } = types::analyze(&program.rules, &sources, &inputs);
        let error = findings
            .into_iter()
            .find(|finding| finding.diagnostic.severity() == Severity::Error);
        match error {
            None => Ok(column_types),
            Some(types::TypeFinding {
                rule: Some(rule),
                diagnostic,
            }) => Err(self
                .pipeline
                .label_instances(diagnostic, program.sources[rule].statement)),
            Some(types::TypeFinding { diagnostic, .. }) => Err(diagnostic),
        }
    }
}
