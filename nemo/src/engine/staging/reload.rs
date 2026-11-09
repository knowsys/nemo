//! Reloading: replacing the loaded program by a changed one, keeping the stored data that the
//! change does not affect.
//!
//! The new statements are compared with the loaded ones.
//! The predicates whose defining statements changed are *rebuilt*:
//! their tables are dropped, and the new program loads them anew.
//! Every other predicate keeps its input facts and loses its derived facts.

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{ColumnOrder, Revision, TableId},
    trie::RowBuffer,
    value::Type,
};

use crate::{
    codes,
    diagnostics::{AboutPredicate, EngineError},
    engine::{
        Engine,
        compile::{
            compiled::CompiledProgram,
            translation::{self, Purpose, TranslationState},
        },
        evaluate::extensional::{ExtensionalBlock, ExtensionalOrigin},
        registered::Registered,
        remote::{LoadKind, Progress, ResourceRequest, SuspendedLoad, Suspension},
        written::WrittenProgram,
    },
    model::{Directive, Identifier, Program, Statement},
};

/// What a reload must rebuild, according to its comparison of the statements.
struct RebuildScope {
    /// Whether every predicate is rebuilt, since a changed directive can change the meaning
    /// of any value.
    all: bool,
    /// Otherwise, the predicates whose defining statements changed.
    predicates: HashSet<String>,
}

/// How a reload's translation ended: done, or prepared and suspended for remote resources.
enum TranslationOutcome<T> {
    /// The translation is done, with this result.
    Done(T),
    /// The translation is prepared, with the engine as it was,
    /// and needs the responses to these requests.
    Missing(Vec<ResourceRequest>),
}

/// What a reload's translation leaves to apply to the engine.
struct Retranslated {
    /// The new compiled program.
    compiled: CompiledProgram,
    /// The rows of the new program's facts and imports, by table.
    facts: HashMap<TableId, RowBuffer>,
    /// The old tables of the rebuilt predicates,
    /// including the scratch tables of bound imports.
    replaced: Vec<(String, TableId)>,
    /// The extensional blocks from before the translation.
    ///
    /// They show which of the old tables hold rows added through
    /// [import_rows](crate::engine::Engine::import_rows).
    extensional: HashMap<TableId, Vec<ExtensionalBlock>>,
}

/// Removes the predicates named in `names` from a translation state and the registered
/// predicates, and returns their tables, including the scratch tables of bound imports.
fn unregister_from(
    state: &mut TranslationState,
    predicates: &mut HashMap<String, Registered>,
    names: &[String],
) -> Vec<(String, TableId)> {
    let mut replaced: Vec<(String, TableId)> = Vec::new();
    for name in names {
        if let Some(Registered { table, .. }) = predicates.remove(name) {
            state.seeds.remove(&table);
            state.plan.remove(name);
            state.open.remove(name);
            replaced.push((name.clone(), table));
        }
        if let Some(scratch) = state.scratch.remove(name) {
            replaced.push((name.clone(), scratch));
        }
    }
    replaced
}

/// Returns, per statement, whether its data is already stored,
/// which holds for a fact or an import of a predicate that stays registered.
///
/// The predicate names are resolved by `state`, the translation state from before the reload.
fn preloaded(
    statements: &[Statement],
    state: &TranslationState,
    predicates: &HashMap<String, Registered>,
) -> Vec<bool> {
    statements
        .iter()
        .map(|statement| {
            let predicate = match statement {
                Statement::Fact(fact) => &fact.predicate,
                Statement::Directive(Directive::Import(import)) => &import.predicate,
                _ => return false,
            };
            state
                .resolve(predicate)
                .is_some_and(|name| predicates.contains_key(&name))
        })
        .collect()
}

impl Engine {
    /// Runs the pipeline stages over a replacement program and rebuilds the stored data for it.
    pub(in crate::engine) fn reload_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        tracing::info!(
            statements = program.statements.len(),
            "reloading the program"
        );
        if self.program.is_none() {
            return Err(EngineError::NoProgram
                .diagnostic()
                .with_hint("call `load()` first; `reload()` replaces a loaded program"));
        }
        let already = self.pipeline.reports().len();
        let staged = self.stage(program)?;
        if !self.rewriting_stages(&staged).is_empty() {
            self.translate_written_reload(&staged)?;
        }
        let load = SuspendedLoad::new(LoadKind::Reload, staged, already);
        self.reload_staged(load, already)
    }

    /// Translates the written program of a staged replacement as
    /// [reload_staged](Self::reload_staged) first translates the program that the stages made
    /// of it, but reads no data and stores nothing.
    fn translate_written_reload(&mut self, staged: &super::Staged) -> Result<(), Diagnostic> {
        let held = self.revision.expect("a loaded program has a revision");
        let rebuild = self.rebuild_by_diff(
            &self.statements_of(held),
            &self.statements_of(staged.revision),
        );
        let rebuilt = self.rebuilt_predicates(&rebuild, &HashSet::new());
        let mut state = self.translation.clone();
        let mut predicates = self.predicates.clone();
        unregister_from(&mut state, &mut predicates, &rebuilt);
        let preloaded = preloaded(
            &self.statements_of(staged.written),
            &self.translation,
            &predicates,
        );
        state.clear_statement_state();
        self.translate_written(
            staged.written,
            state,
            predicates,
            preloaded,
            Purpose::Validate,
        )?;
        Ok(())
    }

    /// Returns the statements of a revision.
    fn statements_of(&self, revision: crate::pipeline::RevisionId) -> Vec<Statement> {
        self.pipeline
            .statements(revision)
            .map(|(_, statement)| statement.clone())
            .collect()
    }

    /// Translates a staged replacement and rebuilds the stored data.
    ///
    /// The translation is first prepared, which registers and stores nothing.
    /// If it needs remote resources that are not fetched yet, the reload is suspended for them
    /// with the loaded program as it was, and commits from the prepared translation state
    /// once every response is in.
    /// The reports from index `already` on are rewritten to refer to the written program.
    pub(in crate::engine) fn reload_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<Progress, Diagnostic> {
        load.take_responses();
        if load.slots.has_pending() {
            return Ok(self.suspend(Suspension::Load(load)));
        }
        let old = self.program.take().expect("a reload replaces a program");
        let held = self.revision.expect("a loaded program has a revision");
        let staged = load.staged;
        let written = WrittenProgram::new(&self.pipeline, staged.written, staged.revision);

        let old_statements = self.statements_of(held);
        let (new_ids, new_statements): (Vec<_>, Vec<Statement>) = self
            .pipeline
            .statements(staged.revision)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();
        let rebuild = self.rebuild_by_diff(&old_statements, &new_statements);
        let previous = &old.column_types;
        let stages = self.rewriting_stages(&staged);
        let retranslated = self.reload_translation(
            &mut load,
            &written,
            &new_ids,
            &new_statements,
            &rebuild,
            previous,
            &stages,
        );
        self.pipeline
            .present_reports(already, |report| written.present(report));
        let retranslated = match retranslated {
            Ok(TranslationOutcome::Done(retranslated)) => retranslated,
            Ok(TranslationOutcome::Missing(missing)) => {
                self.program = Some(old);
                load.add_requests(missing, &mut self.request_ids);
                return Ok(self.suspend(Suspension::Load(load)));
            }
            Err(problem) => {
                self.program = Some(old);
                return Err(written.present(problem));
            }
        };
        self.replace_store(retranslated.replaced, &retranslated.extensional);
        #[cfg(debug_assertions)]
        for (&table, rows) in &retranslated.facts {
            self.assert_stored_types(&retranslated.compiled.column_types, table, rows);
        }
        self.store_reloaded(retranslated.facts);
        self.reset_counters();
        self.skeletons.clear();
        self.program = Some(retranslated.compiled);
        self.revision = Some(staged.revision);
        self.written = Some(written);
        Ok(Progress::Done)
    }

    /// Returns what a reload rebuilds, by comparing the old and the new statements.
    ///
    /// The comparison is between rendered statements.
    /// A rendered statement parses back to the same statement,
    /// so two statements are the same, apart from their spans, if they render the same.
    /// A statement is unchanged if its rendering occurs as often in the old program as in
    /// the new one.
    /// A predicate is rebuilt if a statement defining it changed,
    /// and every predicate is rebuilt if a changed directive can change the meaning of any
    /// value, or if a changed statement names a predicate that cannot be resolved.
    fn rebuild_by_diff(
        &self,
        old_statements: &[Statement],
        new_statements: &[Statement],
    ) -> RebuildScope {
        let mut net: HashMap<String, i64> = HashMap::new();
        for statement in old_statements {
            *net.entry(statement.to_string()).or_default() += 1;
        }
        for statement in new_statements {
            *net.entry(statement.to_string()).or_default() -= 1;
        }
        let changed =
            |statement: &Statement| net.get(&statement.to_string()).copied().unwrap_or(0) != 0;

        let mut rebuild = RebuildScope {
            all: false,
            predicates: HashSet::new(),
        };
        let touch = |predicate: &Identifier, rebuild: &mut RebuildScope| match self
            .translation
            .resolve(predicate)
        {
            Some(name) => {
                rebuild.predicates.insert(name);
            }
            None => rebuild.all = true,
        };
        for statement in old_statements.iter().chain(new_statements) {
            if !changed(statement) {
                continue;
            }
            match statement {
                Statement::Fact(fact) => touch(&fact.predicate, &mut rebuild),
                // Rules are compiled from the new program anyway.
                Statement::Rule(_) => {}
                Statement::Directive(directive) => match directive {
                    Directive::Import(import) => touch(&import.predicate, &mut rebuild),
                    Directive::Declare(declare) => touch(&declare.predicate, &mut rebuild),
                    Directive::Input(input) => touch(&input.predicate, &mut rebuild),
                    // These store no rows.
                    Directive::Output(_) | Directive::Export(_) => {}
                    _ => rebuild.all = true,
                },
            }
        }
        rebuild
    }

    /// Unregisters the predicates named in `names`, so that the new program registers them
    /// anew, and returns their old tables, including the scratch tables of bound imports.
    ///
    /// It also forgets the extensional blocks of these tables.
    /// The old tables themselves are dropped only if the reload succeeds.
    fn unregister(&mut self, names: &[String]) -> Vec<(String, TableId)> {
        let replaced = unregister_from(&mut self.translation, &mut self.predicates, names);
        for (_, table) in &replaced {
            self.extensional.remove(table);
        }
        replaced
    }

    /// Translates the new program against the kept predicates,
    /// whose facts and imports are preloaded:
    /// their rows are kept, and their resources are not read again.
    ///
    /// A kept import may have been stored without columns or rows that the new rules read.
    /// The translation then fails with [PRUNING_INSTABILITY](crate::codes::PRUNING_INSTABILITY)
    /// for its predicate, and is repeated with that predicate rebuilt as well.
    /// A failed translation restores the previous state.
    /// So does a prepared translation that needs remote resources that are not fetched yet.
    /// `load` then keeps the prepared translation state and the predicates rebuilt so far,
    /// and the next call commits from them.
    /// The remote resources are read from those that `load` fetched,
    /// and a repetition reads them again.
    /// `previous` gives the column types the replaced program was analyzed with,
    /// and `stages` the pipeline stages that made the program.
    #[expect(clippy::too_many_arguments, reason = "the parts of one reload")]
    fn reload_translation(
        &mut self,
        load: &mut SuspendedLoad,
        written: &WrittenProgram,
        new_ids: &[crate::pipeline::StatementId],
        new_statements: &[Statement],
        rebuild: &RebuildScope,
        previous: &HashMap<TableId, Vec<Type>>,
        stages: &[&'static str],
    ) -> Result<TranslationOutcome<Retranslated>, Diagnostic> {
        let mut unstable: HashSet<String> = std::mem::take(&mut load.unstable);
        loop {
            let rebuilt = self.rebuilt_predicates(rebuild, &unstable);

            // Everything below must be undoable until translation succeeds.
            let saved_state = self.translation.clone();
            let saved_predicates = self.predicates.clone();
            let saved_extensional = self.extensional.clone();
            let replaced = self.unregister(&rebuilt);
            let preloaded = preloaded(new_statements, &saved_state, &self.predicates);
            let context = self.loading_context();

            let state = match load.prepared.take() {
                Some(state) => state,
                None => {
                    let mut state = self.translation.clone();
                    state.clear_statement_state();
                    let (stores, mut setup) =
                        self.translation_parts(Purpose::of_load(stages), context);
                    setup.written = Some(written);
                    setup.previous_types = Some(previous);
                    let mut translator =
                        translation::Translator::new(stores, setup, state, preloaded.clone());
                    let requests = translator.prepare(new_statements, new_ids);
                    let (state, _) = translator.finish();
                    match requests.map(|requests| load.missing(requests)) {
                        Ok(missing) if missing.is_empty() => state,
                        outcome => {
                            // Nothing was registered or stored, but the rebuilt
                            // predicates were unregistered.
                            self.forget_registrations(&saved_predicates, &saved_state);
                            self.translation = saved_state;
                            self.extensional = saved_extensional;
                            match outcome {
                                Ok(missing) => {
                                    load.prepared = Some(state);
                                    load.unstable = unstable;
                                    return Ok(TranslationOutcome::Missing(missing));
                                }
                                Err(problem) => {
                                    if Self::unstable_import(&problem, &mut unstable) {
                                        continue;
                                    }
                                    return Err(problem);
                                }
                            }
                        }
                    }
                }
            };

            let (stores, mut setup) = self.translation_parts(Purpose::of_load(stages), context);
            setup.written = Some(written);
            setup.previous_types = Some(previous);
            let mut translator = translation::Translator::new(stores, setup, state, preloaded);
            // A repetition reads the fetched resources again.
            let outcome = translator.commit(new_statements, new_ids, load.fetched.clone());
            let (state, facts) = translator.finish();
            self.translation = state;
            match outcome {
                Ok(compiled) => {
                    return Ok(TranslationOutcome::Done(Retranslated {
                        compiled,
                        facts,
                        replaced,
                        extensional: saved_extensional,
                    }));
                }
                Err(problem) => {
                    // Undo the registrations of this translation and
                    // restore the previous state.
                    self.forget_registrations(&saved_predicates, &saved_state);
                    self.translation = saved_state;
                    self.extensional = saved_extensional;
                    if Self::unstable_import(&problem, &mut unstable) {
                        continue;
                    }
                    return Err(problem);
                }
            }
        }
    }

    /// Returns whether a reload's translation failed because a stored import does not cover
    /// what the new rules read ([PRUNING_INSTABILITY](crate::codes::PRUNING_INSTABILITY)),
    /// for a predicate that is not rebuilt yet.
    ///
    /// If so, the predicate is added to `unstable`, the predicates to rebuild for this reason.
    fn unstable_import(problem: &Diagnostic, unstable: &mut HashSet<String>) -> bool {
        problem.code() == codes::PRUNING_INSTABILITY
            && problem
                .predicate()
                .is_some_and(|name| unstable.insert(name.to_owned()))
    }

    /// Returns the registered predicates that a reload rebuilds.
    ///
    /// They are every registered predicate if `rebuild` says so.
    /// Otherwise, they are the changed predicates and those in `unstable`,
    /// whose stored import does not cover what the new rules read.
    fn rebuilt_predicates(
        &self,
        rebuild: &RebuildScope,
        unstable: &HashSet<String>,
    ) -> Vec<String> {
        if rebuild.all {
            return self.predicates.keys().cloned().collect();
        }
        rebuild
            .predicates
            .iter()
            .chain(unstable)
            .filter(|name| self.predicates.contains_key(*name))
            .cloned()
            .collect()
    }

    /// Drops the replaced tables, and the derived rows of every kept table.
    ///
    /// `extensional` gives the extensional blocks from before the reload.
    /// The rows that were added through [import_rows](crate::engine::Engine::import_rows) to a
    /// replaced table cannot be reproduced from statements,
    /// so dropping them is reported with a warning.
    fn replace_store(
        &mut self,
        replaced: Vec<(String, TableId)>,
        extensional: &HashMap<TableId, Vec<ExtensionalBlock>>,
    ) {
        for (name, table) in replaced {
            if extensional
                .get(&table)
                .into_iter()
                .flatten()
                .any(|block| block.origin == ExtensionalOrigin::ImportRows)
            {
                self.pipeline
                    .report(EngineError::ImportedRowsDropped { predicate: name }.diagnostic());
            }
            self.manager.drop_table(table);
        }
        let tables: Vec<TableId> = self
            .predicates
            .values()
            .map(|&Registered { table, .. }| table)
            .collect();
        for table in tables {
            self.drop_derived(table);
        }
    }

    /// Stores the rows of the new program's facts and imports at one new revision,
    /// as an extensional block of each table that gains rows.
    fn store_reloaded(&mut self, facts: HashMap<TableId, RowBuffer>) {
        let revision = Revision(self.step.max(1));
        let mut grew = false;
        for (table, rows) in facts {
            let arity = rows.arity();
            let stored = self
                .manager
                .add_rows(table, revision, ColumnOrder::identity(arity), rows);
            if stored > 0 {
                grew = true;
                self.record_extensional(table, revision, ExtensionalOrigin::Load);
            }
        }
        self.step = self.step.max(1);
        if grew {
            self.step += 1;
        }
    }

    /// Resets the counters and the application log after a reload:
    /// everything stored is loaded, nothing is derived,
    /// no table is marked as having gained input facts,
    /// and the log of rule applications is empty.
    fn reset_counters(&mut self) {
        self.loaded = 0;
        for &Registered { table, .. } in self.predicates.values() {
            self.loaded += self
                .manager
                .row_count(table, Revision(0)..Revision(self.step));
        }
        self.derived = 0;
        self.dirty.clear();
        self.fixpoint = false;
        self.applications.clear();
        self.contexts.clear();
        self.derivation_time.clear();
    }
}
