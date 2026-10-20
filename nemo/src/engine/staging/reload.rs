//! Reloading: a changed program over the engine's stored data.
//!
//! The statements are diffed against the loaded ones,
//! the predicates whose definitions changed are rebuilt from their extensional blocks,
//! and everything else is kept.

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
        remote::{LoadKind, RemoteResponse, ResourceRequest, SuspendedLoad, Suspension},
        written::WrittenProgram,
    },
    model::{Directive, Identifier, Program, Statement},
};

/// What must be rebuilt, according to a reload's diff of the statements.
struct RebuildScope {
    /// Everything: a changed directive can change the meaning of any value.
    all: bool,
    /// Otherwise, the predicates whose defining statements changed.
    predicates: HashSet<String>,
}

/// How a reload's translation ended: done, or suspended for the remote
/// resources it needs.
enum Translated<T> {
    /// Translated.
    Done(T),
    /// Undone and suspended for these requests.
    Suspended(Vec<ResourceRequest>),
}

/// What a reload's translation leaves to apply:
/// the new program, its fact rows, the tables of the rebuilt predicates,
/// and the extensional blocks before the translation.
///
/// The rows added through `import_rows` in these blocks are reported when their tables are
/// dropped.
struct Retranslated {
    /// The new program.
    compiled: CompiledProgram,
    /// Its fact rows, by table.
    facts: HashMap<TableId, RowBuffer>,
    /// The rebuilt predicates' old tables, the scratch tables of bound imports
    /// included.
    replaced: Vec<(String, TableId)>,
    /// The extensional blocks before the translation.
    extensional: HashMap<TableId, Vec<ExtensionalBlock>>,
}

/// Removes the predicates a reload rebuilds from a translation state and the registered
/// predicates, returning their tables, the scratch tables of bound imports included.
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

/// Returns, per statement, whether its data is already stored:
/// a fact or an import of a predicate that stays registered,
/// its predicate name resolved by the translation state from before the reload.
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
    pub(in crate::engine) fn reload_program(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.abandon();
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
        let load = SuspendedLoad::new(LoadKind::Reload, staged);
        self.reload_staged(load, already)
    }

    /// Translates the written program of a staged replacement as
    /// [reload_staged](Self::reload_staged) first translates the program the stages made of it,
    /// reading no data and storing nothing.
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
    /// If remote resources are missing, it undoes the translation and suspends (N0491).
    /// The reports from `already` on are rewritten to refer to the written program.
    pub(in crate::engine) fn reload_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<(), Diagnostic> {
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
        let translated = self.pipeline.reports().len();
        let responses = load.responses();
        let previous = &old.column_types;
        let stages = self.rewriting_stages(&staged);
        let retranslated = self.reload_translation(
            &responses,
            &written,
            &new_ids,
            &new_statements,
            &rebuild,
            previous,
            &stages,
        );
        if matches!(retranslated, Ok(Translated::Suspended(_))) {
            self.pipeline.withdraw_reports(translated);
        }
        self.pipeline
            .present_reports(already, |report| written.present(report));
        let retranslated = match retranslated {
            Ok(Translated::Done(retranslated)) => retranslated,
            Ok(Translated::Suspended(requests)) => {
                self.program = Some(old);
                load.ask(requests, &mut self.request_ids);
                return Err(self.suspend(Suspension::Load(load)));
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
        Ok(())
    }

    /// Returns what a reload rebuilds, from the diff of the old and the new statements.
    ///
    /// The diff compares rendered statements.
    /// Term syntax parses back, so two statements are the same if they render the same,
    /// ignoring their spans.
    /// A statement whose renderings occur as often in the old as in the new program is
    /// unchanged.
    /// A predicate is rebuilt if a statement defining it changed,
    /// and everything is rebuilt if a changed directive can change the meaning of any value.
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
                // Rules recompile from the new program regardless.
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

    /// Unregisters the predicates a reload rebuilds, so that the new program registers them anew,
    /// returning their old tables, the scratch tables of bound imports included.
    ///
    /// The old tables are dropped only if the reload succeeds.
    fn unregister(&mut self, names: &[String]) -> Vec<(String, TableId)> {
        let replaced = unregister_from(&mut self.translation, &mut self.predicates, names);
        for (_, table) in &replaced {
            self.extensional.remove(table);
        }
        replaced
    }

    /// Translates the reloaded program against the kept predicates,
    /// whose facts and imports are preloaded: their rows are kept, and their files are not read
    /// again.
    ///
    /// A kept import may have been stored without columns or rows that the new rules read.
    /// The translation then fails with N0316 for it,
    /// and is repeated with that predicate rebuilt as well.
    /// A failed translation restores the previous state,
    /// and so does one that suspends for the remote resources it needs.
    /// Remote resources are read from the responses given so far.
    /// `previous` holds the column types the replaced program was analyzed with,
    /// and `stages` the pipeline stages that made the program.
    #[expect(clippy::too_many_arguments, reason = "the parts of one reload")]
    fn reload_translation(
        &mut self,
        responses: &HashMap<String, RemoteResponse>,
        written: &WrittenProgram,
        new_ids: &[crate::pipeline::StatementId],
        new_statements: &[Statement],
        rebuild: &RebuildScope,
        previous: &HashMap<TableId, Vec<Type>>,
        stages: &[&'static str],
    ) -> Result<Translated<Retranslated>, Diagnostic> {
        let mut unstable: HashSet<String> = HashSet::new();
        loop {
            let rebuilt = self.rebuilt_predicates(rebuild, &unstable);

            // Everything below must be undoable until translation succeeds.
            let saved_state = self.translation.clone();
            let saved_predicates = self.predicates.clone();
            let saved_extensional = self.extensional.clone();
            let replaced = self.unregister(&rebuilt);
            let preloaded = preloaded(new_statements, &saved_state, &self.predicates);

            let mut state = std::mem::take(&mut self.translation);
            state.clear_statement_state();
            let instant = self.loading_instant();
            let (stores, mut setup) = self.translation_parts(Purpose::of_load(stages), instant);
            setup.written = Some(written);
            setup.responses = Some(responses);
            setup.previous_types = Some(previous);
            let mut translator = translation::Translator::new(stores, setup, state, preloaded);
            let outcome = translator.program(new_statements, new_ids);
            let requests = translator.take_requests();
            let (state, facts) = translator.finish();
            self.translation = state;
            match outcome {
                Ok(compiled) if requests.is_empty() => {
                    return Ok(Translated::Done(Retranslated {
                        compiled,
                        facts,
                        replaced,
                        extensional: saved_extensional,
                    }));
                }
                outcome => {
                    // Undo the registrations of this translation and
                    // restore the previous state.
                    self.forget_registrations(&saved_predicates, &saved_state);
                    self.translation = saved_state;
                    self.extensional = saved_extensional;
                    if !requests.is_empty() {
                        return Ok(Translated::Suspended(requests));
                    }
                    let problem = outcome.expect_err("a translation that was not suspended failed");
                    if problem.code() == codes::PRUNING_INSTABILITY
                        && let Some(name) = problem.predicate()
                        && unstable.insert(name.to_owned())
                    {
                        continue;
                    }
                    return Err(problem);
                }
            }
        }
    }

    /// Returns the registered predicates a reload rebuilds:
    /// every one if the diff requires it, else the changed ones
    /// and those whose stored import no longer covers what the rules read.
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

    /// Drops the replaced tables, and returns every kept table to its extensional rows.
    ///
    /// The rows added through `import_rows` that the replaced tables held
    /// cannot be reproduced from statements, so dropping them is reported (N0485).
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

    /// Stores a reloaded program's data as a new extensional block.
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
