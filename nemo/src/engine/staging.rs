//! Loading, extending, reloading and checking a program,
//! and adding rows through [import_rows](crate::engine::Engine::import_rows).
//!
//! A loading method first *stages* its program: it adds the program to the pipeline as the
//! written revision, and runs the pipeline stages over it.

mod reload;

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    management::{ColumnOrder, Revision, TableId},
    trie::RowBuffer,
    value::{DataValue, Type},
};

use crate::{
    diagnostics::EngineError,
    engine::{
        CheckReport, Engine, InferredTypes,
        check::rewrite_unvalued,
        compile::compiled::CompiledProgram,
        compile::translation::{self, Purpose, TranslationState},
        compile::types,
        evaluate::extensional::ExtensionalOrigin,
        imported_rows::{RowBlock, UnloadedRows},
        registered::Registered,
        remote::{LoadKind, Progress, SuspendedLoad, Suspension},
        transform::sparql_rewrite,
        written::WrittenProgram,
    },
    model::{Directive, Program, Statement},
    pipeline::{NameScope, RevisionId, Stages, StatementId},
};

/// A program staged in the pipeline: its written revision,
/// and the revision that the pipeline stages made of it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Staged {
    /// The written revision.
    pub(super) written: RevisionId,
    /// The revision that is compiled.
    pub(super) revision: RevisionId,
}

/// The pipeline stages of an engine: its own,
/// and those added through [stages_mut](Engine::stages_mut).
#[derive(Debug, Clone)]
pub(crate) struct EngineStages(pub(crate) Stages);

impl Default for EngineStages {
    /// Returns the engine's own pipeline stages.
    fn default() -> Self {
        let mut stages = Stages::new();
        stages.push(sparql_rewrite::SparqlRewrite);
        Self(stages)
    }
}

/// Fails for a program that is not linked, which is a program with a `@use` directive.
///
/// [link](Engine::link) instantiates these directives.
fn expect_linked(program: &Program) -> Result<(), Diagnostic> {
    let using = program
        .statements
        .iter()
        .find_map(|statement| match statement {
            Statement::Directive(Directive::Use(using)) => Some(using),
            _ => None,
        });
    match using {
        None => Ok(()),
        Some(using) => {
            let mut problem = EngineError::NotLinked.diagnostic();
            if let Some(span) = using.span {
                problem = problem.with_label(span, "this module is not linked");
            }
            Err(problem)
        }
    }
}

impl Engine {
    /// Runs the pipeline stages over a first program and loads it.
    pub(in crate::engine) fn load_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        if self.program.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic().with_hint(
                "use `extend()` to add to the loaded program, or `reload()` to replace it",
            ));
        }

        tracing::info!(statements = program.statements.len(), "loading a program");

        // The stages are told which predicates have rows added through `import_rows`,
        // which no statement shows.
        // The translation checks the rows after the program's statements, as it checks facts.
        let already = self.pipeline.reports().len();
        let imported = self.imported_predicates();
        let staged = self.stage(program, &imported)?;
        if !self.rewriting_stages(&staged).is_empty() {
            let predicates = self.predicates.clone();
            self.translate_written(
                staged.written,
                TranslationState::default(),
                predicates,
                Vec::new(),
                Purpose::Validate,
            )?;
        }
        let load = SuspendedLoad::new(LoadKind::Load, staged, already);
        self.load_staged(load, already)
    }

    /// Translates a staged first program and stores its data.
    ///
    /// The translation is first prepared, which registers and stores nothing.
    /// If it needs remote resources that are not fetched yet, the load is suspended for them
    /// with the prepared translation state, and commits from that state once every response
    /// is in.
    /// A load that fails drops the tables it registered and restores the translation state,
    /// so that the engine is as it was before the load.
    /// The reports from index `already` on are rewritten to refer to the written program.
    pub(super) fn load_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<Progress, Diagnostic> {
        load.take_responses();
        if load.slots.has_pending() {
            return Ok(self.suspend(Suspension::Load(load)));
        }
        let revision = load.staged.revision;
        let written = WrittenProgram::new(&self.pipeline, load.staged.written, revision);
        let (ids, statements): (Vec<_>, Vec<_>) = self
            .pipeline
            .statements(revision)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();

        let stages = self.rewriting_stages(&load.staged);
        // Everything below must be undoable until the translation succeeds.
        let saved = self.translation.clone();
        let saved_predicates = self.predicates.clone();
        let context = self.loading_context();
        let (stores, mut setup) = self.translation_parts(Purpose::of_load(&stages), context);
        setup.written = Some(&written);
        let prepared = load.prepared.take();
        let resumed = prepared.is_some();
        let mut translator =
            translation::Translator::new(stores, setup, prepared.unwrap_or_default(), Vec::new());
        let requests = if resumed {
            Ok(Vec::new())
        } else {
            translator
                .prepare(&statements, &ids)
                .map(|requests| load.missing(requests))
        };
        let outcome = match &requests {
            Ok(missing) if missing.is_empty() => {
                Some(translator.commit(&statements, &ids, std::mem::take(&mut load.fetched)))
            }
            _ => None,
        };
        let imported = translator.take_imported_rows();
        let (state, facts) = translator.finish();
        match requests {
            Ok(missing) if !missing.is_empty() => {
                // The preparation registered and stored nothing,
                // so the load is suspended with its state.
                self.pipeline
                    .present_reports(already, |report| written.present(report));
                load.prepared = Some(state);
                load.add_requests(missing, &mut self.request_ids);
                return Ok(self.suspend(Suspension::Load(load)));
            }
            Ok(_) => {}
            Err(problem) => {
                self.translation = state;
                self.forget_registrations(&saved_predicates, &saved);
                self.translation = saved;
                self.pipeline
                    .present_reports(already, |report| written.present(report));
                return Err(written.present(problem));
            }
        }
        self.translation = state;
        self.pipeline
            .present_reports(already, |report| written.present(report));
        let compiled = match outcome.expect("a prepared load with every resource fetched commits") {
            Ok(compiled) => compiled,
            Err(problem) => {
                self.forget_registrations(&saved_predicates, &saved);
                self.translation = saved;
                return Err(written.present(problem));
            }
        };

        for (table, rows) in facts {
            #[cfg(debug_assertions)]
            self.assert_stored_types(&compiled.column_types, table, &rows);
            let arity = rows.arity();
            let stored =
                self.manager
                    .add_rows(table, Revision(0), ColumnOrder::identity(arity), rows);
            if stored > 0 {
                self.record_extensional(table, Revision(0), ExtensionalOrigin::Load);
            }
        }
        // The rows added through `import_rows` get a revision of their own, apart from the
        // program's facts, as such rows do after a load.
        self.step = 1;
        if !imported.is_empty() {
            for (table, rows) in imported {
                #[cfg(debug_assertions)]
                self.assert_stored_types(&compiled.column_types, table, &rows);
                let arity = rows.arity();
                let stored =
                    self.manager
                        .add_rows(table, Revision(1), ColumnOrder::identity(arity), rows);
                if stored > 0 {
                    self.record_extensional(table, Revision(1), ExtensionalOrigin::ImportRows);
                }
            }
            self.step = 2;
        }
        self.unloaded_rows = UnloadedRows::default();
        // Everything stored so far was loaded, not derived.
        for &Registered { table, .. } in self.predicates.values() {
            self.loaded += self
                .manager
                .row_count(table, Revision(0)..Revision(self.step));
        }
        tracing::debug!(
            rules = compiled.rules.len(),
            strata = compiled.strata.len(),
            predicates = self.predicates.len(),
            loaded = self.loaded,
            "program loaded"
        );

        self.skeletons.clear();
        self.program = Some(compiled);
        self.revision = Some(revision);
        self.written = Some(written);
        Ok(Progress::Done)
    }

    /// Stages a program: adds it to the pipeline as the written revision,
    /// and runs the pipeline stages over it.
    ///
    /// `imported` names the predicates with rows added through
    /// [import_rows](Engine::import_rows), which the stages see only by name.
    /// Fails for a program that is not linked, and for a problem a stage reports.
    pub(super) fn stage(
        &mut self,
        program: Program,
        imported: &[String],
    ) -> Result<Staged, Diagnostic> {
        expect_linked(&program)?;
        let written = self.pipeline.load(program);
        let revision = self
            .pipeline
            .apply_imported(&self.stages.0, written, imported)?;
        Ok(Staged { written, revision })
    }

    /// Returns the pipeline stages that changed a staged program, in order,
    /// or none if the compiled revision has the same statements as the written one.
    pub(super) fn rewriting_stages(&self, staged: &Staged) -> Vec<&'static str> {
        let same = |left: RevisionId, right: RevisionId| {
            let ids = |revision| self.pipeline.statements(revision).map(|(id, _)| id);
            ids(left).eq(ids(right))
        };
        if same(staged.written, staged.revision) {
            return Vec::new();
        }
        let mut stages = Vec::new();
        for pair in self.pipeline.lineage(staged.revision).windows(2) {
            if let Some(stage) = self.pipeline.stage(pair[1])
                && !stages.contains(&stage)
                && !same(pair[0], pair[1])
            {
                stages.push(stage);
            }
        }
        stages
    }

    /// Translates a written program as its load would, but reads no data and stores nothing,
    /// so that its problems are reported against the statements as written.
    ///
    /// The translation starts from the given state and registered predicates,
    /// and `preloaded` says, per statement, whether its data is already stored.
    /// `purpose` is [Purpose::Validate] or [Purpose::Check].
    /// The type analysis and the stratification are thus checked on the written statements.
    /// The translation of the program that the pipeline stages made of them then reports only
    /// what reading the data finds.
    /// For a check, the parameters without a value are first rewritten into inputs
    /// (see [rewrite_unvalued]).
    ///
    /// Returns the compiled program and the predicates registered for it.
    pub(super) fn translate_written(
        &mut self,
        written: RevisionId,
        state: TranslationState,
        predicates: HashMap<String, Registered>,
        preloaded: Vec<bool>,
        purpose: Purpose<'_>,
    ) -> Result<(CompiledProgram, HashMap<String, Registered>), Diagnostic> {
        let statements: Vec<(StatementId, Statement)> = self
            .pipeline
            .statements(written)
            .map(|(id, statement)| (id, statement.clone()))
            .collect();
        let statements = if purpose == Purpose::Check {
            // The rewrite may leave statements out, so `preloaded` would not match them.
            debug_assert!(preloaded.is_empty(), "a check preloads nothing");
            rewrite_unvalued(statements)
        } else {
            statements
        };
        let (ids, statements): (Vec<_>, Vec<_>) = statements.into_iter().unzip();
        let mut scratch = self.scratch_stores(predicates);
        let (stores, setup) = self.scratch_translation_parts(purpose, &mut scratch);
        let mut translator = translation::Translator::new(stores, setup, state, preloaded);
        let compiled = translator.program(&statements, &ids)?;
        Ok((compiled, scratch.into_predicates()))
    }

    /// Stages an extension: checks it, and appends it to `held`, the loaded revision.
    ///
    /// The check fails for an extension that is not linked,
    /// or that changes the `@base`, a prefix or a parameter of the loaded program.
    /// The pipeline stages then run over the whole program without rewriting the loaded
    /// statements.
    /// So a rule of the extension that reads a loaded import is rewritten with the loaded
    /// program in view, and the names that the stages introduce are new to the whole program.
    fn stage_extension(
        &mut self,
        program: Program,
        held: RevisionId,
    ) -> Result<Staged, Diagnostic> {
        expect_linked(&program)?;
        let addition = self.pipeline.load(program);
        self.check_extension(addition)?;
        let frozen = self.pipeline.statements(held).count();
        let merged = self.pipeline.merge(held, addition);
        let held_written = self
            .written
            .as_ref()
            .expect("a loaded program has a written program")
            .revision();
        let written = self.pipeline.merge(held_written, addition);
        let merged = self.revive_sparql_imports(held, merged);
        let imported = self.imported_predicates();
        let revision = self
            .pipeline
            .extend(&self.stages.0, merged, frozen, &imported)?;
        Ok(Staged { written, revision })
    }

    /// Restores the SPARQL imports without a `where` clause that the `sparql` stage removed
    /// from the loaded program because no rule read them, so that the extension's rules can
    /// read them.
    ///
    /// `held` is the loaded revision, and `merged` the revision with the extension appended.
    /// Returns the revision with the restored imports, or `merged` if there are none.
    /// The stage removes them again if they stay unread.
    fn revive_sparql_imports(&mut self, held: RevisionId, merged: RevisionId) -> RevisionId {
        let root = self.pipeline.lineage(held)[0];
        let live: HashSet<StatementId> = self.pipeline.statements(held).map(|(id, _)| id).collect();
        let revived: Vec<(StatementId, Statement)> = self
            .pipeline
            .statements(root)
            .filter(|(id, statement)| {
                !live.contains(id) && sparql_rewrite::is_plain_sparql_import(statement)
            })
            .map(|(id, statement)| (id, statement.clone()))
            .collect();
        if revived.is_empty() {
            return merged;
        }
        let mut commit = self.pipeline.commit(merged, "extension");
        for (id, statement) in revived {
            commit.derive(&[id], statement);
        }
        commit.finish()
    }

    /// Undoes what a failed or suspended translation registered.
    ///
    /// `predicates` and `state` are the registered predicates and the translation state from
    /// before the translation.
    /// Every table registered since, for a predicate or as the scratch table of a bound import,
    /// is dropped, and the registered predicates are reset to `predicates`.
    fn forget_registrations(
        &mut self,
        predicates: &HashMap<String, Registered>,
        state: &TranslationState,
    ) {
        let stale: Vec<TableId> = self
            .predicates
            .values()
            .map(|registered| registered.table)
            .filter(|table| {
                !predicates
                    .values()
                    .any(|&Registered { table: kept, .. }| kept == *table)
            })
            .chain(
                self.translation
                    .scratch
                    .iter()
                    .filter(|(name, table)| state.scratch.get(*name) != Some(*table))
                    .map(|(_, &table)| table),
            )
            .collect();
        for table in stale {
            self.manager.drop_table(table);
        }
        self.predicates = predicates.clone();
    }

    /// Runs the pipeline stages over an extension and loads it.
    pub(in crate::engine) fn extend_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        tracing::info!(
            statements = program.statements.len(),
            "extending the program"
        );
        if self.program.is_none() {
            return Err(EngineError::NoProgram
                .diagnostic()
                .with_hint("call `load()` first; `extend()` adds to a loaded program"));
        }
        let held = self.revision.expect("a loaded program has a revision");
        let already = self.pipeline.reports().len();
        let staged = self.stage_extension(program, held)?;
        if !self.rewriting_stages(&staged).is_empty() {
            self.translate_written_extension(&staged)?;
        }
        let load = SuspendedLoad::new(LoadKind::Extend, staged, already);
        self.extend_staged(load, already)
    }

    /// Translates the written program of a staged extension as
    /// [extend_staged](Self::extend_staged) translates the program that the stages made of it,
    /// but reads no data and stores nothing.
    fn translate_written_extension(&mut self, staged: &Staged) -> Result<(), Diagnostic> {
        let held_written = self
            .written
            .as_ref()
            .expect("a loaded program has a written program")
            .revision();
        let mut preloaded = vec![true; self.pipeline.statements(held_written).count()];
        preloaded.resize(self.pipeline.statements(staged.written).count(), false);
        let mut state = self.translation.clone();
        state.clear_statement_state();
        let predicates = self.predicates.clone();
        self.translate_written(
            staged.written,
            state,
            predicates,
            preloaded,
            Purpose::Validate,
        )?;
        Ok(())
    }

    /// Translates a staged extension and stores its data.
    ///
    /// The translation is first prepared, which registers and stores nothing,
    /// so the loaded program stays as it is while the extension is suspended for remote
    /// resources that are not fetched yet.
    /// It commits from the prepared translation state once every response is in.
    /// The reports from index `already` on are rewritten to refer to the written program.
    pub(super) fn extend_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<Progress, Diagnostic> {
        load.take_responses();
        if load.slots.has_pending() {
            return Ok(self.suspend(Suspension::Load(load)));
        }
        let held = self.revision.expect("a loaded program has a revision");
        let merged = load.staged.revision;
        let written = WrittenProgram::new(&self.pipeline, load.staged.written, merged);
        let (ids, statements): (Vec<_>, Vec<Statement>) = self
            .pipeline
            .statements(merged)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();
        let mut preloaded = vec![true; self.pipeline.statements(held).count()];
        preloaded.resize(statements.len(), false);
        let stages = self.rewriting_stages(&load.staged);

        // The part of the state that statements determine is rebuilt from the full list of
        // statements. The part that stored data determines is kept,
        // since the facts and imports that are not read again cannot rebuild it.
        let state = match load.prepared.take() {
            Some(state) => state,
            None => {
                let mut state = self.translation.clone();
                state.clear_statement_state();
                let context = self.loading_context();
                let (stores, mut setup) =
                    self.translation_parts(Purpose::of_load(&stages), context);
                setup.written = Some(&written);
                let mut translator =
                    translation::Translator::new(stores, setup, state, preloaded.clone());
                let requests = translator.prepare(&statements, &ids);
                let (state, _) = translator.finish();
                let missing = match requests {
                    Ok(requests) => load.missing(requests),
                    Err(problem) => {
                        self.pipeline
                            .present_reports(already, |report| written.present(report));
                        return Err(written.present(problem));
                    }
                };
                if !missing.is_empty() {
                    self.pipeline
                        .present_reports(already, |report| written.present(report));
                    load.prepared = Some(state);
                    load.add_requests(missing, &mut self.request_ids);
                    return Ok(self.suspend(Suspension::Load(load)));
                }
                state
            }
        };

        let old = self.program.take().expect("an extension extends a program");
        let saved = self.translation.clone();
        let saved_predicates = self.predicates.clone();
        let context = self.loading_context();
        let (stores, mut setup) = self.translation_parts(Purpose::of_load(&stages), context);
        setup.written = Some(&written);
        setup.previous_types = Some(&old.column_types);
        let mut translator = translation::Translator::new(stores, setup, state, preloaded);
        let outcome = translator.commit(&statements, &ids, std::mem::take(&mut load.fetched));
        let (state, facts) = translator.finish();
        self.translation = state;
        let mut compiled = match outcome {
            Ok(compiled) => compiled,
            Err(problem) => {
                // Undo the registrations of this translation and restore
                // the previous state.
                self.forget_registrations(&saved_predicates, &saved);
                self.translation = saved;
                self.program = Some(old);
                self.pipeline
                    .present_reports(already, |report| written.present(report));
                return Err(written.present(problem));
            }
        };
        self.pipeline
            .present_reports(already, |report| written.present(report));
        #[cfg(debug_assertions)]
        for (&table, rows) in &facts {
            self.assert_stored_types(&compiled.column_types, table, rows);
        }
        self.store_extension(facts);
        self.adopt_extension(&mut compiled, &old);
        self.skeletons.clear();
        self.program = Some(compiled);
        self.revision = Some(merged);
        self.written = Some(written);
        self.fixpoint = false;
        Ok(Progress::Done)
    }

    /// Stores the rows of an extension's facts and imports at one new revision,
    /// as an extensional block of each table that gains rows.
    fn store_extension(&mut self, facts: HashMap<TableId, RowBuffer>) {
        let revision = Revision(self.step);
        let mut grew = false;
        for (table, rows) in facts {
            let arity = rows.arity();
            let stored = self
                .manager
                .add_rows(table, revision, ColumnOrder::identity(arity), rows);
            if stored > 0 {
                grew = true;
                self.record_extensional(table, revision, ExtensionalOrigin::Load);
                self.dirty.insert(table);
                self.loaded += stored;
            }
        }
        if grew {
            self.step += 1;
        }
    }

    /// Copies the semi-naive progress of each loaded rule into the extended program.
    ///
    /// The loaded statements come first in the merged program,
    /// so the loaded rules keep their indices and the step of their last application,
    /// and semi-naive evaluation continues where it left off.
    /// A new rule may add facts to its head predicates for the first time.
    /// Their tables are conservatively marked as gaining input facts,
    /// so the derived facts that depend on them through negation or aggregation are dropped
    /// and derived again.
    fn adopt_extension(&mut self, compiled: &mut CompiledProgram, old: &CompiledProgram) {
        for (rule, previous) in compiled.rules.iter_mut().zip(&old.rules) {
            rule.last_applied = previous.last_applied;
        }
        for rule in compiled.rules.iter().skip(old.rules.len()) {
            for table in Self::head_tables(rule) {
                self.dirty.insert(table);
            }
        }
    }

    /// Checks that an extension changes no `@base`, prefix, or parameter of the loaded
    /// program, which would change the meaning of the loaded statements or the stored values.
    ///
    /// `addition` is the revision that contains the extension alone.
    /// Returns an error for the first directive that would change one.
    fn check_extension(&self, addition: RevisionId) -> Result<(), Diagnostic> {
        for (_, statement) in self.pipeline.statements(addition) {
            let Statement::Directive(directive) = statement else {
                continue;
            };
            let refusal = match directive {
                Directive::Base(base)
                    if self.translation.base_iri.as_deref() != Some(base.iri.as_str()) =>
                {
                    Some((EngineError::ExtensionDeclaresBase, directive.span()))
                }
                Directive::Prefix(prefix)
                    if self
                        .translation
                        .prefixes
                        .get(&prefix.prefix)
                        .is_some_and(|iri| iri != &prefix.iri) =>
                {
                    Some((EngineError::ExtensionRebindsPrefix, directive.span()))
                }
                Directive::Parameter(parameter)
                    if self
                        .translation
                        .globals
                        .contains_key(&parameter.variable.name) =>
                {
                    Some((
                        EngineError::ExtensionRedefinesParameter {
                            name: parameter.variable.name.clone(),
                        },
                        directive.span(),
                    ))
                }
                _ => None,
            };
            if let Some((kind, span)) = refusal {
                let mut problem = kind.diagnostic();
                if let Some(span) = span {
                    problem = problem.with_label(span, "declared here");
                }
                return Err(problem);
            }
        }
        Ok(())
    }

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

        let (table, _) = self.written_predicate(predicate).map_err(|unknown| {
            unknown.with_hint("first `extend()` the program with statements that use it")
        })?;
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
        let mut translator = translation::Translator::new(stores, setup, state, Vec::new());
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
        #[cfg(debug_assertions)]
        if let Some(program) = &self.program {
            self.assert_stored_types(&program.column_types, table, &rows);
        }
        let revision = Revision(self.step);
        self.step += 1;
        let arity = rows.arity();
        let stored = self
            .manager
            .add_rows(table, revision, ColumnOrder::identity(arity), rows);
        if stored > 0 {
            self.record_extensional(table, revision, ExtensionalOrigin::ImportRows);
            self.dirty.insert(table);
            self.fixpoint = false;
            self.loaded += stored;
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
            .names
            .iter()
            .map(|names| types::RuleSourceInfo { span: None, names })
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
                .label_instances(diagnostic, program.statements[rule])),
            Some(types::TypeFinding { diagnostic, .. }) => Err(diagnostic),
        }
    }

    /// Registers, parses and links a program text, and passes the result to `call`
    /// (see [ingest](Engine::ingest)).
    pub(in crate::engine) fn ingest_text(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
        call: impl FnOnce(&mut Engine, Program) -> Result<(), Diagnostic>,
    ) -> Result<(), Vec<Diagnostic>> {
        let file = self
            .add_source(name, text)
            .map_err(|refused| vec![refused])?;
        let (program, diagnostics) = self.parse_source(file);
        if diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity() == Severity::Error)
        {
            return Err(diagnostics);
        }
        self.link(program)
            .and_then(|linked| call(self, linked))
            .map_err(|failed| vec![failed])
    }

    /// Registers, parses, links and checks a program text
    /// (see [check_source](Engine::check_source)).
    pub(in crate::engine) fn check_text(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> CheckReport {
        let file = match self.add_source(name, text) {
            Ok(file) => file,
            Err(refused) => return CheckReport::new(vec![refused], None),
        };
        let (program, mut diagnostics) = self.parse_source(file);
        let already = self.reports().len();
        let linked = self.link(program);
        diagnostics.extend(self.reports()[already..].iter().cloned());
        let types = match linked {
            Ok(linked) => {
                let (checked, types) = self.check(linked).into_parts();
                diagnostics.extend(checked);
                types
            }
            Err(problem) => {
                diagnostics.push(problem);
                None
            }
        };
        CheckReport::new(diagnostics, types)
    }

    /// Runs a check: stages the program,
    /// and translates the written program into scratch stores.
    ///
    /// Returns the types that the translation's type analysis inferred.
    /// Fails for a problem that ends the check early:
    /// the engine holds a program, the program is not linked, or a pipeline stage fails.
    pub(in crate::engine) fn check_inner(
        &mut self,
        program: Program,
    ) -> Result<InferredTypes, Diagnostic> {
        if self.program.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic());
        }
        tracing::debug!(statements = program.statements.len(), "checking a program");
        let staged = self.stage(program, &[])?;
        let (compiled, tables) = self.translate_written(
            staged.written,
            TranslationState::default(),
            HashMap::new(),
            Vec::new(),
            Purpose::Check,
        )?;
        let written = WrittenProgram::new(&self.pipeline, staged.written, staged.revision);
        let texts: Vec<Statement> = self
            .pipeline
            .statements(staged.written)
            .map(|(_, statement)| statement.clone())
            .collect();
        Ok(InferredTypes::of_check(
            NameScope::of(&texts),
            written.predicates(),
            &tables,
            &compiled,
        ))
    }
}
