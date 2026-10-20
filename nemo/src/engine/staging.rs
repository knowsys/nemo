//! Loading, extending, reloading and checking a program,
//! and adding rows through `import_rows` after a load.

mod reload;

use std::collections::HashMap;

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
        check::open_parameters,
        compile::compiled::CompiledProgram,
        compile::translation::{self, Purpose, TranslationState},
        compile::types,
        evaluate::extensional::ExtensionalOrigin,
        registered::Registered,
        remote::{LoadKind, SuspendedLoad, Suspension},
        written::WrittenProgram,
    },
    model::{Directive, Fact, Identifier, Program, Statement, Term},
    pipeline::{NameScope, RevisionId, Stages},
};

/// A program staged in the pipeline: the written revision, as loaded,
/// and the revision the pipeline stages made of it.
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
        #[cfg_attr(not(feature = "sparql"), expect(unused_mut))]
        let mut stages = Stages::new();
        #[cfg(feature = "sparql")]
        stages.push(super::transform::sparql_rewrite::SparqlRewrite);
        Self(stages)
    }
}

/// Returns N0490 for a program that is not linked: its `@use` directives are for
/// [link](Engine::link) to instantiate.
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
    pub(in crate::engine) fn load_program(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.abandon();
        if self.program.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic().with_hint(
                "use `extend()` to add to the loaded program, or `reload()` to replace it",
            ));
        }

        // Rows added through `import_rows` before the program join it as
        // facts, so they pass the same declaration checks and seed the
        // type analysis like the program's own facts.
        let mut program = program;
        let imported_rows = self.unloaded_rows.len();
        program.statements.append(&mut self.unloaded_rows);
        tracing::info!(statements = program.statements.len(), "loading a program");

        let already = self.pipeline.reports().len();
        let staged = self.stage(program)?;
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
        let mut load = SuspendedLoad::new(LoadKind::Load, staged);
        load.imported_rows = imported_rows;
        self.load_staged(load, already)
    }

    /// Translates a staged first program and stores its data.
    ///
    /// If remote resources are missing, it undoes the translation and suspends (N0491).
    /// The reports from `already` on are rewritten to refer to the written program.
    pub(super) fn load_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<(), Diagnostic> {
        let revision = load.staged.revision;
        let written = WrittenProgram::new(&self.pipeline, load.staged.written, revision);
        let (ids, statements): (Vec<_>, Vec<_>) = self
            .pipeline
            .statements(revision)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();

        let translated = self.pipeline.reports().len();
        let saved_predicates = self.predicates.clone();
        let responses = load.responses();
        let stages = self.rewriting_stages(&load.staged);
        let instant = self.loading_instant();
        let (stores, mut setup) = self.translation_parts(Purpose::of_load(&stages), instant);
        setup.written = Some(&written);
        setup.responses = Some(&responses);
        let mut translator =
            translation::Translator::new(stores, setup, TranslationState::default(), Vec::new());
        let outcome = translator.program(&statements, &ids);
        let requests = translator.take_requests();
        let (state, facts) = translator.finish();
        self.translation = state;
        if !requests.is_empty() {
            self.forget_registrations(&saved_predicates, &TranslationState::default());
            self.translation = TranslationState::default();
            self.pipeline.withdraw_reports(translated);
            self.pipeline
                .present_reports(already, |report| written.present(report));
            load.ask(requests, &mut self.request_ids);
            return Err(self.suspend(Suspension::Load(load)));
        }
        self.pipeline
            .present_reports(already, |report| written.present(report));
        let compiled = outcome.map_err(|problem| written.present(problem))?;

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
        // Everything stored so far was loaded, not derived.
        for &Registered { table, .. } in self.predicates.values() {
            self.loaded += self.manager.row_count(table, Revision(0)..Revision(1));
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
        self.step = 1;
        Ok(())
    }

    /// Loads a linked program into the pipeline as the written program,
    /// and runs the pipeline stages over it.
    pub(super) fn stage(&mut self, program: Program) -> Result<Staged, Diagnostic> {
        expect_linked(&program)?;
        let written = self.pipeline.load(program);
        let revision = self.pipeline.apply(&self.stages.0, written)?;
        Ok(Staged { written, revision })
    }

    /// Returns the pipeline stages that changed a staged program, in order,
    /// or none if the revision compiled holds the written statements unchanged.
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

    /// Translates a written program as its load would, reading no data and storing nothing,
    /// so that its problems are reported against the statements as written.
    ///
    /// The translation starts from the given state and registered predicates,
    /// and `preloaded` marks the statements whose data is already stored.
    /// The analysis of the program, and its strata, are proved over these statements.
    /// The pipeline stages rewrite the program afterwards,
    /// and the translation of what they made then reports only what reading finds.
    /// A check reads the parameters without a value as inputs (see [open_parameters]).
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
        let (ids, statements): (Vec<_>, Vec<_>) = self
            .pipeline
            .statements(written)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();
        let (statements, ids) = if purpose == Purpose::Check {
            open_parameters(statements, ids)
        } else {
            (statements, ids)
        };
        let mut scratch = self.scratch_stores(predicates);
        let (stores, setup) = self.scratch_translation_parts(purpose, &mut scratch);
        let mut translator = translation::Translator::new(stores, setup, state, preloaded);
        let compiled = translator.program(&statements, &ids)?;
        Ok((compiled, scratch.into_predicates()))
    }

    /// Stages an extension: checks that it is linked and changes no `@base`, prefix, or
    /// parameter of the loaded program, and appends it to the loaded revision.
    ///
    /// The pipeline stages then run over the whole program without rewriting the loaded
    /// statements, so a rule of the extension that reads a loaded import is rewritten
    /// with the loaded program in view, and the names the stages introduce are new
    /// to the whole program.
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
        #[cfg(feature = "sparql")]
        let merged = self.revive_sparql_imports(held, merged);
        let revision = self.pipeline.extend(&self.stages.0, merged, frozen)?;
        Ok(Staged { written, revision })
    }

    /// Brings back the SPARQL imports without a `where` clause that the `sparql` stage removed
    /// from the loaded program because no rule read them, so that the extension's rules can
    /// read them.
    ///
    /// The stage removes them again if they stay unread.
    #[cfg(feature = "sparql")]
    fn revive_sparql_imports(&mut self, held: RevisionId, merged: RevisionId) -> RevisionId {
        use std::collections::HashSet;

        use crate::{engine::transform::sparql_rewrite, pipeline::StatementId};

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
    /// Every table that exists now but did not before, of a predicate or the scratch table of a
    /// bound import, is dropped, and the predicate map returns to what it was.
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
    pub(in crate::engine) fn extend_program(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.abandon();
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
        let load = SuspendedLoad::new(LoadKind::Extend, staged);
        self.extend_staged(load, already)
    }

    /// Translates the written program of a staged extension as [extend_staged](Self::extend_staged)
    /// translates the program the stages made of it, reading no data and storing nothing.
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
    /// If remote resources are missing, it undoes the translation and suspends (N0491).
    /// The reports from `already` on are rewritten to refer to the written program.
    pub(super) fn extend_staged(
        &mut self,
        mut load: SuspendedLoad,
        already: usize,
    ) -> Result<(), Diagnostic> {
        let old = self.program.take().expect("an extension extends a program");
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

        // The state derived from statements is rebuilt from the full list.
        // The state derived from data keeps what skipped facts and unread
        // imports cannot derive again.
        let translated = self.pipeline.reports().len();
        let saved = self.translation.clone();
        let saved_predicates = self.predicates.clone();
        let mut state = std::mem::take(&mut self.translation);
        state.clear_statement_state();
        let responses = load.responses();
        let stages = self.rewriting_stages(&load.staged);
        let instant = self.loading_instant();
        let (stores, mut setup) = self.translation_parts(Purpose::of_load(&stages), instant);
        setup.written = Some(&written);
        setup.responses = Some(&responses);
        setup.previous_types = Some(&old.column_types);
        let mut translator = translation::Translator::new(stores, setup, state, preloaded);
        let outcome = translator.program(&statements, &ids);
        let requests = translator.take_requests();
        let (state, facts) = translator.finish();
        self.translation = state;
        let suspended = !requests.is_empty();
        let mut compiled = match outcome {
            Ok(compiled) if !suspended => compiled,
            outcome => {
                // Undo the registrations of this translation and restore
                // the previous state.
                self.forget_registrations(&saved_predicates, &saved);
                self.translation = saved;
                self.program = Some(old);
                if suspended {
                    self.pipeline.withdraw_reports(translated);
                }
                self.pipeline
                    .present_reports(already, |report| written.present(report));
                if suspended {
                    load.ask(requests, &mut self.request_ids);
                    return Err(self.suspend(Suspension::Load(load)));
                }
                let problem = outcome.expect_err("a translation that was not suspended failed");
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
        Ok(())
    }

    /// Stores an extension's facts and imports as one fresh extensional block.
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

    /// Carries each old rule's semi-naive progress into the extended program.
    ///
    /// The old statements are a prefix of the merged list, so the old rules keep their indices
    /// and the step of their last application,
    /// and semi-naive evaluation continues where it left off.
    /// A new rule may add facts to its head predicates for the first time.
    /// Their tables are conservatively marked as gaining input facts,
    /// so derived facts that depend on them through negation or aggregation are dropped and
    /// derived again.
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
    /// program, which would change the meaning of statements already loaded or values
    /// already stored.
    ///
    /// Returns an error for the first such directive.
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
    /// as facts of the program before a load, and as a new revision after one.
    ///
    /// After a load, the program was analyzed with the types its columns could hold.
    /// Rows within the target's inferred types keep that analysis;
    /// rows that widen them are kept only if the whole program, analyzed again,
    /// still keeps every declaration, and the call is refused otherwise.
    pub(in crate::engine) fn admit_rows(
        &mut self,
        predicate: &str,
        rows: impl IntoIterator<Item = Vec<DataValue>>,
    ) -> Result<(), Diagnostic> {
        self.abandon();
        if self.program.is_none() {
            for row in rows {
                self.unloaded_rows.push(Statement::Fact(Fact::new(
                    Identifier::Iri(predicate.to_owned()),
                    Self::row_terms(predicate, &row)?,
                )));
            }
            return Ok(());
        }

        let (table, _) = self.written_predicate(predicate).map_err(|unknown| {
            unknown.with_hint("first `extend()` the program with statements that use it")
        })?;
        // Each row is checked as a fact of the program is (declarations,
        // the element types `@type` declares, encoding), against the kept
        // translation state. A refused call leaves the seeds as they were.
        let seeds_before = self.translation.seeds.get(&table).cloned();
        let state = std::mem::take(&mut self.translation);
        let instant = self.loading_instant();
        let (stores, setup) = self.translation_parts(Purpose::Load, instant);
        let mut translator = translation::Translator::new(stores, setup, state, Vec::new());
        let mut outcome = Ok(());
        for row in rows {
            let fact = match Self::row_terms(predicate, &row) {
                Ok(terms) => Fact::new(Identifier::Iri(predicate.to_owned()), terms),
                Err(problem) => {
                    outcome = Err(problem);
                    break;
                }
            };
            if let Err(problem) = translator.fact(&fact) {
                // The row has no source position, so the note shows it.
                let terms: Vec<String> = fact.terms.iter().map(ToString::to_string).collect();
                outcome = Err(problem.with_note(format!(
                    "the row given to `import_rows` is `{}({})`",
                    Identifier::Bare(predicate.to_owned()),
                    terms.join(", ")
                )));
                break;
            }
        }
        let (state, mut facts) = translator.finish();
        self.translation = state;
        if let Err(problem) = outcome {
            self.restore_seeds(table, seeds_before);
            return Err(problem);
        }

        let Some(rows) = facts.remove(&table) else {
            return Ok(());
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

    /// Returns whether everything a table's seeds allow lies within its inferred column types,
    /// so that the analysis the program was loaded with still holds.
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

    /// Puts back a table's seeds as they were before a refused call.
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

    /// Analyzes the loaded program again over the kept translation state,
    /// returning the inferred column types,
    /// or the first error of a declaration the program is no longer shown to keep.
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

    /// Returns a row given through `import_rows` as fact terms, or N0487 for a row holding a
    /// null.
    fn row_terms(predicate: &str, row: &[DataValue]) -> Result<Vec<Term>, Diagnostic> {
        let terms: Option<Vec<Term>> = row.iter().map(Term::value).collect();
        terms.ok_or_else(|| {
            EngineError::NullInRow {
                predicate: predicate.to_owned(),
            }
            .diagnostic()
        })
    }

    /// Carries a program text through registration, parsing, linking, and a loading method.
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

    /// Carries a program text through registration, parsing, linking, and a check.
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

    /// Runs the check: the pipeline stages, whose own problems it reports,
    /// and a translation of the program as written into scratch stores,
    /// returning the types its analysis inferred.
    ///
    /// Errors that end a check early are returned as `Err`:
    /// N0481 if the engine holds a program, and N0490 for a program that is not linked.
    pub(in crate::engine) fn check_inner(
        &mut self,
        program: Program,
    ) -> Result<InferredTypes, Diagnostic> {
        if self.program.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic());
        }
        tracing::debug!(statements = program.statements.len(), "checking a program");
        let staged = self.stage(program)?;
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
