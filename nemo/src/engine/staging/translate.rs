//! The translation of a staged program for a loading method.
//!
//! A loading method changes the engine only if its translation is done.
//! It takes a [Checkpoint] before the translation,
//! and [translate](Engine::translate) returns the engine to it otherwise:
//! if the translation fails, if it needs remote resources that are not fetched yet,
//! or if it would have to read a stored import again.
//!
//! The written program of a staged program that the pipeline stages changed is translated
//! first, into [ScratchStores], so that its problems are reported against the statements as
//! written ([translate_written](Engine::translate_written)).

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{TableId, TableManager},
    trie::RowBuffer,
    value::ErrorCounts,
};

use crate::{
    engine::{
        Engine,
        check::rewrite_unvalued,
        compile::{
            compiled::CompiledProgram,
            import_plan::Instability,
            translation::{
                Prepared, Purpose, TranslationSetup, TranslationState, TranslationStores,
                Translator,
            },
        },
        remote::{ResourceRequest, SuspendedLoad},
        staging::Staged,
        stores::Registered,
        written::WrittenProgram,
    },
    model::Statement,
    pipeline::{RevisionId, StatementId},
};

/// Stores that a translation writes to in place of the engine's,
/// so that the engine's stores stay as they were.
///
/// They are the engine's tables without their rows, a dictionary, registered predicates
/// and error counts.
pub(super) struct ScratchStores {
    /// The engine's tables, under their ids, without their rows.
    manager: TableManager,
    /// A dictionary separate from the engine's.
    dictionary: Dictionary,
    /// The registered predicates that the translation starts from.
    predicates: HashMap<String, Registered>,
    /// Error counts separate from the engine's.
    errors: ErrorCounts,
}

impl ScratchStores {
    /// Returns the predicates registered in these stores.
    fn into_predicates(self) -> HashMap<String, Registered> {
        self.predicates
    }
}

/// A staged program as a loading method translates it.
pub(super) struct StagedProgram {
    /// The revision that is compiled.
    pub(super) revision: RevisionId,
    /// The ids of the revision's statements, in program order.
    pub(super) ids: Vec<StatementId>,
    /// The revision's statements, in program order.
    pub(super) statements: Vec<Statement>,
    /// The written program, which diagnostics refer to.
    pub(super) written: WrittenProgram,
    /// The pipeline stages that changed the written program, in order.
    pub(super) stages: Vec<&'static str>,
}

/// What a translation that is done leaves to store.
pub(super) struct TranslatedProgram {
    /// The compiled program.
    pub(super) compiled: CompiledProgram,
    /// The rows of the program's facts and imports, by table.
    pub(super) facts: HashMap<TableId, RowBuffer>,
    /// The rows given through [import_rows](crate::engine::Engine::import_rows) before the
    /// first load, by table.
    pub(super) imported: Vec<(TableId, RowBuffer)>,
}

/// How the translation of a staged program ended.
pub(super) enum Translated {
    /// The translation is done.
    /// The engine has the predicates that it registered and the translation state that it left.
    Done(Box<TranslatedProgram>),
    /// The translation is prepared, and commits once these requests have responses.
    Missing(Vec<ResourceRequest>),
    /// The translation cannot commit,
    /// since these stored imports do not cover what the program reads of them.
    Unstable(Vec<Instability>),
}

/// The registered predicates and the translation state of an engine at one moment.
pub(super) struct Checkpoint {
    /// The translation state.
    pub(super) translation: TranslationState,
    /// The registered predicates.
    predicates: HashMap<String, Registered>,
}

impl Engine {
    /// Returns a staged program as a loading method translates it.
    pub(super) fn staged_program(&self, staged: &Staged) -> StagedProgram {
        let (ids, statements) = self
            .pipeline
            .statements(staged.revision)
            .map(|(id, statement)| (id, statement.clone()))
            .unzip();

        StagedProgram {
            revision: staged.revision,
            ids,
            statements,
            written: WrittenProgram::new(&self.pipeline, staged.written, staged.revision),
            stages: self.rewriting_stages(staged),
        }
    }

    /// Returns the registered predicates and the translation state as they are now.
    pub(super) fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            translation: self.translation.clone(),
            predicates: self.stores.predicates.clone(),
        }
    }

    /// Returns the engine to a checkpoint.
    ///
    /// Every table registered since, for a predicate or as the scratch table of a bound import,
    /// is dropped, and the registered predicates and the translation state are restored.
    fn roll_back(&mut self, checkpoint: Checkpoint) {
        let kept: HashSet<TableId> = checkpoint
            .predicates
            .values()
            .map(|registered| registered.table)
            .chain(checkpoint.translation.scratch.values().copied())
            .collect();
        let stale: Vec<TableId> = self
            .stores
            .predicates
            .values()
            .map(|registered| registered.table)
            .chain(self.translation.scratch.values().copied())
            .filter(|table| !kept.contains(table))
            .collect();
        for table in stale {
            self.stores.manager.drop_table(table);
        }

        self.stores.predicates = checkpoint.predicates;
        self.translation = checkpoint.translation;
    }

    /// Translates a staged program, and returns how the translation ended.
    ///
    /// `preloaded` says, per statement, whether its data is already stored.
    /// Unless the translation is done, the engine is returned to `checkpoint`.
    /// A translation that needs remote resources leaves its prepared state in `load`,
    /// and the next call for `load` commits from it, with the resources that `load` fetched.
    pub(super) fn translate(
        &mut self,
        load: &mut SuspendedLoad,
        program: &StagedProgram,
        preloaded: Vec<bool>,
        checkpoint: Checkpoint,
    ) -> Result<Translated, Diagnostic> {
        let outcome = self.prepare_and_commit(load, program, preloaded);
        if !matches!(outcome, Ok(Translated::Done(_))) {
            self.roll_back(checkpoint);
        }

        outcome
    }

    /// Prepares the translation of a staged program, unless `load` holds its prepared state,
    /// and commits it if nothing stands in the way.
    ///
    /// Preparing registers and stores nothing.
    /// A commit that fails leaves the engine with what it registered.
    fn prepare_and_commit(
        &mut self,
        load: &mut SuspendedLoad,
        program: &StagedProgram,
        preloaded: Vec<bool>,
    ) -> Result<Translated, Diagnostic> {
        let context = self.loading_context();
        let purpose = Purpose::of_load(&program.stages);

        let state = match load.prepared.take() {
            Some(state) => state,
            None => {
                // The part of the state that statements determine is rebuilt from the full
                // list of statements. The part that stored data determines is kept,
                // since the facts and imports that are not read again cannot rebuild it.
                let mut state = self.translation.clone();
                state.clear_statement_state();

                let (stores, mut setup) = self.translation_parts(purpose, context);
                setup.written = Some(&program.written);
                let mut translator = Translator::new(stores, setup, state, preloaded.clone());
                let prepared = translator.prepare(&program.statements, &program.ids);
                let (state, _) = translator.finish();

                let Prepared { requests, unstable } = prepared?;
                if !unstable.is_empty() {
                    return Ok(Translated::Unstable(unstable));
                }

                let missing = load.missing(requests);
                if !missing.is_empty() {
                    load.prepared = Some(state);
                    return Ok(Translated::Missing(missing));
                }

                state
            }
        };

        let (stores, mut setup) = self.translation_parts(purpose, context);
        setup.written = Some(&program.written);
        let mut translator = Translator::new(stores, setup, state, preloaded);
        let fetched = std::mem::take(&mut load.fetched);
        let compiled = translator.commit(&program.statements, &program.ids, fetched);
        let imported = translator.take_imported_rows();
        let (state, facts) = translator.finish();

        // The state names the scratch tables that the commit registered,
        // which a roll-back drops.
        self.translation = state;

        Ok(Translated::Done(Box::new(TranslatedProgram {
            compiled: compiled?,
            facts,
            imported,
        })))
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
    /// A check first reads the program's names and parameters,
    /// and then rewrites the parameters that it has no value for into inputs
    /// (see [rewrite_unvalued]).
    ///
    /// Returns the compiled program and the predicates registered for it.
    pub(in crate::engine) fn translate_written(
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

        // The rewrite of a check may leave statements out, so `preloaded` would not match them.
        debug_assert!(
            purpose != Purpose::Check || preloaded.is_empty(),
            "a check preloads nothing"
        );
        let mut scratch = self.scratch_stores(predicates);
        let (stores, setup) = self.scratch_translation_parts(purpose, &mut scratch);
        let mut translator = Translator::new(stores, setup, state, preloaded);
        let compiled = if purpose == Purpose::Check {
            let written: Vec<Statement> = statements
                .iter()
                .map(|(_, statement)| statement.clone())
                .collect();
            let unvalued = translator.check_names(&written)?;
            let (ids, statements): (Vec<_>, Vec<_>) =
                rewrite_unvalued(statements, &unvalued).into_iter().unzip();
            translator.check_statements(&statements, &ids)?
        } else {
            let (ids, statements): (Vec<_>, Vec<_>) = statements.into_iter().unzip();
            translator.program(&statements, &ids)?
        };

        Ok((compiled, scratch.into_predicates()))
    }

    /// Returns the stores and the setup of one translation.
    ///
    /// They are borrowed separately,
    /// so that the translation can write the stores while it reads the rest of the engine.
    /// The translation evaluates its ground expressions in `context`.
    pub(in crate::engine) fn translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        context: EvaluationContext,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        self.parts(purpose, None, context)
    }

    /// Returns scratch stores beside the engine's tables,
    /// which start from the given registered predicates.
    fn scratch_stores(&self, predicates: HashMap<String, Registered>) -> ScratchStores {
        ScratchStores {
            manager: self.stores.manager.without_rows(),
            dictionary: Dictionary::new(),
            predicates,
            errors: ErrorCounts::default(),
        }
    }

    /// Returns the stores and the setup of a translation that writes to `scratch`,
    /// so that the engine's stores stay as they were.
    ///
    /// The translation still reports to the engine's pipeline.
    /// Its ground expressions are evaluated at the instant chosen for the next run,
    /// if one is chosen, and otherwise at the current instant, which this does not choose.
    fn scratch_translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: &'a mut ScratchStores,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        let context = self.run.next_context.unwrap_or_else(EvaluationContext::now);
        self.parts(purpose, Some(scratch), context)
    }

    /// Returns the stores and the setup of a translation.
    ///
    /// The stores are `scratch` if it is given, and the engine's otherwise.
    fn parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: Option<&'a mut ScratchStores>,
        context: EvaluationContext,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        let (manager, dictionary, predicates, errors) = match scratch {
            Some(scratch) => (
                &mut scratch.manager,
                &mut scratch.dictionary,
                &mut scratch.predicates,
                &mut scratch.errors,
            ),
            None => (
                &mut self.stores.manager,
                &mut self.stores.dictionary,
                &mut self.stores.predicates,
                &mut self.run.errors,
            ),
        };

        (
            TranslationStores {
                manager,
                dictionary,
                predicates,
                pipeline: &mut self.pipeline,
                errors,
            },
            TranslationSetup {
                import_directory: self.settings.import_directory.as_deref(),
                resources: &self.settings.resources,
                telemetry: &self.settings.telemetry,
                passes: &self.settings.passes,
                purpose,
                written: None,
                previous_types: self
                    .loaded
                    .as_ref()
                    .map(|loaded| &loaded.compiled.column_types),
                context,
                imported: self.unloaded_rows.blocks(),
            },
        )
    }
}
