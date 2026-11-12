//! Loading, extending and reloading a program,
//! and adding rows through [import_rows](crate::engine::Engine::import_rows).
//!
//! A loading method first *stages* its program: it adds the program to the pipeline as the
//! written revision, and runs the pipeline stages over it.
//! It then [translates](translate) the staged program and stores its data.
//! The three loading methods share these steps, and differ in what they keep:
//! a first load starts from nothing, an [extension](extend) keeps everything that is stored,
//! and a [reload] keeps the input rows of the predicates that the change leaves alone.
//!
//! A loading method that fails leaves the engine as it was,
//! and the pipeline without the statements of its program.

mod context;
mod extend;
mod reload;
mod rows;
mod translate;

use std::sync::Arc;

use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    management::{ColumnOrder, Revision, TableId},
    trie::RowBuffer,
};

use self::translate::{StagedProgram, Translated};
use crate::{
    diagnostics::EngineError,
    engine::{
        Engine,
        compile::{
            compiled::CompiledProgram,
            import_plan::Instability,
            translation::{Purpose, TranslationState},
        },
        evaluate::extensional::ExtensionalOrigin,
        imported_rows::UnloadedRows,
        link::is_instance_parameter,
        loaded::LoadedProgram,
        remote::{LoadKind, Progress, ResourceRequest, SuspendedLoad, Suspension},
        transform::sparql_rewrite,
    },
    model::{Directive, Program, Statement},
    pipeline::{ReportMark, RevisionId, Stages},
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

/// How far a loading method got with its staged program.
enum Loaded {
    /// The program is translated and its data is stored.
    /// This is the compiled program, which the engine holds from now on.
    Done(CompiledProgram),
    /// The method needs the responses to these requests, and has changed nothing.
    Missing(Vec<ResourceRequest>),
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

/// Returns the error of a loading method that cannot read a stored import again,
/// about the first of the stored imports that do not cover what its program reads.
fn refuse_unstable(unstable: &[Instability]) -> Diagnostic {
    unstable
        .first()
        .expect("an unstable translation names its imports")
        .diagnostic()
}

impl Engine {
    /// Runs the pipeline stages over a first program and loads it.
    pub(in crate::engine) fn load_program(
        &mut self,
        program: Program,
    ) -> Result<Progress, Diagnostic> {
        if self.loaded.is_some() {
            return Err(EngineError::AlreadyLoaded.diagnostic().with_hint(
                "use `extend()` to add to the loaded program, or `reload()` to replace it",
            ));
        }

        tracing::info!(statements = program.statements.len(), "loading a program");
        self.start_loading(LoadKind::Load, program)
    }

    /// Returns the loaded program of a method that extends or replaces it.
    fn held(&self) -> &LoadedProgram {
        self.loaded
            .as_ref()
            .expect("the method was given a loaded program to change")
    }

    /// Stages the program of a loading method, and translates it as far as it gets
    /// (see [continue_loading](Self::continue_loading)).
    ///
    /// A program that fails to stage leaves no statement in the pipeline.
    fn start_loading(&mut self, kind: LoadKind, program: Program) -> Result<Progress, Diagnostic> {
        let reports = self.pipeline.mark_reports();
        let statements = self.pipeline.mark();
        let staged = match self.stage_for(kind, program) {
            Ok(staged) => staged,
            Err(problem) => {
                self.pipeline.rewind(statements);
                return Err(problem);
            }
        };

        let load = SuspendedLoad::new(kind, staged, reports, statements);
        self.continue_loading(load, reports)
    }

    /// Stages a program as its loading method does.
    ///
    /// If the pipeline stages changed the program, the program as written is translated first,
    /// without reading data or storing anything.
    /// So the type analysis and the stratification are checked on the written statements,
    /// and the translation of the staged program reports only what reading the data finds.
    fn stage_for(&mut self, kind: LoadKind, program: Program) -> Result<Staged, Diagnostic> {
        let staged = match kind {
            LoadKind::Extend => self.stage_extension(program)?,
            LoadKind::Load | LoadKind::Reload => {
                // The stages are told which predicates have rows added through
                // `import_rows`, which no statement shows.
                let imported = self.imported_predicates();
                self.stage(program, &imported)?
            }
        };
        if self.rewriting_stages(&staged).is_empty() {
            return Ok(staged);
        }

        match kind {
            LoadKind::Load => self.validate_load(&staged)?,
            LoadKind::Extend => self.validate_extension(&staged)?,
            LoadKind::Reload => self.validate_reload(&staged)?,
        }

        Ok(staged)
    }

    /// Continues a loading method from its staged program.
    ///
    /// The method waits while a request it sent has no response.
    /// Otherwise it translates the program and stores its data.
    /// If the translation needs remote resources that are not fetched yet,
    /// the method is suspended for them, having changed nothing.
    /// A method that fails also changes nothing,
    /// and leaves none of its program's statements in the pipeline.
    ///
    /// The reports made since `reports` are rewritten to refer to the written program,
    /// as is the error.
    pub(in crate::engine) fn continue_loading(
        &mut self,
        mut load: SuspendedLoad,
        reports: ReportMark,
    ) -> Result<Progress, Diagnostic> {
        load.take_responses();
        if load.slots.has_pending() {
            return Ok(self.suspend(Suspension::Load(load)));
        }

        let program = self.staged_program(&load.staged);
        let outcome = match load.kind {
            LoadKind::Load => self.load_staged(&mut load, &program),
            LoadKind::Extend => self.extend_staged(&mut load, &program),
            LoadKind::Reload => self.reload_staged(&mut load, &program),
        };
        self.pipeline
            .present_reports(reports, |report| program.written.present(report));

        match outcome {
            Ok(Loaded::Done(compiled)) => {
                let (rules, strata) = (compiled.rules.len(), compiled.strata.len());
                self.skeletons.clear();
                self.last_applied.resize(compiled.rules.len(), 0);
                self.loaded = Some(LoadedProgram {
                    written: program.written,
                    revision: program.revision,
                    compiled: Arc::new(compiled),
                });

                tracing::debug!(
                    method = load.kind.method(),
                    rules,
                    strata,
                    predicates = self.predicates.len(),
                    loaded = self.loaded_facts(),
                    "program loaded"
                );

                Ok(Progress::Done)
            }
            Ok(Loaded::Missing(missing)) => {
                load.add_requests(missing, &mut self.request_ids);
                Ok(self.suspend(Suspension::Load(load)))
            }
            Err(problem) => {
                self.pipeline.rewind(load.statements_from);
                Err(program.written.present(problem))
            }
        }
    }

    /// Translates the written program of a staged first program as
    /// [load_staged](Self::load_staged) translates the program that the stages made of it,
    /// but reads no data and stores nothing.
    fn validate_load(&mut self, staged: &Staged) -> Result<(), Diagnostic> {
        let predicates = self.predicates.clone();
        self.translate_written(
            staged.written,
            TranslationState::default(),
            predicates,
            Vec::new(),
            Purpose::Validate,
        )?;
        Ok(())
    }

    /// Translates a staged first program and stores its data.
    ///
    /// The program's facts and imports go into revision 0.
    /// The rows added through [import_rows](Engine::import_rows) before the load get a revision
    /// of their own, as such rows do after a load.
    fn load_staged(
        &mut self,
        load: &mut SuspendedLoad,
        program: &StagedProgram,
    ) -> Result<Loaded, Diagnostic> {
        let checkpoint = self.checkpoint();
        let translated = match self.translate(load, program, Vec::new(), checkpoint)? {
            Translated::Done(translated) => *translated,
            Translated::Missing(missing) => return Ok(Loaded::Missing(missing)),
            Translated::Unstable(unstable) => return Err(refuse_unstable(&unstable)),
        };

        self.assert_stored_types_of(&translated.compiled, &translated.facts);
        self.store_input(translated.facts, Revision(0), ExtensionalOrigin::Load);
        self.step = 1;

        if !translated.imported.is_empty() {
            let imported = translated
                .imported
                .iter()
                .map(|(table, rows)| (table, rows));
            self.assert_stored_types_of(&translated.compiled, imported);
            self.store_input(
                translated.imported,
                Revision(1),
                ExtensionalOrigin::ImportRows,
            );
            self.step = 2;
        }

        self.unloaded_rows = UnloadedRows::default();

        Ok(Loaded::Done(translated.compiled))
    }

    /// Checks the rows that a loading method is about to store against the column types that
    /// its program was analyzed with (see [assert_stored_types](Self::assert_stored_types)).
    fn assert_stored_types_of<'r>(
        &self,
        compiled: &CompiledProgram,
        rows: impl IntoIterator<Item = (&'r TableId, &'r RowBuffer)>,
    ) {
        for (&table, rows) in rows {
            self.assert_stored_types(&compiled.column_types, table, rows);
        }
    }

    /// Stores input rows at one revision, as an extensional block of each table that gains
    /// rows, and returns the tables that gained rows.
    fn store_input(
        &mut self,
        rows: impl IntoIterator<Item = (TableId, RowBuffer)>,
        revision: Revision,
        origin: ExtensionalOrigin,
    ) -> Vec<TableId> {
        let mut grown = Vec::new();
        for (table, rows) in rows {
            let order = ColumnOrder::identity(rows.arity());
            let stored = self.manager.add_rows(table, revision, order, rows);
            if stored > 0 {
                self.record_extensional(table, revision, origin);
                grown.push(table);
            }
        }

        grown
    }

    /// Gives the `@parameter` declarations of a program the values set through
    /// [set_parameter](Engine::set_parameter), whatever values they state.
    ///
    /// Only the declarations of the program itself are given values.
    /// The values of a module instance's parameters come from its `@use`.
    /// For an `extension`, the parameters of the loaded program count as declared.
    /// Fails for a value whose parameter is not declared.
    fn supply_parameters(&self, program: &mut Program, extension: bool) -> Result<(), Diagnostic> {
        if self.parameters.is_empty() {
            return Ok(());
        }

        let mut declared: Vec<String> = Vec::new();
        for statement in &mut program.statements {
            let Statement::Directive(Directive::Parameter(parameter)) = statement else {
                continue;
            };

            let name = &parameter.variable.name;
            if is_instance_parameter(name) {
                continue;
            }

            if let Some(value) = self.parameters.get(name) {
                parameter.value = Some(value.clone());
            }

            declared.push(name.clone());
        }

        if extension {
            let loaded = self.translation.globals.keys();
            declared.extend(loaded.filter(|name| !is_instance_parameter(name)).cloned());
        }

        let Some(unknown) = self.parameters.keys().find(|name| !declared.contains(name)) else {
            return Ok(());
        };

        let mut problem = EngineError::UnknownParameter {
            name: unknown.clone(),
        }
        .diagnostic();
        if !declared.is_empty() {
            declared.sort();
            declared.dedup();
            problem = problem.with_note(format!(
                "the program declares: `${}`",
                declared.join("`, `$")
            ));
        }

        Err(problem)
    }

    /// Stages a program: adds it to the pipeline as the written revision,
    /// and runs the pipeline stages over it.
    ///
    /// `imported` names the predicates with rows added through
    /// [import_rows](Engine::import_rows), which the stages see only by name.
    /// The program's parameters first get the values set through
    /// [set_parameter](Engine::set_parameter).
    /// Fails for a program that is not linked, for a value set for a parameter that the
    /// program does not declare, and for a problem a stage reports.
    pub(in crate::engine) fn stage(
        &mut self,
        mut program: Program,
        imported: &[String],
    ) -> Result<Staged, Diagnostic> {
        expect_linked(&program)?;
        self.supply_parameters(&mut program, false)?;
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

    /// Registers, parses and links a program text, and passes the result to `call`
    /// (see [with_source](Engine::with_source)).
    pub(in crate::engine) fn with_source_text(
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
}
