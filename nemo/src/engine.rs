//! The reasoning engine, which evaluates a program to its fixpoint.
//!
//! An [Engine] owns the physical stores (tables, dictionary),
//! the program [pipeline](crate::pipeline), and the compiled rules.
//! [load](Engine::load) translates a program:
//! facts become stored rows, and rules compile into normalized executable form.
//! [reason](Engine::reason) runs semi-naive evaluation to the fixpoint:
//! rules apply round-robin, each application reads only what is new since its last one,
//! and evaluation stops after an iteration over all rules without new rows.
//! [rows](Engine::rows) reads a predicate's contents back as values.
//!
//! The engine covers Datalog with expressions (filters, assignments, head computations),
//! stratified negation, aggregates, existential rules (the restricted chase),
//! and global parameters.
//! Other features are reported as unsupported (N0203).
//!
//! The engine's modules sit in layers:
//! - `link`: modules instantiated into the program the engine loads,
//! - `transform`: the pipeline stages a loaded program passes first,
//! - `compile`: statements into executable rules,
//! - `evaluate`: the rules to their fixpoint,
//! - `trace`: why a fact holds,
//! - `results`: reading the stored facts back.
//!
//! Alongside them:
//! - `staging`: loading, extending, reloading and checking a program,
//! - `remote`: methods suspended for data from the network,
//! - `cancel`: cancellation.

mod cancel;
mod check;
mod compile;
mod evaluate;
mod link;
mod registered;
pub mod remote;
mod results;
mod staging;
#[cfg(debug_assertions)]
mod stored_types;
mod trace;
mod transform;
mod written;

#[cfg(test)]
mod tests;
pub use cancel::CancelToken;
pub use check::{CheckReport, InferredTypes, RuleTypes};
pub use results::{
    PredicateCursor, RowChunk,
    export::{ExportFormat, ExportSpec},
    predicates::{PredicateInfo, PredicateRole},
};

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{Revision, TableId, TableManager},
    value::{DataValue, ErrorCounts, Type, ValueRef},
};

use crate::{
    diagnostics::{AboutResource, EngineError},
    engine::{
        compile::compiled::CompiledProgram,
        compile::translation::TranslationState,
        evaluate::extensional::ExtensionalBlock,
        registered::Registered,
        remote::{Failure, PendingRequest, Suspension},
    },
    model::{Fact, FileId, Program, Statement},
    pipeline::{Pipeline, RevisionId, Stages},
    telemetry::{EngineEvent, EngineTimings, EventSink, SinkSlot},
    trace::TraceResponse,
};

/// The reasoning engine over one program.
///
/// A panic inside a call is a bug in Nemo.
/// It can leave the engine half-updated,
/// so an engine that a panic unwound out of must be dropped, not used again.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::Engine,
///     model::{FileId, Program},
/// };
///
/// let (program, diagnostics) = Program::parse(
///     "edge(1, 2) . edge(2, 3) .
///      reach(?x, ?y) :- edge(?x, ?y) .
///      reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
///     FileId::new(0),
/// );
/// assert!(diagnostics.is_empty());
///
/// let mut engine = Engine::new();
/// engine.load(program)?;
/// engine.reason()?;
/// assert!(engine.is_at_fixpoint());
/// assert_eq!(engine.count("reach")?, 3);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Default)]
pub struct Engine {
    /// The program pipeline (source of truth and report sink).
    pipeline: Pipeline,
    /// The table store.
    manager: TableManager,
    /// The dictionary.
    dictionary: Dictionary,
    /// The optimization passes, as [retain_imports](Self::retain_imports) describes.
    passes: compile::optimize::OptimizationPasses,
    /// The pipeline stages a loaded program passes (see [stages_mut](Self::stages_mut)).
    stages: staging::EngineStages,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    predicates: HashMap<String, Registered>,
    /// Facts added through [import_rows](Self::import_rows) before a program is loaded,
    /// which [load](Self::load) translates with the program.
    unloaded_rows: Vec<Statement>,
    /// In-memory resources registered through
    /// [add_resource](Self::add_resource), read by imports in place of
    /// the filesystem.
    resources: HashMap<String, Arc<[u8]>>,
    /// The module roots and the module texts registered by name, which linking reads.
    modules: link::ModuleSources,
    /// The compiled rules, present after [load](Self::load).
    program: Option<CompiledProgram>,
    /// The translation state kept between translations, which extensions,
    /// and rows added through [import_rows](Self::import_rows) after a load, are translated
    /// against.
    translation: TranslationState,
    /// The pipeline revision the compiled program was translated from.
    revision: Option<RevisionId>,
    /// The written program: the program as loaded, which results and diagnostics refer to.
    written: Option<written::WrittenProgram>,
    /// The application log: one entry per rule application that stored rows,
    /// ascending by step.
    ///
    /// It is tracing's record of which rule wrote which revision.
    /// [reload](Self::reload), which drops everything derived, clears it.
    applications: Vec<evaluate::application::LoggedApplication>,
    /// The evaluation context of each reasoning run.
    ///
    /// An explanation re-executes a run's applications in the run's context.
    /// Cleared with the application log.
    contexts: evaluate::contexts::RunContexts,
    /// The instant of what the loading methods translated since the last reasoning run began:
    /// the `NOW()` of its facts and option values, and of the next run.
    /// `None` if nothing was translated since.
    instant: Option<EvaluationContext>,
    /// The plan skeletons of the compiled program's rules.
    skeletons: evaluate::application::PlanSkeletons,
    /// The directory relative import resources resolve against, if one is set.
    import_directory: Option<std::path::PathBuf>,
    /// Per table, the wall time in nanoseconds of the rule applications that stored rows into it,
    /// which a query reports as the time deriving a predicate took.
    ///
    /// Cleared with the application log.
    derivation_time: HashMap<TableId, u64>,
    /// Per table, its extensional blocks, ascending.
    extensional: HashMap<TableId, Vec<ExtensionalBlock>>,
    /// The tables that gained input facts since the last [reason](Self::reason),
    /// or may gain facts through new rules.
    ///
    /// The next `reason` drops the derived facts that depend on them through negation or
    /// aggregation, and derives them again.
    dirty: HashSet<TableId>,
    /// The engine step counter, step 0 holding the loaded facts.
    step: u64,
    /// Rule applications performed so far, fetch rules included.
    applied: u64,
    /// Whether the stored facts are at a fixpoint: the last [reason](Self::reason)
    /// ran to completion and nothing was added since.
    ///
    /// A cancelled run leaves it false, and tracing then fails with N0510.
    fixpoint: bool,
    /// The facts stored by loading: the program's own and its imports'.
    loaded: usize,
    /// New facts derived by reasoning, beyond the loaded ones.
    derived: usize,
    /// Evaluation errors counted across all applications.
    errors: ErrorCounts,
    /// Wall-clock accounting of the reasoning so far.
    timings: EngineTimings,
    /// The cancellation flag [reason](Self::reason) checks between rule applications.
    ///
    /// [cancel_token](Self::cancel_token) hands out handles to it.
    cancel: CancelToken,
    /// Where telemetry events go, nowhere until a sink is [installed](Self::set_telemetry).
    telemetry: SinkSlot,
    /// The method suspended for data from the network, if any (see [pending](Self::pending)).
    suspension: Option<Suspension>,
    /// Per fetch rule, its requests in the current run and the failures `on_error = warn`
    /// let it continue past, reported as the run ends.
    fetch_tallies: HashMap<usize, evaluate::reason::FetchTally>,
    /// The last id given to a request of a suspended method.
    request_ids: u64,
}

impl Engine {
    /// Creates an empty engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a source text under a name, returning the id its spans use.
    ///
    /// Parsing the text against this id makes every diagnostic, the parser's and the engine's,
    /// point into the same source.
    /// The engine also registers here the module files it reads for `@use` directives.
    ///
    /// # Errors
    ///
    /// Returns N0480 for a text of 4 GiB or more.
    pub fn add_source(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<FileId, Diagnostic> {
        let (name, text) = (name.into(), text.into());
        if u32::try_from(text.len()).is_err() {
            return Err(EngineError::SourceTooLarge { name: name.clone() }
                .diagnostic()
                .with_resource(name));
        }
        Ok(self.pipeline.add_source(name, text))
    }

    /// Reads a program file and registers it under its path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read (N0208),
    /// or if it is too large to address (N0480).
    pub fn read_source(&mut self, path: &std::path::Path) -> Result<FileId, Diagnostic> {
        let name = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|error| {
            EngineError::SourceUnreadable {
                name: name.clone(),
                reason: error.to_string(),
            }
            .diagnostic()
            .with_resource(name.clone())
        })?;
        self.add_source(name, text)
    }

    /// Parses a registered source under its own id,
    /// so that the spans of what parsing reports point into the registered text.
    ///
    /// # Panics
    ///
    /// Panics for an id the engine did not hand out.
    pub fn parse_source(&self, file: FileId) -> (Program, Vec<Diagnostic>) {
        let text = self
            .source(file)
            .expect("the engine handed out the file id");
        Program::parse(text, file)
    }

    /// Returns the text a file id addresses, for rendering diagnostics against it,
    /// including the files the engine read on the program's behalf.
    pub fn source(&self, file: FileId) -> Option<&str> {
        self.pipeline.text(file)
    }

    /// Returns the name a file id addresses.
    pub fn file_name(&self, file: FileId) -> Option<&str> {
        self.pipeline.file_name(file)
    }

    /// Returns every source registered so far, by id, name, and text,
    /// including the module files the engine read for `@use` directives.
    pub fn sources(&self) -> impl Iterator<Item = (FileId, &str, &str)> {
        self.pipeline.sources()
    }

    /// Sets the directory relative import resources resolve against,
    /// in place of the directory of the file a directive was written in.
    ///
    /// A module's own imports still resolve against the module's directory,
    /// so that a module can ship the data it reads.
    pub fn set_import_directory(&mut self, directory: impl Into<std::path::PathBuf>) {
        self.import_directory = Some(directory.into());
    }

    /// Registers an in-memory resource:
    /// an import whose resource name matches reads these bytes instead of the filesystem.
    ///
    /// This serves data that exists only in memory,
    /// such as a browser blob or a text built in another language, to `@import` directives.
    pub fn add_resource(&mut self, name: impl Into<String>, bytes: impl Into<Arc<[u8]>>) {
        self.resources.insert(name.into(), bytes.into());
    }

    /// Keeps every import whole: every declared column, every row.
    ///
    /// By default, imports are loaded partially:
    /// columns no rule reads are skipped, and rows no rule can use are discarded.
    /// This does not change what the rules derive.
    /// Skipped columns are never parsed,
    /// so a row that is malformed only in a skipped column
    /// is stored rather than dropped and reported.
    /// The counts and warnings of a partial load can therefore differ from those of a whole one.
    /// Call this before writing imported tables out verbatim,
    /// or before tracing facts by all their source columns.
    ///
    /// It takes effect for programs loaded afterwards.
    pub fn retain_imports(&mut self) {
        self.passes.imports.clear();
    }

    /// Returns the pipeline stages a loaded program passes, so that more stages can be added.
    ///
    /// The list starts with the engine's own: `sparql`, if the feature is on.
    /// A stage rewrites the program through a [Commit](crate::pipeline::Commit)
    /// and declares each rewrite,
    /// so that results and diagnostics can be mapped back to the program as loaded.
    /// Changes take effect for programs loaded afterwards.
    pub fn stages_mut(&mut self) -> &mut Stages {
        &mut self.stages.0
    }

    /// Directs the engine's telemetry [EngineEvent]s into the given sink.
    ///
    /// [telemetry](crate::telemetry) describes the events
    /// and the ready-made [Recorder](crate::telemetry::Recorder).
    /// Without a sink, events cost nothing.
    pub fn set_telemetry(&mut self, sink: Arc<dyn EventSink<EngineEvent>>) {
        self.telemetry.install(sink);
    }

    /// Returns the cancellation handle of this engine's runs:
    /// cloneable, thread-safe, and shared by every handle handed out.
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }

    /// Links a program: every `@use` instantiated, and every module-qualified name resolved,
    /// into the program [load](Self::load) takes.
    ///
    /// A program without a `@use` comes back as it is.
    /// A `@use` names a module relative to the directory of the file it is written in,
    /// the working directory for a program without a file,
    /// or inside a [module root](Self::add_module_root).
    /// A text registered with [add_module](Self::add_module) is read instead of the module's file.
    /// Warnings go to the [reports](Self::reports).
    ///
    /// # Errors
    ///
    /// Returns the first problem with the modules:
    /// a file that cannot be read or does not parse, a cycle, an alias taken twice,
    /// a name an instance does not offer, or a binding or a parameter that does not fit.
    pub fn link(&mut self, program: Program) -> Result<Program, Diagnostic> {
        self.link_program(program)
    }

    /// Loads a program: facts become stored rows, imports read their resources, and rules
    /// compile.
    ///
    /// The program becomes the written program:
    /// results, traces and diagnostics refer to its statements,
    /// not to the forms the pipeline stages rewrite them into.
    /// A program using modules must be [linked](Self::link) first.
    /// A relative resource resolves against the directory of the file its directive was
    /// written in, or against the [import directory](Self::set_import_directory) if one is set.
    ///
    /// # Errors
    ///
    /// Returns N0481 if the engine already holds a program (see [has_program](Self::has_program)),
    /// and N0490 for a program that is not linked.
    /// Returns the first problem of the program or its data otherwise,
    /// or N0491 if its imports need data from the network (see [pending](Self::pending)).
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn load(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.load_program(program)
    }

    /// Extends the loaded program:
    /// the new statements join it, new facts are stored, new imports read,
    /// and the whole program compiles again, since strata and join orders are global.
    ///
    /// Every table is kept, and every unchanged rule continues semi-naive evaluation where it
    /// left off.
    /// The next [reason](Self::reason) continues to the new fixpoint.
    /// If a new statement adds facts to a predicate that is negated or aggregated,
    /// the derived facts that depend on it are dropped and derived again.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded.
    /// Returns an error for an extension that would change what loaded statements mean:
    /// declaring `@base`, rebinding a prefix, or redefining a parameter.
    /// Use [reload](Self::reload) for these.
    /// Returns the first problem of the extension or its data otherwise,
    /// or N0491 if its imports need data from the network (see [pending](Self::pending)).
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn extend(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.extend_program(program)
    }

    /// Replaces the program, which is how statements are retracted.
    ///
    /// Every derived row is dropped, every rule starts over,
    /// and the next [reason](Self::reason) derives everything again from the input facts.
    ///
    /// The dictionary, the registered sources and resources are kept,
    /// and so are the loaded data of every predicate whose defining statements
    /// (facts, imports, declarations) are unchanged.
    /// In particular, unchanged imports are not read again.
    /// A changed `@prefix`, `@base`, `@parameter`, or `@type` can change the meaning of any value,
    /// so it rebuilds everything.
    /// Rows added through [import_rows](Self::import_rows) to a *rebuilt* predicate
    /// cannot be reproduced from statements, so they are dropped with warning N0485.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded, and N0490 for a program that is not linked.
    /// Returns the first problem of the program or its data otherwise,
    /// or N0491 if its changed imports need data from the network (see [pending](Self::pending)).
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn reload(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.reload_program(program)
    }

    /// Checks a program without loading it:
    /// parameters resolve, directives validate, rules compile, and the whole program
    /// stratifies, but no imported data is read and no facts are stored.
    /// A parameter without a value, which a run needs supplied, is read as any value of its
    /// declared type, as an `@input` predicate holds any rows its declaration allows.
    ///
    /// Unlike [load](Self::load), a check collects problems rather than ending at the first,
    /// and reports everything it found, warnings included,
    /// with the types the analysis inferred.
    /// Reading no data, a check suits a program while it is edited.
    /// A check leaves the engine unchanged, so the engine can still load a program afterwards.
    /// Like a load, it takes a [linked](Self::link) program.
    /// For a program that is not linked, the reported diagnostics include N0490.
    pub fn check(&mut self, program: Program) -> CheckReport {
        let already = self.pipeline.reports().len();
        let (types, aborted) = match self.check_inner(program) {
            Ok(types) => (Some(types), None),
            Err(problem) => (None, Some(problem)),
        };
        let mut reports = self.pipeline.reports()[already..].to_vec();
        reports.extend(aborted);
        CheckReport::new(reports, types)
    }

    /// Registers a program text, [links](Self::link) what it parses to,
    /// and applies one of the loading methods (`load`, `extend`, `reload`) to the result.
    ///
    /// If parsing reports an error, the text is rejected with everything parsing reported.
    /// Warnings alone do not stop it.
    ///
    /// # Errors
    ///
    /// Returns the diagnostics of the step that failed:
    /// registration, parsing, linking, or the loading method.
    pub fn ingest(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
        call: impl FnOnce(&mut Engine, Program) -> Result<(), Diagnostic>,
    ) -> Result<(), Vec<Diagnostic>> {
        self.ingest_text(name, text, call)
    }

    /// Registers a program text and checks it without loading it.
    ///
    /// Reports what parsing, linking and [check](Self::check) reported, warnings included,
    /// or the error of a failed registration.
    pub fn check_source(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> CheckReport {
        self.check_text(name, text)
    }

    /// Adds rows computed in memory, rather than read from a file, to a predicate.
    ///
    /// Before [load](Self::load), the rows join the program as facts,
    /// with declaration checks, type analysis, and everything else.
    /// After a load, each row is checked as a fact of the program is,
    /// and the rows are stored in the predicate's table as a new revision.
    /// The next [reason](Self::reason) treats them as new facts and continues to the new
    /// fixpoint, dropping and deriving again the facts that depend on the predicate through
    /// negation or aggregation.
    /// The predicate is named by its resolved form, as in [rows](Self::rows).
    ///
    /// # Errors
    ///
    /// After a load, returns N0486 for a predicate the program does not use,
    /// since only [extend](Self::extend) introduces new ones.
    /// Returns N0487 for a row holding a null, since nulls are only created by reasoning,
    /// and an error for a row of the wrong width or outside the predicate's declaration.
    pub fn import_rows(
        &mut self,
        predicate: &str,
        rows: impl IntoIterator<Item = Vec<DataValue>>,
    ) -> Result<(), Diagnostic> {
        self.admit_rows(predicate, rows)
    }

    /// Returns the requests the suspended method waits on, or none if no method is suspended.
    ///
    /// A method that needs data from the network suspends:
    /// it returns N0491 and lists its requests here.
    /// This happens for
    /// - [load](Self::load), [extend](Self::extend) or [reload](Self::reload) with remote imports,
    /// - [reason](Self::reason) with a bound import, an import with a `where` clause.
    ///
    /// The stored data is consistent, as after a cancellation.
    /// Pass each response to [respond](Self::respond) and call [resume](Self::resume).
    /// With the `http` feature, `nemo::driver::HttpDriver` does both over HTTP.
    pub fn pending(&self) -> Vec<PendingRequest> {
        self.suspension
            .as_ref()
            .map_or_else(Vec::new, Suspension::pending)
    }

    /// Passes in the response to a pending request:
    /// its body, or the failure that prevented one.
    ///
    /// Returns `false` if no pending request has the id.
    /// A failure is final, so retry before passing it in.
    /// A request too large for the server (status 413 or 414),
    /// or one whose response the server cut off partway, typically at its time limit,
    /// is sent again in smaller pieces if the engine can split it.
    pub fn respond(&mut self, id: u64, response: Result<Arc<[u8]>, Failure>) -> bool {
        self.suspension
            .as_mut()
            .is_some_and(|suspension| suspension.respond(id, response))
    }

    /// Continues the suspended method with the responses given,
    /// and does nothing if no method is suspended.
    ///
    /// The method may suspend again, for requests still without a response or for new ones.
    /// A loading method translates its program again,
    /// reading its remote resources from the responses.
    /// A reasoning run continues at the application it suspended in.
    ///
    /// # Errors
    ///
    /// Returns what the continued method returns, including N0491 if it suspends again.
    pub fn resume(&mut self) -> Result<(), Diagnostic> {
        self.resume_suspended()
    }

    /// Runs semi-naive evaluation to the fixpoint, stratum by stratum.
    ///
    /// A stratum's negated predicates are final before it starts.
    /// The run is one evaluation [context](EvaluationContext):
    /// every `NOW()` it evaluates reads the same instant, the one the facts loaded for it
    /// were evaluated at.
    /// That is the instant of the first loading method since the last run,
    /// or, if there was none, the instant the run began at.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded.
    /// Returns N0207 if the run's [cancel token](Self::cancel_token) is cancelled,
    /// which the run checks between rule applications.
    /// The run stops there with the tables consistent and its progress recorded,
    /// and a further `reason()` continues toward the fixpoint.
    /// Returns N0491 if the fetch of a bound import suspends the run
    /// for the responses to its requests (see [pending](Self::pending)).
    /// The run continues at that application with [resume](Self::resume),
    /// or with `reason()`, which does the same for a suspended run.
    /// Returns the first failure of a rule application otherwise.
    pub fn reason(&mut self) -> Result<(), Diagnostic> {
        let already = self.pipeline.reports().len();
        let outcome = self.reason_to_fixpoint();
        let Some(written) = &self.written else {
            return outcome;
        };
        self.pipeline
            .map_reports(already, |report| written.present(report));
        outcome.map_err(|problem| written.present(problem))
    }

    /// Returns every predicate of the program, sorted by name.
    ///
    /// Predicates introduced by pipeline stages are not listed.
    pub fn predicates(&self) -> Vec<PredicateInfo> {
        let mut names = self.listed();
        names.sort();
        names
            .into_iter()
            .filter_map(|name| self.predicate(&name))
            .collect()
    }

    /// Returns a predicate of the program, or `None` for a name the program never used.
    ///
    /// Its role follows the written program:
    /// a predicate is derived if a rule has it in a head,
    /// and extensional if a fact, an import, or an `@input` produces facts for it,
    /// or if rows were added to it through [import_rows](Self::import_rows).
    /// A predicate that is only read counts as extensional, even if it holds no facts.
    pub fn predicate(&self, name: &str) -> Option<PredicateInfo> {
        Some(PredicateInfo {
            name: name.to_owned(),
            arity: self.arity_of(name)?,
            role: self.role_of(name),
        })
    }

    /// Returns the program's outputs:
    /// the predicates it declares `@output` or exports, in declaration order,
    /// or every predicate rules derive, sorted by name, if it declares neither.
    ///
    /// An output is stored whole,
    /// so [rows](Self::rows) and [count](Self::count) never fail with N0489 for it.
    pub fn outputs(&self) -> Vec<String> {
        self.output_names()
    }

    /// Returns the type of value each column of a predicate can hold,
    /// as the type analysis inferred it:
    /// an upper bound per column, including the types of nested fields of tuples and maps.
    ///
    /// Returns `None` for a name the program never used,
    /// and for a predicate the engine does not store whole.
    /// [TypeSyntax](crate::values::TypeSyntax) writes a type in declaration syntax.
    pub fn column_types(&self, predicate: &str) -> Option<&[Type]> {
        let (table, _) = self.whole_predicate(predicate).ok()?;
        self.program
            .as_ref()?
            .column_types
            .get(&table)
            .map(Vec::as_slice)
    }

    /// Returns the number of input facts the program's predicates hold: its
    /// facts and imports, and the rows added through [import_rows](Self::import_rows).
    pub fn loaded_facts(&self) -> usize {
        self.fact_counts().0
    }

    /// Returns the number of facts the program's predicates hold from reasoning,
    /// a bound import's fetches included.
    pub fn derived_facts(&self) -> usize {
        self.fact_counts().1
    }

    /// Returns the stored rows of a predicate, decoded into values.
    ///
    /// This is the convenience that drains a [cursor](Self::cursor) in one call.
    /// For large results, read in bounded chunks through the cursor instead.
    ///
    /// # Errors
    ///
    /// Returns what [cursor](Self::cursor) returns.
    pub fn rows(&mut self, predicate: &str) -> Result<Vec<Vec<DataValue>>, Diagnostic> {
        let mut cursor = self.cursor(predicate)?;
        let mut rows = Vec::with_capacity(cursor.remaining());
        while !cursor.is_done() {
            let chunk = self.read_chunk(&mut cursor, PredicateCursor::BUDGET)?;
            rows.extend(chunk.rows());
        }
        Ok(rows)
    }

    /// Opens a cursor over a predicate's stored rows, which [read_chunk](Self::read_chunk) reads.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded, N0486 for a predicate the program does not have,
    /// and N0489 for one the engine does not store whole, naming what is left out.
    pub fn cursor(&self, predicate: &str) -> Result<PredicateCursor, Diagnostic> {
        self.open_cursor(predicate)
    }

    /// Returns the next rows of a cursor, at most `budget` of them,
    /// decoded into an owned column-major [RowChunk], which is empty once the cursor is exhausted.
    ///
    /// The cursor holds no borrow, and each read borrows the engine for its own duration only.
    ///
    /// # Errors
    ///
    /// Returns N0206 for a cursor whose table has changed since it was opened,
    /// rather than return rows that no longer match the table.
    /// [is_current](Self::is_current) checks this beforehand.
    pub fn read_chunk(
        &mut self,
        cursor: &mut PredicateCursor,
        budget: usize,
    ) -> Result<RowChunk, Diagnostic> {
        self.decode_chunk(cursor, budget)
    }

    /// Returns whether a cursor still reads the rows it was opened on:
    /// no later work changed its predicate's table.
    ///
    /// [read_chunk](Self::read_chunk) fails with N0206 for a cursor that is not current.
    pub fn is_current(&self, cursor: &PredicateCursor) -> bool {
        // The blocks are compared before the range is read: a merged
        // block would make the cursor's range misaligned.
        self.manager.blocks(cursor.table) == cursor.blocks
            && self.manager.row_count(cursor.table, cursor.range.clone()) == cursor.rows
    }

    /// Returns the number of stored rows of a predicate, counted without decoding any values.
    ///
    /// # Errors
    ///
    /// Returns what [cursor](Self::cursor) returns.
    pub fn count(&self, predicate: &str) -> Result<usize, Diagnostic> {
        let (table, _) = self.whole_predicate(predicate)?;
        Ok(self
            .manager
            .row_count(table, Revision(0)..Revision(self.step.max(1))))
    }

    /// Returns the export directives, resolved, in directive order.
    pub fn exports(&self) -> &[ExportSpec] {
        self.program
            .as_ref()
            .map_or(&[], |program| program.exports.as_slice())
    }

    /// Writes every export directive's rows into the given directory.
    ///
    /// Files take their declared resource name,
    /// or the conventional `predicate.extension`, with the characters of an IRI predicate that
    /// no file name takes mangled to `_`.
    /// A predicate the program never used yields an empty file.
    /// Rows with cells that do not fit a declared column format are dropped and reported.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded, and the failure to write a file.
    pub fn export(&mut self, directory: impl AsRef<std::path::Path>) -> Result<(), Diagnostic> {
        let specs = self.exports().to_vec();
        self.export_specs(directory, &specs)
    }

    /// Writes the rows of the given export specifications into the given directory.
    ///
    /// For predicates the program does not export, the specifications can be built directly,
    /// for example to write every predicate or to choose another serialization.
    ///
    /// # Errors
    ///
    /// Returns N0486 or N0489 for a predicate the engine does not hold or does not store whole,
    /// and the failure to write a file.
    pub fn export_specs(
        &mut self,
        directory: impl AsRef<std::path::Path>,
        specs: &[ExportSpec],
    ) -> Result<(), Diagnostic> {
        self.write_exports(directory, specs)
    }

    /// Serializes one export specification's rows into memory,
    /// as the bytes the file of [export_specs](Self::export_specs) would hold, compression
    /// included.
    ///
    /// This serves environments without a filesystem, such as browsers,
    /// and exports that are streamed elsewhere.
    ///
    /// # Errors
    ///
    /// Returns N0486 or N0489 for a predicate the engine does not hold or does not store whole,
    /// and the failure to serialize the rows.
    pub fn export_bytes(&mut self, spec: &ExportSpec) -> Result<Vec<u8>, Diagnostic> {
        self.serialize_export(spec)
    }

    /// Traces facts: for each, whether it holds after reasoning,
    /// and if it does, a proof tree explaining why, as [trace](crate::trace) types.
    ///
    /// Facts are ground atoms over the program's predicates,
    /// and their named arguments resolve as in the program text.
    /// Traces describe the facts at the fixpoint,
    /// so tracing fails for an engine that has not reached it.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded,
    /// N0209 if the engine holds updates [reason](Self::reason) has not processed,
    /// and N0510 if reasoning has not reached the fixpoint.
    /// Returns an error as well for a fact that names no predicate of the program or does not
    /// fit it.
    pub fn trace(&mut self, facts: &[Fact]) -> Result<TraceResponse, Diagnostic> {
        self.traceable()?;
        let resolved = self.ground_facts(facts)?;
        trace::proof::run(self, resolved)
    }

    /// Traces a page of a predicate's stored rows:
    /// the rows from `start` in the engine's row order, at most `budget` of them.
    ///
    /// # Errors
    ///
    /// Returns what [trace](Self::trace) returns,
    /// and what [cursor](Self::cursor) returns for the predicate.
    pub fn trace_rows(
        &mut self,
        predicate: &str,
        start: usize,
        budget: usize,
    ) -> Result<TraceResponse, Diagnostic> {
        self.trace_page(predicate, start, budget)
    }

    /// Traces every stored row of the given predicates, a page of at most `budget` rows at a
    /// time, handing each page's response to the sink as it is ready.
    ///
    /// Memory thus holds the proofs of one page at a time, not those of a whole predicate.
    ///
    /// # Errors
    ///
    /// Returns the first error of [trace_rows](Self::trace_rows) or of the sink,
    /// which ends the tracing.
    pub fn trace_predicates(
        &mut self,
        predicates: &[String],
        budget: usize,
        sink: impl FnMut(TraceResponse) -> Result<(), Diagnostic>,
    ) -> Result<(), Diagnostic> {
        self.trace_in_pages(predicates, budget, sink)
    }

    /// Answers a `treeForTable` query of the [query format](crate::trace::query):
    /// the selected facts of a predicate, with their derivation expanded by the engine.
    ///
    /// # Errors
    ///
    /// Returns the errors of [trace](Self::trace),
    /// and an error for a query that does not fit the written program.
    pub fn tree_for_table(
        &mut self,
        query: crate::trace::query::TreeForTableQuery,
    ) -> Result<crate::trace::query::TreeForTableResponse, Diagnostic> {
        self.traceable()?;
        trace::query::tree_for_table(self, query)
    }

    /// Answers a `tableEntriesForTreeNodes` query of the [query format](crate::trace::query):
    /// per node of the tree the query describes, the facts consistent with all of its
    /// constraints, parents before children.
    ///
    /// # Errors
    ///
    /// Returns the errors of [trace](Self::trace),
    /// and an error for a query that does not fit the written program.
    pub fn table_entries_for_tree_nodes(
        &mut self,
        query: crate::trace::query::TableEntriesForTreeNodesQuery,
    ) -> Result<Vec<crate::trace::query::NodeResponse>, Diagnostic> {
        self.traceable()?;
        trace::query::table_entries_for_tree_nodes(self, query)
    }

    /// Returns the rule a statement id names, as traces and queries refer to it:
    /// a rule of the written program or a bound import, with its text, `#[name]` and source
    /// position.
    ///
    /// Returns `None` for an id that names no such statement.
    pub fn rule(&self, id: usize) -> Option<crate::trace::WrittenRule> {
        let statement = self.written.as_ref()?.statement(id)?;
        match self.pipeline.statement(statement) {
            Statement::Rule(_) => {}
            Statement::Directive(crate::model::Directive::Import(import)) if import.is_bound() => {}
            _ => return None,
        }
        Some(trace::proof::written_rule(self, statement))
    }

    /// Returns whether the engine holds a program.
    ///
    /// [load](Self::load) fails with N0481 if the engine holds one,
    /// and the methods that need one fail with N0482 if it holds none.
    pub fn has_program(&self) -> bool {
        self.program.is_some()
    }

    /// Returns a value in term syntax as the program reads it back,
    /// in a fact given to [trace](Self::trace) for example.
    ///
    /// Under a base IRI, a bare name is relative to the base,
    /// so every IRI is written in angle brackets, which the program takes as written.
    pub fn term_text(&self, value: ValueRef<'_>) -> String {
        if self.translation.base_iri.is_some() {
            value.bracketed().to_string()
        } else {
            value.to_string()
        }
    }

    /// Returns a predicate's name as the program reads it back:
    /// bare where it reads back bare, and in angle brackets otherwise
    /// or under a base IRI, as [term_text](Self::term_text) writes values.
    pub fn predicate_text(&self, predicate: &str) -> String {
        if self.translation.base_iri.is_some() {
            format!("<{predicate}>")
        } else {
            crate::model::Identifier::bare(predicate).to_string()
        }
    }

    /// Returns whether the stored facts are at the fixpoint of the program: the
    /// last [reason](Self::reason) ran to completion, and nothing was
    /// added since.
    ///
    /// Traces and queries require this, and fail with N0209 or N0510 otherwise.
    pub fn is_at_fixpoint(&self) -> bool {
        self.program.is_some() && self.dirty.is_empty() && self.fixpoint
    }

    /// Returns the number of rule applications performed so far.
    pub fn applications(&self) -> u64 {
        self.applied
    }

    /// Returns the wall-clock accounting of the reasoning so far.
    pub fn timings(&self) -> EngineTimings {
        let mut timings = self.timings;
        timings.stored_orders = self
            .predicates
            .values()
            .map(|registered| self.manager.stored_orders(registered.table) as u64)
            .sum();
        timings
    }

    /// Returns the bytes currently allocated across all stored tables and the dictionary.
    ///
    /// Transient buffers are not counted.
    /// Their high-water mark is [WorkCounts::buffer_bytes](nemo_physical::plan::WorkCounts::buffer_bytes)
    /// in [EngineTimings::counts].
    pub fn memory_bytes(&self) -> usize {
        self.manager.memory_bytes() + self.dictionary.memory().total()
    }

    /// Returns the program as loaded and after every pipeline stage that rewrote it,
    /// each with the stage's name (`"loaded"` for the first) and its text,
    /// which parses back to the same program.
    ///
    /// Returns none before [load](Self::load).
    pub fn revisions(&self) -> Vec<(&'static str, String)> {
        let Some(revision) = self.revision else {
            return Vec::new();
        };
        let mut revisions = Vec::new();
        let mut previous: Option<&[crate::pipeline::StatementId]> = None;
        for revision in self.pipeline.lineage(revision) {
            let statements = self.pipeline.revision_statements(revision);
            // A stage that rewrote nothing shows nothing.
            if previous == Some(statements) {
                continue;
            }
            previous = Some(statements);
            revisions.push((
                self.pipeline.stage(revision).unwrap_or("loaded"),
                self.pipeline.program(revision).to_string(),
            ));
        }
        revisions
    }

    /// Returns the rendered texts of the compiled program's rules, by position.
    ///
    /// A rule's position is its index among the rules of the compiled program,
    /// which the pipeline stages may have rewritten.
    /// Telemetry events and [join_order](Self::join_order) name rules by it,
    /// while traces name them by statement id (see [rule](Self::rule)).
    /// Returns none before [load](Self::load).
    /// A bound SPARQL import is a rule here too, rendered as its directive.
    pub fn ran_rules(&self) -> &[String] {
        self.program
            .as_ref()
            .map_or(&[], |program| program.texts.as_slice())
    }

    /// Returns the join order chosen for the rule at a [position](Self::ran_rules),
    /// as variable ids in the order the join binds them,
    /// or `None` if the program has fewer rules.
    pub fn join_order(&self, position: usize) -> Option<Vec<usize>> {
        self.program
            .as_ref()?
            .rules
            .get(position)
            .map(|rule| rule.order.clone())
    }

    /// Returns the reports accumulated while loading and reasoning.
    pub fn reports(&self) -> &[Diagnostic] {
        self.pipeline.reports()
    }

    /// Returns the evaluation errors counted across all rule applications, by kind.
    ///
    /// A row whose evaluation fails is dropped and counted here.
    pub fn evaluation_errors(&self) -> ErrorCounts {
        self.errors
    }
}
