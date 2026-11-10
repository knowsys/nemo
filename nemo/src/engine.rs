//! The reasoning engine, which evaluates a program to its fixpoint.
//!
//! An [Engine] owns the physical stores (the tables and the dictionary),
//! the program [pipeline](crate::pipeline), and the compiled rules.
//! Its public functions are its *methods*:
//! - The *loading methods*, [load](Engine::load), [extend](Engine::extend) and
//!   [reload](Engine::reload), translate a program.
//!   Facts become stored rows, and rules are compiled into executable form.
//! - [reason](Engine::reason) evaluates the rules semi-naively to the fixpoint.
//!   One call of it to its end is a *run*.
//! - [rows](Engine::rows) and the other result methods read the stored facts back as values.
//!
//! The *written program* is the program as the loading methods received it.
//! The pipeline stages may rewrite it, and what the engine evaluates is the
//! *compiled program*.
//! Results, traces and diagnostics refer to the written program.
//!
//! The engine covers Datalog with expressions (filters, assignments, head computations),
//! stratified negation, aggregates, existential rules (the restricted chase),
//! and global parameters.
//! Other features are reported as unsupported.
//!
//! The engine's submodules, in the order a program passes through them:
//! - `link` instantiates the modules a program uses, before the program is loaded.
//! - `transform` contains the engine's own pipeline stages.
//! - `compile` translates statements into executable rules.
//! - `evaluate` applies the rules until the fixpoint.
//! - `trace` explains why a fact holds.
//! - `results` reads the stored facts back.
//!
//! Alongside them:
//! - `staging` implements the loading methods and the check of a program.
//! - `imported_rows` keeps the rows given through [import_rows](Engine::import_rows).
//! - [remote] implements the methods suspended for data from the network.
//! - `cancel` implements cancellation.

mod cancel;
mod check;
mod compile;
mod evaluate;
mod imported_rows;
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
    value::{DataValue, ErrorCounts, IriRef, Type, ValueRef},
};

use crate::{
    diagnostics::{AboutResource, EngineError},
    engine::{
        compile::compiled::CompiledProgram,
        compile::translation::TranslationState,
        evaluate::extensional::ExtensionalBlock,
        registered::Registered,
        remote::{Client, ClientSlot, PendingRequest, Progress, RequestId, Response, Suspension},
    },
    model::{Fact, FileId, Program, Statement},
    pipeline::{Pipeline, RevisionId, Stages},
    telemetry::{EngineEvent, EngineTimings, EventSink, SinkSlot},
    trace::{TracePart, TraceResponse, TracedFact},
};

/// The reasoning engine over one program.
///
/// A panic inside a method is a bug in Nemo.
/// It can leave the engine half-updated,
/// so after a panic unwinds out of a method, the engine must be dropped, not used again.
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
    /// The program pipeline, which stores the registered sources, every revision of the
    /// program, and the reports.
    pipeline: Pipeline,
    /// The table manager, which stores the tables of the predicates.
    manager: TableManager,
    /// The dictionary, which encodes the stored values.
    dictionary: Dictionary,
    /// The optimization passes that compilation runs.
    ///
    /// [retain_imports](Self::retain_imports) removes the import passes.
    passes: compile::optimize::OptimizationPasses,
    /// The pipeline stages that run over every loaded program
    /// (see [stages_mut](Self::stages_mut)).
    stages: staging::EngineStages,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    predicates: HashMap<String, Registered>,
    /// The rows added through [import_rows](Self::import_rows) before a program is loaded,
    /// one block per predicate.
    ///
    /// The first [load](Self::load) checks them against the program and stores them.
    /// They stay here until a load succeeds.
    unloaded_rows: imported_rows::UnloadedRows,
    /// The in-memory resources registered through [add_resource](Self::add_resource), by name.
    ///
    /// An import of a resource with one of these names reads the bytes from here
    /// instead of the filesystem.
    resources: HashMap<String, Arc<[u8]>>,
    /// The module roots and the module texts registered by name, which [link](Self::link)
    /// reads.
    modules: link::ModuleSources,
    /// The compiled program, or `None` before [load](Self::load).
    program: Option<CompiledProgram>,
    /// The translation state kept between translations.
    ///
    /// An extension, and the rows added through [import_rows](Self::import_rows) after a
    /// load, are translated against it.
    translation: TranslationState,
    /// The pipeline revision the compiled program was translated from.
    revision: Option<RevisionId>,
    /// The written program, which results and diagnostics refer to.
    written: Option<written::WrittenProgram>,
    /// The application log, with one entry per rule application that stored rows,
    /// in ascending order of step.
    ///
    /// It records which rule wrote which revision, for tracing.
    /// [reload](Self::reload) clears it, since a reload drops every derived row.
    applications: Vec<evaluate::application::LoggedApplication>,
    /// The evaluation context of each run.
    ///
    /// Tracing executes a run's rule applications again in the run's context.
    /// It is cleared together with the application log.
    contexts: evaluate::contexts::RunContexts,
    /// The evaluation context of the next run, if a loading method or
    /// [import_rows](Self::import_rows) chose it since the last run began.
    ///
    /// The first such call chooses it at its own instant,
    /// and it evaluates the `NOW()` of facts and option values in it.
    /// Later calls, and the next run, evaluate their `NOW()` in the same context.
    /// A call that fails or is discarded undoes its choice.
    next_context: Option<EvaluationContext>,
    /// The plan skeletons of the compiled program's rules.
    skeletons: evaluate::application::PlanSkeletons,
    /// The directory that relative import resources are resolved against, if one is set.
    import_directory: Option<std::path::PathBuf>,
    /// Per table, the wall time in nanoseconds of the rule applications that stored rows
    /// into it.
    ///
    /// It is cleared together with the application log.
    derivation_time: HashMap<TableId, u64>,
    /// Per table, its extensional blocks in ascending order.
    extensional: HashMap<TableId, Vec<ExtensionalBlock>>,
    /// The tables that gained input facts since the last run,
    /// or may gain facts through new rules.
    ///
    /// The next run first drops the derived facts that depend on them through negation or
    /// aggregation, so that it derives them again (see [recover](Self::recover)).
    dirty: HashSet<TableId>,
    /// The step counter, which is the revision that the next stored rows go into.
    ///
    /// Revision 0 contains the input facts of the first load.
    step: u64,
    /// The number of rule applications so far, including the fetches of bound imports.
    applied: u64,
    /// Whether the last run reached the fixpoint and nothing was added since.
    ///
    /// A cancelled run leaves it false.
    fixpoint: bool,
    /// The number of input rows stored: the program's facts, its imports' rows,
    /// and the rows added through [import_rows](Self::import_rows).
    loaded: usize,
    /// The number of rows that reasoning stored, less those dropped again.
    derived: usize,
    /// The evaluation errors of all rule applications, counted by kind.
    errors: ErrorCounts,
    /// The wall-clock times of the reasoning so far.
    timings: EngineTimings,
    /// The cancellation flag, which [reason](Self::reason) checks between rule applications.
    ///
    /// [cancel_token](Self::cancel_token) returns handles to it.
    cancel: CancelToken,
    /// The sink that receives telemetry events, if one was [installed](Self::set_telemetry).
    telemetry: SinkSlot,
    /// The method suspended for data from the network, if any (see [pending](Self::pending)).
    suspension: Option<Suspension>,
    /// The client through which the plain methods send their requests
    /// (see [set_client](Self::set_client)).
    client: ClientSlot,
    /// Per rule of a bound import, by rule index, the tally of its fetches in the current run.
    ///
    /// The tallies are reported as warnings when the run ends.
    fetch_tallies: HashMap<usize, evaluate::reason::FetchTally>,
    /// The last id given to a request of a suspended method.
    request_ids: u64,
}

impl Engine {
    /// Creates an empty engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a source text under a name, and returns the file id for spans into it.
    ///
    /// If the text is parsed with this id, every diagnostic, from the parser or the engine,
    /// points into the registered text.
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
    /// Returns N0208 if the file cannot be read, and N0480 if it is 4 GiB or larger.
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

    /// Parses a registered source with its own file id.
    ///
    /// The spans of the program and of the diagnostics then point into the registered text.
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

    /// Returns the text registered under a file id, or `None` for an id the engine did not
    /// hand out.
    ///
    /// The registered texts include the module files that [link](Self::link) read,
    /// so that a diagnostic can be rendered against the text its spans point into.
    pub fn source(&self, file: FileId) -> Option<&str> {
        self.pipeline.text(file)
    }

    /// Returns the name registered under a file id, or `None` for an id the engine did not
    /// hand out.
    pub fn file_name(&self, file: FileId) -> Option<&str> {
        self.pipeline.file_name(file)
    }

    /// Returns every registered source as its file id, name and text,
    /// including the module files that [link](Self::link) read.
    pub fn sources(&self) -> impl Iterator<Item = (FileId, &str, &str)> {
        self.pipeline.sources()
    }

    /// Sets the directory against which relative import resources are resolved,
    /// instead of the directory of the file that contains the import.
    ///
    /// A module's own imports are still resolved against the module's directory,
    /// so that a module can ship the data it reads.
    pub fn set_import_directory(&mut self, directory: impl Into<std::path::PathBuf>) {
        self.import_directory = Some(directory.into());
    }

    /// Registers an in-memory resource under a name.
    ///
    /// An import of a resource with that name reads these bytes instead of the filesystem.
    /// This makes data that exists only in memory available to `@import` directives,
    /// such as a browser blob or a text built in another language.
    pub fn add_resource(&mut self, name: impl Into<String>, bytes: impl Into<Arc<[u8]>>) {
        self.resources.insert(name.into(), bytes.into());
    }

    /// Makes imports store every declared column and every row.
    ///
    /// By default, imports are loaded partially:
    /// the columns no rule reads are skipped, and the rows no rule can use are discarded.
    /// This does not change what the rules derive.
    /// Skipped columns are never parsed,
    /// so a row that is malformed only in a skipped column
    /// is stored rather than dropped and reported.
    /// The counts and warnings of a partial load can therefore differ from those of a whole one.
    /// Call this before writing imported tables out unchanged,
    /// or before tracing facts by all their source columns.
    ///
    /// It takes effect for programs loaded afterwards.
    pub fn retain_imports(&mut self) {
        self.passes.imports.clear();
    }

    /// Returns the pipeline stages that run over every loaded program,
    /// so that more stages can be added.
    ///
    /// The list starts with the engine's own stage, `sparql`.
    /// A stage rewrites the program through a [Commit](crate::pipeline::Commit)
    /// and declares each rewrite,
    /// so that results and diagnostics can be mapped back to the written program.
    /// Changes take effect for programs loaded afterwards.
    pub fn stages_mut(&mut self) -> &mut Stages {
        &mut self.stages.0
    }

    /// Sends the engine's telemetry [EngineEvent]s to the given sink.
    ///
    /// [telemetry](crate::telemetry) describes the events
    /// and the ready-made [Recorder](crate::telemetry::Recorder).
    /// Without a sink, events cost nothing.
    pub fn set_telemetry(&mut self, sink: Arc<dyn EventSink<EngineEvent>>) {
        self.telemetry.install(sink);
    }

    /// Returns a handle that cancels the method the engine is running.
    ///
    /// The handle can be cloned and used from any thread,
    /// and every handle of the engine sets the same flag.
    /// [CancelToken] says which methods check it.
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }

    /// Links a program: instantiates every module it uses with `@use`,
    /// and resolves every module-qualified name.
    ///
    /// The result is a program that the loading methods accept.
    /// A program without a `@use` is returned unchanged.
    /// A `@use` names a module relative to the directory of the file that contains it,
    /// or to the working directory for a program without a file,
    /// or inside a [module root](Self::add_module_root).
    /// A text registered with [add_module](Self::add_module) is read instead of the module's file.
    /// Warnings are added to the [reports](Self::reports).
    ///
    /// # Errors
    ///
    /// Returns the first problem with the modules:
    /// a file that cannot be read or does not parse, a cycle, an alias used twice,
    /// a name that an instance does not offer, or a binding or a parameter that does not fit.
    pub fn link(&mut self, program: Program) -> Result<Program, Diagnostic> {
        self.link_program(program)
    }

    /// Loads a program: stores its facts, reads its imports, and compiles its rules.
    ///
    /// The program becomes the written program:
    /// results, traces and diagnostics refer to its statements,
    /// not to what the pipeline stages rewrite them into.
    /// A program that uses modules must be [linked](Self::link) first.
    /// A relative resource is resolved against the
    /// [import directory](Self::set_import_directory) if one is set,
    /// and otherwise against the directory of the file that contains the import.
    ///
    /// Remote imports are fetched through the engine's [client](Self::set_client)
    /// before anything is registered or stored.
    /// Facts and option values evaluate `NOW()` at the same instant as the next run
    /// (see [reason](Self::reason)).
    ///
    /// # Errors
    ///
    /// - Returns N0481 if the engine already holds a program
    ///   (see [has_program](Self::has_program)).
    /// - Returns N0490 for a program that is not linked.
    /// - Returns N0493 if another method is suspended for data from the network
    ///   (see [pending](Self::pending)).
    /// - Returns N0491 if the imports need data from the network but the engine has no client,
    ///   and N0492 if the [cancel token](Self::cancel_token) stops the wait for the responses.
    ///   The load then changes nothing.
    /// - Otherwise, returns the first problem of the program or its data.
    ///
    /// A load that fails registers no predicate, so another program can be loaded instead.
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn load(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.cancel.clear();
        let progress = self.start_load(program);
        self.complete(progress)
    }

    /// Loads a program as [load](Self::load) does, but sends no request.
    ///
    /// If the imports need data from the network, it returns [Progress::Suspended] with their
    /// requests, having registered and stored nothing.
    /// [resume](Self::resume) then continues the load with the responses.
    ///
    /// # Errors
    ///
    /// Returns what [load](Self::load) returns, except N0491 and N0492.
    ///
    /// # Panics
    ///
    /// Panics as [load](Self::load) does.
    pub fn start_load(&mut self, program: Program) -> Result<Progress, Diagnostic> {
        self.refuse_while_suspended("load")?;
        self.choosing_context(|engine| engine.load_program(program))
    }

    /// Extends the loaded program with new statements.
    ///
    /// The new facts are stored and the new imports read.
    /// The whole program is compiled again, since its strata and join orders depend on every
    /// rule.
    /// Every table is kept, and every loaded rule continues semi-naive evaluation where it
    /// left off.
    /// The next [reason](Self::reason) continues to the new fixpoint,
    /// and evaluates `NOW()` at the same instant as the extension's facts and option values.
    /// If a new statement adds facts to a predicate that a rule reads under negation or
    /// aggregation, the derived facts that depend on it are dropped and derived again.
    ///
    /// # Errors
    ///
    /// - Returns N0482 if no program is loaded, N0490 for an extension that is not linked,
    ///   and N0493 if another method is suspended for data from the network.
    /// - Returns N0488, N0483 or N0484 for an extension that declares a `@base` other than the
    ///   loaded program's, binds one of its prefixes to another IRI, or declares one of its
    ///   parameters again.
    ///   These would change the meaning of the loaded statements,
    ///   so they need [reload](Self::reload).
    /// - Returns N0491 or N0492 as [load](Self::load) does, with the loaded program unchanged.
    /// - Otherwise, returns the first problem of the extension or its data.
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn extend(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.cancel.clear();
        let progress = self.start_extend(program);
        self.complete(progress)
    }

    /// Extends the loaded program as [extend](Self::extend) does, but sends no request.
    ///
    /// If the extension's imports need data from the network, it returns
    /// [Progress::Suspended] with their requests, with the loaded program unchanged.
    /// [resume](Self::resume) then continues the extension with the responses.
    ///
    /// # Errors
    ///
    /// Returns what [extend](Self::extend) returns, except N0491 and N0492.
    ///
    /// # Panics
    ///
    /// Panics as [extend](Self::extend) does.
    pub fn start_extend(&mut self, program: Program) -> Result<Progress, Diagnostic> {
        self.refuse_while_suspended("extend")?;
        self.choosing_context(|engine| engine.extend_program(program))
    }

    /// Replaces the loaded program, which is how statements are retracted.
    ///
    /// Every derived row is dropped, every rule starts over,
    /// and the next [reason](Self::reason) derives everything again from the input facts.
    /// It evaluates `NOW()` at the same instant as the program's facts and option values.
    ///
    /// The dictionary and the registered sources and resources are kept.
    /// So are the input facts of every predicate whose defining statements
    /// (facts, imports, declarations) are unchanged, and in particular,
    /// unchanged imports are not read again.
    /// Every predicate whose defining statements changed is *rebuilt*.
    /// Any other changed directive, such as `@prefix`, `@base`, `@parameter` or `@type`,
    /// can change the meaning of any value, so it rebuilds every predicate.
    /// The rows added through [import_rows](Self::import_rows) to a rebuilt predicate
    /// cannot be reproduced from statements, so they are dropped, with a warning.
    ///
    /// # Errors
    ///
    /// - Returns N0482 if no program is loaded, N0490 for a program that is not linked,
    ///   and N0493 if another method is suspended for data from the network.
    /// - Returns N0491 or N0492 as [load](Self::load) does, with the loaded program unchanged.
    /// - Otherwise, returns the first problem of the program or its data.
    ///
    /// # Panics
    ///
    /// Panics if a pipeline stage rewrites the program into one that translation refuses,
    /// although the program as written passes: the stage has a bug.
    pub fn reload(&mut self, program: Program) -> Result<(), Diagnostic> {
        self.cancel.clear();
        let progress = self.start_reload(program);
        self.complete(progress)
    }

    /// Replaces the loaded program as [reload](Self::reload) does, but sends no request.
    ///
    /// If the changed imports need data from the network, it returns [Progress::Suspended]
    /// with their requests, with the loaded program unchanged.
    /// [resume](Self::resume) then continues the reload with the responses.
    ///
    /// # Errors
    ///
    /// Returns what [reload](Self::reload) returns, except N0491 and N0492.
    ///
    /// # Panics
    ///
    /// Panics as [reload](Self::reload) does.
    pub fn start_reload(&mut self, program: Program) -> Result<Progress, Diagnostic> {
        self.refuse_while_suspended("reload")?;
        self.choosing_context(|engine| engine.reload_program(program))
    }

    /// Checks a program without loading it.
    ///
    /// The check resolves the parameters, validates the directives, compiles the rules,
    /// and stratifies the whole program, but reads no imported data and stores no facts.
    /// A parameter declared without a value, which a load must be given, stands for any value
    /// of its declared type, as an `@input` predicate stands for any rows its declaration allows.
    ///
    /// Unlike [load](Self::load), a check does not stop at the first problem.
    /// It reports every problem it found, warnings included,
    /// with the types that the type analysis inferred.
    /// Since it reads no data, a check suits a program while it is edited.
    /// A check stores nothing, so the engine can still load a program afterwards.
    /// Like a load, it needs a [linked](Self::link) program and an engine without a program.
    /// Otherwise, the report contains the error that [load](Self::load) would return.
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

    /// Registers a program text, parses and [links](Self::link) it,
    /// and passes the result to `call`.
    ///
    /// `call` is a loading method, such as [load](Self::load), or another function that
    /// accepts a linked program.
    /// If parsing reports an error, the text is rejected with everything parsing reported.
    /// Warnings alone do not stop it.
    ///
    /// # Errors
    ///
    /// Returns the diagnostics of the step that failed:
    /// registration, parsing, linking, or `call`.
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
    /// The report contains what parsing, linking and [check](Self::check) reported,
    /// warnings included, or the error of a failed registration.
    pub fn check_source(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> CheckReport {
        self.check_text(name, text)
    }

    /// Adds rows computed in memory, rather than read from a file, to a predicate.
    ///
    /// The rows are checked against the program as its facts are,
    /// but the pipeline stages do not see them,
    /// and a [reload](Self::reload) keeps them unless it rebuilds their predicate,
    /// which it reports.
    /// Before [load](Self::load), the rows are kept, and the load checks and stores them with
    /// the program's facts.
    /// A load that fails keeps them, so that a corrected program can be loaded with them.
    /// [discard_rows](Self::discard_rows) drops a predicate's kept rows, for rows that need
    /// correcting themselves.
    /// After a load, the rows are checked against the loaded program,
    /// and stored in the predicate's table as a new revision.
    /// The next [reason](Self::reason) treats them as new facts and continues to the new
    /// fixpoint, dropping and deriving again the facts that depend on the predicate through
    /// negation or aggregation.
    /// The predicate is given by its resolved name, as for [rows](Self::rows).
    ///
    /// # Errors
    ///
    /// A refused call adds no row.
    /// A row that the program refuses gets the diagnostic that a fact with its values would
    /// get, with a note that names the row; before a load, the load returns it.
    ///
    /// - After a load, returns N0486 for a predicate that the program does not use and that
    ///   no rows were given to, since only [extend](Self::extend) introduces new ones.
    /// - Returns N0487 for a row that contains a null, since nulls are only created by
    ///   reasoning.
    /// - Returns N0254 for a row with another number of values than the predicate's other
    ///   rows or than the program uses it with,
    ///   and N0460 for a value that the predicate's declaration does not allow.
    /// - Returns N0493 if a method is suspended for data from the network.
    pub fn import_rows(
        &mut self,
        predicate: &str,
        rows: impl IntoIterator<Item = Vec<DataValue>>,
    ) -> Result<(), Diagnostic> {
        self.refuse_while_suspended("import_rows")?;
        self.choosing_context(|engine| engine.admit_rows(predicate, rows).map(|()| Progress::Done))
            .map(drop)
    }

    /// Drops the rows given through [import_rows](Self::import_rows) to a predicate that wait
    /// for the first [load](Self::load), and returns whether there were any.
    ///
    /// Rows that a load stored stay in the predicate's table.
    /// The predicate is given by its resolved name, as for [import_rows](Self::import_rows).
    ///
    /// # Errors
    ///
    /// Returns N0493 if a method is suspended for data from the network,
    /// since a suspended load reads the kept rows when it continues.
    pub fn discard_rows(&mut self, predicate: &str) -> Result<bool, Diagnostic> {
        self.refuse_while_suspended("discard_rows")?;
        Ok(self.unloaded_rows.discard(predicate))
    }

    /// Sets the client through which the plain methods ([load](Self::load),
    /// [extend](Self::extend), [reload](Self::reload), [reason](Self::reason)) send their
    /// requests for data from the network, or removes it.
    ///
    /// A new engine has the HTTP client of the `http` feature if it is built with that
    /// feature, and no client otherwise.
    /// Without a client, a plain method that needs data from the network fails.
    /// The `start_*` methods, such as [start_load](Self::start_load), never send a request:
    /// they return the requests, and [resume](Self::resume) accepts the responses.
    ///
    /// Returns the client that was set before, which can be set again later.
    pub fn set_client(
        &mut self,
        client: Option<Box<dyn Client + Send>>,
    ) -> Option<Box<dyn Client + Send>> {
        std::mem::replace(&mut self.client, ClientSlot(client)).0
    }

    /// Returns the requests that the suspended method needs responses to,
    /// or none if no method is suspended.
    ///
    /// A method is suspended after a `start_*` method or [resume](Self::resume) returned
    /// [Progress::Suspended].
    /// A run is also suspended after the [cancel token](Self::cancel_token) stopped
    /// [reason](Self::reason) while its client waited for the responses of a fetch.
    pub fn pending(&self) -> Vec<PendingRequest> {
        self.suspension
            .as_ref()
            .map_or_else(Vec::new, Suspension::pending)
    }

    /// Continues the suspended method with responses to its requests.
    ///
    /// Each response is given under the id that [pending](Self::pending) listed for its
    /// request.
    /// It is either the body's bytes or the failure that prevented them.
    /// While some request has no response yet, the method stays suspended,
    /// and the result lists the requests still pending.
    /// Once every request has a response, the method continues where it was suspended:
    /// a loading method completes its translation,
    /// and a run continues at the rule application that fetched.
    /// It may then be suspended again, for new requests.
    ///
    /// A failure is final, so retry a failed request before passing its failure in.
    /// A run's request that was too large for the server (status 413 or 414),
    /// or whose response the server cut off partway, typically at its time limit,
    /// is listed again in smaller pieces if the engine can split it.
    /// With no method suspended and no response given, this does nothing.
    ///
    /// # Errors
    ///
    /// Returns what the continued method returns.
    ///
    /// # Panics
    ///
    /// Panics if a response is given under an id that [pending](Self::pending) does not list,
    /// or two responses under one id, and as the continued method does.
    pub fn resume(
        &mut self,
        responses: impl IntoIterator<Item = (RequestId, Response)>,
    ) -> Result<Progress, Diagnostic> {
        self.cancel.clear();
        self.pass_responses(responses)
    }

    /// Discards the suspended method, if there is one, and returns whether there was.
    ///
    /// A suspended loading method has changed nothing, so the engine is as before the method.
    /// A suspended run stops with the tables consistent, as after a cancellation,
    /// and reports the requests that its fetches skipped so far.
    /// The next [reason](Self::reason) continues toward the fixpoint.
    pub fn discard_pending(&mut self) -> bool {
        let suspended = self.suspension.is_some();
        self.abandon();
        suspended
    }

    /// Evaluates the rules semi-naively to the fixpoint, stratum by stratum.
    ///
    /// The predicates that a stratum negates are final before the stratum starts.
    /// A run evaluates in one [context](EvaluationContext), so every `NOW()` in it reads the
    /// same instant.
    /// The facts loaded since the last run evaluated their `NOW()` at this instant too.
    /// The instant is that of the first successful call, since the last run,
    /// of a loading method or of [import_rows](Self::import_rows) after a load.
    /// Without such a call, it is the instant at which the run starts.
    /// The bound imports, which are the imports with a `where` clause, fetch their data through
    /// the engine's [client](Self::set_client).
    /// If the cancel token stopped a run while its client waited for responses,
    /// the run is still suspended, and this method continues it.
    ///
    /// # Errors
    ///
    /// - Returns N0482 if no program is loaded,
    ///   and N0493 if a loading method is suspended for data from the network.
    /// - Returns N0207 if the [cancel token](Self::cancel_token) is cancelled.
    ///   The run checks the token between rule applications,
    ///   and stops there with the tables consistent and its progress recorded.
    ///   A later call continues toward the fixpoint.
    /// - Returns N0492 if the token stops the wait for the responses of a fetch.
    ///   The run then stays suspended, and a later call continues it.
    /// - Returns N0491 if a bound import needs data from the network but the engine has no
    ///   client. The run then stops as after a cancellation.
    /// - Otherwise, returns the first failure of a rule application.
    pub fn reason(&mut self) -> Result<(), Diagnostic> {
        let progress = self.start_reason();
        self.complete(progress)
    }

    /// Reasons as [reason](Self::reason) does, but sends no request.
    ///
    /// If a bound import needs data from the network, it returns [Progress::Suspended] with
    /// the requests.
    /// [resume](Self::resume) then continues the run at that rule application with the
    /// responses.
    /// If a run is already suspended, it returns that run's pending requests again.
    ///
    /// # Errors
    ///
    /// Returns what [reason](Self::reason) returns, except N0491 and N0492.
    pub fn start_reason(&mut self) -> Result<Progress, Diagnostic> {
        self.cancel.clear();
        match &self.suspension {
            Some(Suspension::Load(_)) => self.refuse_while_suspended("reason")?,
            Some(suspended) if suspended.has_pending() => {
                return Ok(Progress::Suspended(self.pending()));
            }
            _ => {}
        }
        self.reason_until_suspended()
    }

    /// Starts or continues a run, until the fixpoint or until the run is suspended for data
    /// from the network.
    ///
    /// The run's reports and error are rewritten to refer to the written program.
    pub(in crate::engine) fn reason_until_suspended(&mut self) -> Result<Progress, Diagnostic> {
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

    /// Returns the description of a predicate of the program,
    /// or `None` for a name the program never used.
    ///
    /// Its [role](PredicateRole) follows the written program.
    /// A predicate is derived if a written rule has it in a head.
    /// It is extensional if a written fact, import or `@input` produces facts for it,
    /// or if rows were added to it through [import_rows](Self::import_rows).
    /// A predicate that is only read counts as extensional, even if it holds no facts.
    pub fn predicate(&self, name: &str) -> Option<PredicateInfo> {
        Some(PredicateInfo {
            name: name.to_owned(),
            arity: self.arity_of(name)?,
            role: self.role_of(name),
        })
    }

    /// Returns the program's outputs.
    ///
    /// They are the predicates that the program declares `@output` or exports,
    /// in declaration order.
    /// If it declares neither, they are the predicates that its rules derive, sorted by name.
    /// An output is stored whole,
    /// so [rows](Self::rows) and [count](Self::count) deliver all of its facts.
    pub fn outputs(&self) -> Vec<String> {
        self.output_names()
    }

    /// Returns the types of the values that each column of a predicate can hold,
    /// as the type analysis inferred them.
    ///
    /// Each type is an upper bound, which includes the types of the nested fields of tuples
    /// and maps.
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

    /// Returns the number of input facts that the program's predicates hold:
    /// the rows of its facts and imports, and the rows added through
    /// [import_rows](Self::import_rows).
    pub fn loaded_facts(&self) -> usize {
        self.fact_counts().0
    }

    /// Returns the number of facts that reasoning stored in the program's predicates,
    /// including the rows that bound imports fetched.
    pub fn derived_facts(&self) -> usize {
        self.fact_counts().1
    }

    /// Returns the stored rows of a predicate, decoded into values.
    ///
    /// This reads a [cursor](Self::cursor) to its end in one call.
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

    /// Opens a cursor over a predicate's stored rows, for [read_chunk](Self::read_chunk) to read.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded, N0486 for a predicate the program does not have,
    /// and N0489 for one the engine does not store whole, naming what is left out.
    pub fn cursor(&self, predicate: &str) -> Result<PredicateCursor, Diagnostic> {
        self.open_cursor(predicate)
    }

    /// Returns the next rows of a cursor, at most `budget` of them, decoded into a [RowChunk].
    ///
    /// The chunk is empty once the cursor has delivered every row.
    /// The cursor holds no borrow of the engine, and each read borrows the engine only while it
    /// runs.
    ///
    /// # Errors
    ///
    /// Returns N0206 for a cursor whose table changed after the cursor was opened,
    /// since its remaining rows would no longer match the table.
    /// [is_current](Self::is_current) checks for this beforehand.
    pub fn read_chunk(
        &mut self,
        cursor: &mut PredicateCursor,
        budget: usize,
    ) -> Result<RowChunk, Diagnostic> {
        self.decode_chunk(cursor, budget)
    }

    /// Returns whether a cursor still reads the rows it was opened on,
    /// which holds while nothing changes its predicate's table.
    ///
    /// A cursor on a predicate that a [reload](Self::reload) rebuilt is not current.
    /// [read_chunk](Self::read_chunk) refuses a cursor that is not current.
    pub fn is_current(&self, cursor: &PredicateCursor) -> bool {
        // A reload drops the table of a predicate that it rebuilds,
        // and the table manager answers only for the tables it holds.
        let held = self
            .predicates
            .values()
            .any(|registered| registered.table == cursor.table);
        // The blocks are compared first, since counting the rows of a range panics if the range
        // does not align with the table's blocks, which a merge of blocks can cause.
        held && self.manager.blocks(cursor.table) == cursor.blocks
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

    /// Returns the program's export directives as specifications, in directive order,
    /// or none before [load](Self::load).
    pub fn exports(&self) -> &[ExportSpec] {
        self.program
            .as_ref()
            .map_or(&[], |program| program.exports.as_slice())
    }

    /// Writes the rows of every export directive into the given directory.
    ///
    /// Each export is written to the file that [ExportSpec::file_name] names,
    /// or to the standard output if its resource name is empty.
    /// A predicate the program never used is written as an empty file.
    /// Rows with values that do not fit a declared column format are dropped, with a warning.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded, and the failure to write a file.
    pub fn export(&mut self, directory: impl AsRef<std::path::Path>) -> Result<(), Diagnostic> {
        let specs = self.exports().to_vec();
        self.export_specs(directory, &specs)
    }

    /// Writes the rows of the given export specifications into the given directory,
    /// as [export](Self::export) does.
    ///
    /// The specifications can also be built directly,
    /// for example to write a predicate that the program does not export,
    /// or to write in another serialization.
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

    /// Returns the bytes that [export_specs](Self::export_specs) would write to the file of one
    /// export specification, compression included.
    ///
    /// This serves environments without a filesystem, such as browsers,
    /// and exports that are sent somewhere other than a file.
    ///
    /// # Errors
    ///
    /// Returns N0486 or N0489 for a predicate the engine does not hold or does not store whole,
    /// and the failure to serialize the rows.
    pub fn export_bytes(&mut self, spec: &ExportSpec) -> Result<Vec<u8>, Diagnostic> {
        self.serialize_export(spec)
    }

    /// Traces facts: returns for each whether it holds after reasoning,
    /// and if it does, the derivation that explains why.
    ///
    /// The derivations of all the facts form one graph of facts,
    /// in the types of [trace](crate::trace).
    /// The facts are ground atoms over the program's predicates,
    /// and their named arguments are resolved as in the program text.
    /// Traces describe the facts at the fixpoint,
    /// so the engine must be [at the fixpoint](Self::is_at_fixpoint).
    ///
    /// # Errors
    ///
    /// - Returns N0482 if no program is loaded,
    ///   N0209 if the engine holds updates that [reason](Self::reason) has not processed,
    ///   and N0510 if the last run did not reach the fixpoint.
    /// - Returns an error for a fact that names no predicate of the program,
    ///   or that does not fit its predicate.
    pub fn trace(&mut self, facts: &[Fact]) -> Result<TraceResponse, Diagnostic> {
        self.traceable()?;
        let resolved = self.ground_facts(facts)?;
        trace::proof::run(self, resolved)
    }

    /// Traces every stored row of the given predicates, and then the given facts,
    /// as [trace](Self::trace) does.
    ///
    /// Returns the traced facts, each with its node.
    /// The response is passed to `sink` in [parts](crate::trace::TracePart) as it grows,
    /// one batch of rows at a time.
    /// Each fact is proven once and becomes one node,
    /// which is listed after the nodes of its premises.
    /// Every proof is kept in memory until the trace ends.
    ///
    /// # Errors
    ///
    /// Returns what [trace](Self::trace) returns for the facts,
    /// what [cursor](Self::cursor) returns for a predicate,
    /// and the first error of `sink`, which ends the tracing.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo::{
    ///     engine::Engine,
    ///     model::{FileId, Program},
    /// };
    ///
    /// let (program, _) = Program::parse(
    ///     "edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) .",
    ///     FileId::new(0),
    /// );
    /// let mut engine = Engine::new();
    /// engine.load(program)?;
    /// engine.reason()?;
    ///
    /// let mut parts = Vec::new();
    /// engine.trace_predicates(&["reach".to_owned()], &[], |part| {
    ///     parts.push(part);
    ///     Ok(())
    /// })?;
    ///
    /// // Node ids continue across the parts.
    /// let nodes: Vec<_> = parts.iter().flat_map(|part| &part.nodes).collect();
    /// let node = nodes
    ///     .iter()
    ///     .find(|node| node.fact == "reach(1, 2)")
    ///     .expect("the row is traced");
    /// let derivation = node.derivation.as_ref().expect("a rule derives the fact");
    /// let premises = derivation.premises.iter().map(|&premise| nodes[premise].fact.as_str());
    /// assert_eq!(node.inference(premises), "reach(1, 2) :- edge(1, 2) .");
    /// # Ok::<(), nemo::diagnostics::Diagnostic>(())
    /// ```
    pub fn trace_predicates(
        &mut self,
        predicates: &[String],
        facts: &[Fact],
        sink: impl FnMut(TracePart) -> Result<(), Diagnostic>,
    ) -> Result<Vec<TracedFact>, Diagnostic> {
        self.trace_in_parts(predicates, facts, sink)
    }

    /// Answers a `treeForTable` query of the [query format](crate::trace::query).
    ///
    /// The response contains the selected facts of a predicate, with their derivations
    /// expanded.
    ///
    /// # Errors
    ///
    /// Returns N0482, N0209 or N0510 as [trace](Self::trace) does,
    /// and an error for a query that does not fit the written program.
    pub fn tree_for_table(
        &mut self,
        query: crate::trace::query::TreeForTableQuery,
    ) -> Result<crate::trace::query::TreeForTableResponse, Diagnostic> {
        self.traceable()?;
        trace::query::tree_for_table(self, query)
    }

    /// Answers a `tableEntriesForTreeNodes` query of the [query format](crate::trace::query).
    ///
    /// The response lists, per node of the tree that the query describes,
    /// the facts consistent with all of its constraints, with parents before children.
    ///
    /// # Errors
    ///
    /// Returns N0482, N0209 or N0510 as [trace](Self::trace) does,
    /// and an error for a query that does not fit the written program.
    pub fn table_entries_for_tree_nodes(
        &mut self,
        query: crate::trace::query::TableEntriesForTreeNodesQuery,
    ) -> Result<Vec<crate::trace::query::NodeResponse>, Diagnostic> {
        self.traceable()?;
        trace::query::table_entries_for_tree_nodes(self, query)
    }

    /// Returns the rule with the given statement id, by which traces and queries refer to it.
    ///
    /// A statement id is the position of a statement in the written program.
    /// The rule is a rule of the written program or a bound import,
    /// with its text, `#[name]` and source position.
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
    /// [load](Self::load) needs an engine without a program,
    /// and most other methods need one with a program.
    pub fn has_program(&self) -> bool {
        self.program.is_some()
    }

    /// Returns a value in term syntax that the program reads back as the same value,
    /// for example in a fact given to [trace](Self::trace).
    ///
    /// Under a base IRI, a bare name is relative to the base,
    /// so every IRI is written in angle brackets, which the program parses as the IRI they
    /// enclose.
    pub fn term_text(&self, value: ValueRef<'_>) -> String {
        if self.translation.base_iri.is_some() {
            value.bracketed().to_string()
        } else {
            value.to_string()
        }
    }

    /// Returns a predicate's name written so that the program reads it back as the same name.
    ///
    /// The name is written bare if it is read back unchanged,
    /// and in angle brackets otherwise or under a base IRI, as for [term_text](Self::term_text).
    pub fn predicate_text(&self, predicate: &str) -> String {
        if self.translation.base_iri.is_some() {
            IriRef::new(predicate).bracketed().to_string()
        } else {
            crate::model::Identifier::bare(predicate).to_string()
        }
    }

    /// Returns whether the stored facts are at the fixpoint of the program,
    /// which holds if the last run reached it and nothing was added since.
    ///
    /// The tracing methods, such as [trace](Self::trace), require it.
    pub fn is_at_fixpoint(&self) -> bool {
        self.program.is_some() && self.dirty.is_empty() && self.fixpoint
    }

    /// Returns the number of rule applications so far.
    pub fn applications(&self) -> u64 {
        self.applied
    }

    /// Returns the wall-clock times and work counts of the reasoning so far.
    pub fn timings(&self) -> EngineTimings {
        let mut timings = self.timings;
        timings.stored_orders = self
            .predicates
            .values()
            .map(|registered| self.manager.stored_orders(registered.table) as u64)
            .sum();
        timings
    }

    /// Returns the number of bytes currently allocated by the stored tables and the dictionary.
    ///
    /// Transient buffers are not counted.
    /// Their high-water mark is
    /// [WorkCounts::buffer_bytes](nemo_physical::plan::WorkCounts::buffer_bytes)
    /// in [EngineTimings::counts].
    pub fn memory_bytes(&self) -> usize {
        self.manager.memory_bytes() + self.dictionary.memory().total()
    }

    /// Returns the written program, and the program after every pipeline stage that rewrote it.
    ///
    /// Each program comes with the name of its stage, or `"loaded"` for the written program,
    /// and with its text, which parses back to the same program.
    /// Returns none before [load](Self::load).
    pub fn revisions(&self) -> Vec<(&'static str, String)> {
        let Some(revision) = self.revision else {
            return Vec::new();
        };
        let mut revisions = Vec::new();
        let mut previous: Option<&[crate::pipeline::StatementId]> = None;
        for revision in self.pipeline.lineage(revision) {
            let statements = self.pipeline.revision_statements(revision);
            // A stage that rewrote nothing is left out.
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
    /// which are the rules after the pipeline stages rewrote them.
    /// Telemetry events and [join_order](Self::join_order) name rules by their position,
    /// while traces name them by statement id (see [rule](Self::rule)).
    /// A bound import is a rule here too, rendered as its directive.
    /// Returns none before [load](Self::load).
    pub fn ran_rules(&self) -> &[String] {
        self.program
            .as_ref()
            .map_or(&[], |program| program.texts.as_slice())
    }

    /// Returns the join order chosen for the rule at a [position](Self::ran_rules),
    /// as variable ids in the order the join binds them,
    /// or `None` if there is no rule at that position.
    pub fn join_order(&self, position: usize) -> Option<Vec<usize>> {
        self.program
            .as_ref()?
            .rules
            .get(position)
            .map(|rule| rule.order.clone())
    }

    /// Returns the diagnostics that the methods reported so far,
    /// such as the warnings of loading and reasoning.
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
