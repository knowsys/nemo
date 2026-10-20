//! One command line run, from source files to written exports.
//!
//! [Run::perform] is the run in stages:
//! the engine and what observes it, the program, loading, the exports settled before the work,
//! reasoning, writing, tracing, and the reports.
//! Each stage is a method here or in the module of its concern:
//! the selection of predicates, the export and print paths, the parameters, the traces,
//! the summary, and the observability document.

mod document;
mod exports;
mod parameters;
mod print;
mod selection;
mod summary;
mod trace;

use std::{
    cell::Cell,
    process::ExitCode,
    sync::Arc,
    time::{Duration, Instant},
};

use nemo::{
    diagnostics::render::{Renderer, SourceTexts},
    driver::HttpDriver,
    engine::{Engine, ExportSpec},
    model::{FileId, Program},
    telemetry::{Recorder, TracingSink},
};
use nemo_physical::diagnostics::{Diagnostic, Severity};

use crate::{
    arguments::{Arguments, Report},
    error::CliError,
    explain, logging,
};

/// The exit code of a run that completed with warnings.
const WARNED: u8 = 3;

/// The wall-clock times of a run's stages.
#[derive(Debug, Clone, Copy)]
struct Times {
    /// The instant the run began, which the total is measured from.
    started: Instant,
    /// Reading and parsing the programs.
    parse: Duration,
    /// Loading the imported data.
    load: Duration,
    /// Reasoning to the fixpoint.
    reason: Duration,
    /// Writing the exports.
    export: Duration,
    /// Everything, from the first file read.
    total: Duration,
}

impl Times {
    /// Times counted from now, no stage run yet.
    fn start() -> Self {
        Self {
            started: Instant::now(),
            parse: Duration::ZERO,
            load: Duration::ZERO,
            reason: Duration::ZERO,
            export: Duration::ZERO,
            total: Duration::ZERO,
        }
    }
}

/// Returns the diagnostic for something claiming the standard output the JSON
/// report writes its document to.
fn stdout_conflict(claimant: &str) -> Diagnostic {
    CliError::StdoutConflict {
        claimant: claimant.to_owned(),
    }
    .diagnostic()
}

/// One command line run: reads the programs, reasons, writes the
/// exports, and reports what happened.
#[derive(Debug)]
pub(crate) struct Run<'a> {
    /// What was asked for.
    arguments: &'a Arguments,
    /// The texts the run was read from, for resolving spans.
    sources: SourceTexts,
    /// Whether a warning was reported, which makes a completed run exit 3.
    warned: Cell<bool>,
    /// How many of the engine's reports have been shown.
    shown: Cell<usize>,
}

impl<'a> Run<'a> {
    /// Prepares a run of the given arguments.
    pub(crate) fn new(arguments: &'a Arguments) -> Self {
        Self {
            arguments,
            sources: SourceTexts::new(),
            warned: Cell::new(false),
            shown: Cell::new(0),
        }
    }

    /// Carries the run out, returning the process exit code.
    pub(crate) fn execute(mut self) -> ExitCode {
        match self.perform() {
            Ok(code) => code,
            Err(problem) => {
                self.show(&problem);
                ExitCode::FAILURE
            }
        }
    }

    /// Performs the run itself, stage by stage.
    ///
    /// Anything it cannot recover from surfaces as one diagnostic.
    fn perform(&mut self) -> Result<ExitCode, Diagnostic> {
        if let Some(code) = &self.arguments.explain {
            // Explaining a code is the whole of such a run.
            explain::print(code)?;
            return Ok(ExitCode::SUCCESS);
        }
        let mut times = Times::start();
        let mut engine = self.engine()?;
        let level = self.arguments.reporting();
        self.reserve_stdout(level)?;
        let recorder = self.observe(level)?;
        engine.set_telemetry(Arc::new(TracingSink));
        let dashboard = self.dashboard(recorder.as_ref())?;

        let Some(program) = self.program(&mut engine, &mut times)? else {
            return Ok(ExitCode::FAILURE);
        };
        self.load(&mut engine, program, &mut times, recorder.as_deref())?;
        let specifications = self.exports(&engine, level)?;
        self.reason(&mut engine, &mut times)?;
        self.write(&mut engine, &specifications, &mut times)?;
        self.trace(&mut engine)?;

        self.summarize(&engine, times, recorder.as_deref());
        let facts = recorder.is_some().then(|| self.facts(&engine, times));
        self.document(level, facts.as_ref(), recorder.as_deref())?;
        if let Some(dashboard) = dashboard {
            Self::serve(dashboard, facts);
        }
        Ok(self.exit_code())
    }

    /// Returns the engine, set up as the arguments ask:
    /// the worker threads, the import directory, the module roots,
    /// and the imports kept whole if the run shows the given predicates.
    ///
    /// Returns an error for a module root that is not `name=directory` with a name,
    /// or whose name cannot name a root.
    fn engine(&self) -> Result<Engine, Diagnostic> {
        if let Some(threads) = self.arguments.threads {
            // The first parallel phase reads the thread count, and none
            // runs before the engine is made.
            let applied = nemo_physical::parallel::Workers::set(threads as usize);
            debug_assert!(applied, "the thread count is set before any parallel phase");
        }
        let mut engine = Engine::new();
        if let Some(directory) = &self.arguments.import_dir {
            engine.set_import_directory(directory);
        }
        for setting in &self.arguments.module_root {
            let Some((name, directory)) = setting
                .split_once('=')
                .filter(|(name, _)| !name.trim().is_empty())
            else {
                return Err(CliError::InvalidModuleRoot {
                    setting: setting.clone(),
                }
                .diagnostic());
            };
            engine.add_module_root(name.trim(), directory)?;
        }
        if self.arguments.selects_extensional() || self.arguments.traces() {
            // Writing the given predicates shows tables verbatim, and a
            // trace names facts by their written columns; the loader must
            // not leave out what no rule happens to read.
            engine.retain_imports();
        }
        Ok(engine)
    }

    /// Returns an error, before any work, for what would share the
    /// standard output with the JSON report's document: the printed
    /// results, or a trace printed rather than written. An export to it
    /// is an error once the exports are known.
    fn reserve_stdout(&self, level: Report) -> Result<(), Diagnostic> {
        if level != Report::Json {
            return Ok(());
        }
        // The document owns stdout; anything else written there would
        // leave neither readable.
        if self.arguments.printing().is_some() {
            return Err(stdout_conflict("the printed results")
                .with_hint("pass only one of `--print` and `--report json`"));
        }
        if self.arguments.traces_to_stdout() {
            return Err(stdout_conflict("the trace")
                .with_hint("write the trace to a file with `--trace-output`"));
        }
        Ok(())
    }

    /// Installs the run's subscriber,
    /// which the log lines, the telemetry stream the engine puts on the `tracing` wire,
    /// and the event log meet in,
    /// with a recorder reading the stream if the report or the dashboard needs one.
    fn observe(&self, level: Report) -> Result<Option<Arc<Recorder>>, Diagnostic> {
        let recorder = (level.observes() || self.arguments.dashboard.is_some())
            .then(|| Arc::new(Recorder::new()));
        logging::install(self.arguments, recorder.as_ref())?;
        Ok(recorder)
    }

    /// Reads the programs and the `--param` settings,
    /// and parses them into the one program to run,
    /// or returns `None` if a program failed to parse, which was reported already.
    fn program(
        &mut self,
        engine: &mut Engine,
        times: &mut Times,
    ) -> Result<Option<Program>, Diagnostic> {
        // The engine hands out file ids, because it reads files this run
        // never names: the modules a program pulls in. Registering every
        // text through it before parsing is what keeps one numbering.
        let mut programs = Vec::with_capacity(self.arguments.programs.len());
        for path in &self.arguments.programs {
            let file = engine.read_source(path).map_err(|problem| {
                if Arguments::is_option_value(path) {
                    problem.with_hint(
                        "write an option's value with `=`, as `-P=idb` or `--dashboard=8080`",
                    )
                } else {
                    problem
                }
            })?;
            programs.push(file);
        }
        let mut parameters = Vec::with_capacity(self.arguments.param.len());
        for setting in &self.arguments.param {
            parameters.push(self.parameter(engine, setting)?);
        }
        self.sources.absorb(engine.sources());

        let start = Instant::now();
        let Some(mut program) = self.parse(engine, &programs) else {
            return Ok(None);
        };
        for parameter in &parameters {
            self.supply(engine, &mut program, parameter)?;
        }
        times.parse = start.elapsed();
        Ok(Some(program))
    }

    /// Parses every program into one, reporting as it goes,
    /// or returns `None` if any of them failed to parse.
    fn parse(&self, engine: &Engine, programs: &[FileId]) -> Option<Program> {
        let mut statements = Vec::new();
        let mut failed = false;
        for &file in programs {
            let (program, diagnostics) = engine.parse_source(file);
            for diagnostic in &diagnostics {
                self.show(diagnostic);
                failed |= diagnostic.severity() == Severity::Error;
            }
            statements.extend(program.statements);
        }
        (!failed).then(|| Program::new(statements))
    }

    /// Links and loads the program:
    /// its modules, its imports (remote ones fetched by the driver), and its facts.
    ///
    /// What the engine reports is shown as the stage ends,
    /// so a stage that fails does not bury the warnings before it.
    fn load(
        &mut self,
        engine: &mut Engine,
        program: Program,
        times: &mut Times,
        recorder: Option<&Recorder>,
    ) -> Result<(), Diagnostic> {
        let start = Instant::now();
        let loaded = engine
            .link(program)
            .and_then(|linked| HttpDriver::new().drive(engine, |engine| engine.load(linked)));
        times.load = start.elapsed();
        // Linking is what reads a program's modules, so the sources it
        // registered are taken in before anything is shown against them.
        self.sources.absorb(engine.sources());
        self.reports(engine);
        loaded?;
        if let Some(recorder) = recorder {
            recorder.set_rules(engine.ran_rules().to_vec());
        }
        Ok(())
    }

    /// Reasons to the fixpoint, the bound imports' requests fetched by the driver,
    /// showing what the engine reports and the rows it dropped on the way.
    fn reason(&self, engine: &mut Engine, times: &mut Times) -> Result<(), Diagnostic> {
        let start = Instant::now();
        let reasoned = HttpDriver::new().drive(engine, Engine::reason);
        times.reason = start.elapsed();
        self.reports(engine);
        self.attrition(engine);
        reasoned
    }

    /// Writes the results: printed to the standard output, or exported
    /// as specified.
    fn write(
        &self,
        engine: &mut Engine,
        specifications: &[ExportSpec],
        times: &mut Times,
    ) -> Result<(), Diagnostic> {
        let start = Instant::now();
        let written = match self.arguments.printing() {
            Some(mode) => self.print(engine, mode),
            None => engine.export_specs(&self.arguments.export_dir, specifications),
        };
        times.export = start.elapsed();
        times.total = times.started.elapsed();
        self.reports(engine);
        written
    }

    /// Shows the reports the engine has gathered beyond the ones already
    /// shown.
    fn reports(&self, engine: &Engine) {
        let reports = engine.reports();
        for report in &reports[self.shown.get().min(reports.len())..] {
            self.show(report);
        }
        self.shown.set(reports.len());
    }

    /// Reports the rows that rule evaluation dropped, if any did.
    fn attrition(&self, engine: &Engine) {
        for (kind, count) in engine.evaluation_errors().iter() {
            self.show(
                &CliError::DroppedDerivations {
                    count: count.into(),
                    kind: kind.to_string(),
                }
                .diagnostic(),
            );
        }
    }

    /// Writes a diagnostic to the error stream,
    /// a warning only if the run shows them, and notes it either way for the exit code.
    fn show(&self, diagnostic: &Diagnostic) {
        if diagnostic.severity() == Severity::Warning {
            self.warned.set(true);
            if self.arguments.no_warnings {
                return;
            }
        }
        eprint!("{}", Renderer::new(&self.sources).show(diagnostic));
    }

    /// Returns the exit code of a completed run:
    /// 3 if a warning was reported and warnings count, 0 otherwise.
    fn exit_code(&self) -> ExitCode {
        if self.warned.get() && !self.arguments.no_warnings {
            ExitCode::from(WARNED)
        } else {
            ExitCode::SUCCESS
        }
    }
}
