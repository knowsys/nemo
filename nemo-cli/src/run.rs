//! One command line run, from the program files to the written exports.
//!
//! [Run::perform] runs the stages in order: setting up the engine and its observers,
//! parsing the program, loading, settling the exports, reasoning, writing the results,
//! tracing, and reporting.
//! Each stage is a method of [Run], defined here or in the submodule for its concern.

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
    engine::{Engine, ExportSpec},
    model::{FileId, Program},
    telemetry::{EngineEvent, EventSink, LogSink, Recorder},
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
    /// The instant the run began, from which the total is measured.
    started: Instant,
    /// The time spent parsing the programs and supplying the parameters.
    parse: Duration,
    /// The time spent linking and loading the program.
    load: Duration,
    /// The time spent reasoning to the fixpoint.
    reason: Duration,
    /// The time spent writing or printing the results.
    export: Duration,
    /// The time from the start of the run until the results were written.
    total: Duration,
}

impl Times {
    /// Returns times that start now, with every stage at zero.
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

/// The engine's telemetry sink in a run.
///
/// It passes each event to the `tracing` stream, which the log lines and `--events` read,
/// and to the recorder, if the run has one.
struct RunSink {
    /// The recorder of the report and the dashboard, if the run has one.
    recorder: Option<Arc<Recorder>>,
}

impl EventSink<EngineEvent> for RunSink {
    fn record(&self, event: &EngineEvent) {
        LogSink.record(event);
        if let Some(recorder) = &self.recorder {
            recorder.record(event);
        }
    }

    fn enabled(&self) -> bool {
        self.recorder.is_some() || LogSink.enabled()
    }
}

/// Returns the diagnostic for an output that would share standard output with the JSON report.
///
/// `claimant` names that output, as "the printed results".
fn stdout_conflict(claimant: &str) -> Diagnostic {
    CliError::StdoutConflict {
        claimant: claimant.to_owned(),
    }
    .diagnostic()
}

/// One command line run, which reads the programs, reasons, writes the exports,
/// and reports what happened.
#[derive(Debug)]
pub(crate) struct Run<'a> {
    /// The command line arguments.
    arguments: &'a Arguments,
    /// The texts of the programs and their modules, against which diagnostics are shown.
    sources: SourceTexts,
    /// Whether a warning was reported, for the [exit code](Self::exit_code).
    warned: Cell<bool>,
    /// How many of the engine's reports have been shown.
    shown: Cell<usize>,
}

impl<'a> Run<'a> {
    /// Creates a run of the given arguments.
    pub(crate) fn new(arguments: &'a Arguments) -> Self {
        Self {
            arguments,
            sources: SourceTexts::new(),
            warned: Cell::new(false),
            shown: Cell::new(0),
        }
    }

    /// Performs the run, shows its error if it fails, and returns the process exit code.
    pub(crate) fn execute(mut self) -> ExitCode {
        match self.perform() {
            Ok(code) => code,
            Err(problem) => {
                self.show(&problem);
                ExitCode::FAILURE
            }
        }
    }

    /// Performs the stages of the run in order, and returns the exit code.
    ///
    /// It returns anything it cannot recover from as one diagnostic.
    /// A program that fails to parse is not returned as an error.
    /// Its diagnostics are shown, and the exit code is a failure.
    fn perform(&mut self) -> Result<ExitCode, Diagnostic> {
        if let Some(code) = &self.arguments.explain {
            // With `--explain`, explaining the code is all that the run does.
            explain::print(code)?;
            return Ok(ExitCode::SUCCESS);
        }
        let mut times = Times::start();
        let mut engine = self.engine()?;
        let level = self.arguments.reporting();
        self.reserve_stdout(level)?;
        let recorder = self.observe(&mut engine, level)?;
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

    /// Creates the engine with the settings of the arguments.
    ///
    /// These are the number of worker threads, the import directory, the module roots,
    /// and whether imports are loaded whole, which they are if the run writes or prints the
    /// extensional predicates, or traces.
    /// Returns an error for a module root that is not written as `name=directory` with a
    /// nonempty name, or whose name the engine rejects.
    fn engine(&self) -> Result<Engine, Diagnostic> {
        if let Some(threads) = self.arguments.threads {
            // The thread count is read by the first parallel phase, and none runs before the
            // engine is created.
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
            // Writing the extensional predicates shows the imported tables as they were
            // read, and a trace names facts by all their columns. So the imports must keep
            // the columns and rows that no rule reads.
            engine.retain_imports();
        }
        Ok(engine)
    }

    /// Returns an error if the report level is JSON and the printed results or a trace would
    /// also be written to standard output.
    ///
    /// It runs before any work is done.
    /// An export to standard output is checked once the exports are known,
    /// in [exports](Self::exports).
    fn reserve_stdout(&self, level: Report) -> Result<(), Diagnostic> {
        if level != Report::Json {
            return Ok(());
        }
        // The JSON document needs standard output to itself, since anything else written
        // there would make both unreadable.
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

    /// Installs the run's `tracing` subscriber and the engine's telemetry sink,
    /// and returns a recorder of the telemetry if the report or the dashboard needs one.
    ///
    /// The subscriber receives both the log lines and the engine's telemetry,
    /// as [logging::install] describes.
    fn observe(
        &self,
        engine: &mut Engine,
        level: Report,
    ) -> Result<Option<Arc<Recorder>>, Diagnostic> {
        let recorder = (level.observes() || self.arguments.dashboard.is_some())
            .then(|| Arc::new(Recorder::new()));
        logging::install(self.arguments)?;
        engine.set_telemetry(Arc::new(RunSink {
            recorder: recorder.clone(),
        }));
        Ok(recorder)
    }

    /// Reads and parses the programs into one program, and gives the engine the `--param`
    /// values.
    ///
    /// Returns `None` if a program failed to parse, after showing its diagnostics.
    fn program(
        &mut self,
        engine: &mut Engine,
        times: &mut Times,
    ) -> Result<Option<Program>, Diagnostic> {
        // The engine assigns the file ids, since it also reads files that the command line
        // does not name: the modules that a program uses. Registering every text through
        // the engine before parsing gives all files one numbering.
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
        let Some(program) = self.parse(engine, &programs) else {
            return Ok(None);
        };
        for parameter in &parameters {
            self.supply(engine, parameter)?;
        }
        times.parse = start.elapsed();
        Ok(Some(program))
    }

    /// Parses the programs into one program and shows their diagnostics,
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

    /// Links and loads the program: its modules, its imports and its facts.
    ///
    /// The engine's client fetches the remote imports.
    /// The engine's reports are shown before an error is returned,
    /// so that a failed stage still shows the warnings that came before the failure.
    /// The recorder, if there is one, receives the texts of the compiled program's rules.
    fn load(
        &mut self,
        engine: &mut Engine,
        program: Program,
        times: &mut Times,
        recorder: Option<&Recorder>,
    ) -> Result<(), Diagnostic> {
        let start = Instant::now();
        let loaded = engine.link(program).and_then(|linked| engine.load(linked));
        times.load = start.elapsed();
        // Linking reads the program's modules, so their sources are added before any
        // diagnostic is shown against them.
        self.sources.absorb(engine.sources());
        self.reports(engine);
        loaded?;
        if let Some(recorder) = recorder {
            recorder.set_rules(engine.compiled_rules().to_vec());
        }
        Ok(())
    }

    /// Reasons to the fixpoint, and shows the engine's reports and the rows that rule
    /// evaluation dropped.
    ///
    /// The engine's client fetches the remote data that reasoning needs.
    fn reason(&self, engine: &mut Engine, times: &mut Times) -> Result<(), Diagnostic> {
        let start = Instant::now();
        let reasoned = engine.reason();
        times.reason = start.elapsed();
        self.reports(engine);
        self.attrition(engine);
        reasoned
    }

    /// Writes the results, either printed to standard output or exported as `specifications`
    /// say.
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

    /// Shows the engine's reports that have not been shown yet.
    fn reports(&self, engine: &Engine) {
        let reports = engine.reports();
        for report in &reports[self.shown.get().min(reports.len())..] {
            self.show(report);
        }
        self.shown.set(reports.len());
    }

    /// Shows a warning for each kind of error for which rule evaluation dropped rows.
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

    /// Writes a diagnostic to the error stream, and notes a warning for the exit code.
    ///
    /// A warning is noted even with `--no-warnings`, which only keeps it from being written.
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
    /// 3 if a warning was reported without `--no-warnings`, and 0 otherwise.
    fn exit_code(&self) -> ExitCode {
        if self.warned.get() && !self.arguments.no_warnings {
            ExitCode::from(WARNED)
        } else {
            ExitCode::SUCCESS
        }
    }
}

#[cfg(test)]
mod tests {
    use nemo::telemetry::TARGET;
    use tracing_subscriber::{
        filter::{LevelFilter, Targets},
        layer::SubscriberExt,
    };

    use super::*;

    /// The run's sink asks for events when the recorder or a subscriber of the telemetry target
    /// takes them, and only then.
    ///
    /// The case without a subscriber comes first, since `tracing` caches the interest of a
    /// callsite per process, across all live dispatchers.
    #[test]
    fn the_run_sink_is_enabled_for_a_consumer_only() {
        let alone = RunSink { recorder: None };
        let recording = RunSink {
            recorder: Some(Arc::new(Recorder::new())),
        };
        assert!(!alone.enabled(), "nothing takes the events");
        assert!(recording.enabled(), "the recorder takes the events");

        let listening = tracing_subscriber::registry()
            .with(Targets::new().with_target(TARGET, LevelFilter::TRACE));
        tracing::subscriber::with_default(listening, || {
            assert!(alone.enabled(), "the subscriber takes the events");
        });
    }
}
