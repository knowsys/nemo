//! The report document of a run, and where it goes.
//!
//! The document combines the run's facts, such as its stage times and counts,
//! with the recorded telemetry.
//! It is written to standard output as JSON or to the export directory as an HTML page,
//! and the live dashboard serves it while the run goes on.

use std::{sync::Arc, time::Duration};

use nemo::{
    engine::Engine,
    telemetry::{ErrorCount, PredicateSize, ProgramText, Recorder, ReportDocument, RunFacts},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    arguments::Report,
    dashboard::Dashboard,
    error::CliError,
    run::{Part, Run, Times},
    stdout,
};

/// The name of the HTML report's file in the export directory.
pub(super) const REPORT_PAGE: &str = "report.html";

impl Run<'_> {
    /// Returns the run's title, which lists the program paths as they were given.
    fn title(&self) -> String {
        let names: Vec<String> = self
            .arguments
            .programs
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        names.join(", ")
    }

    /// Returns the facts of the finished run for the report document.
    ///
    /// These are the stage times, the counts of facts and rule applications, the memory,
    /// the reasoning timings, the dropped rows, the size of each predicate,
    /// and the program's text after each pipeline stage.
    pub(super) fn facts(&self, engine: &Engine, times: Times) -> RunFacts {
        let nanos = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let errors = engine
            .evaluation_errors()
            .iter()
            .map(|(kind, count)| ErrorCount {
                kind: kind.to_string(),
                count,
            })
            .collect();
        // A predicate whose rows cannot be counted is left out,
        // for example one that the engine does not store whole.
        let predicates = engine
            .predicates()
            .into_iter()
            .filter_map(|info| {
                let rows = engine.count(&info.name).ok()?;
                Some(PredicateSize {
                    name: info.name,
                    columns: info.arity,
                    rows: rows as u64,
                })
            })
            .collect();
        RunFacts {
            parse: nanos(times.parse),
            load: nanos(times.load),
            reason: nanos(times.reason),
            export: nanos(times.export),
            total: nanos(times.total),
            trace: self.arguments.traces().then(|| nanos(times.trace)),
            loaded: engine.loaded_facts() as u64,
            derived: engine.derived_facts() as u64,
            applications: engine.applications(),
            memory: engine.memory_bytes() as u64,
            timings: engine.timings(),
            errors,
            predicates,
            programs: engine
                .revisions()
                .into_iter()
                .map(|(stage, text)| ProgramText {
                    stage: stage.to_owned(),
                    text,
                })
                .collect(),
        }
    }

    /// Writes the report document as `level` selects.
    ///
    /// [Report::Json] writes it to standard output as JSON,
    /// and [Report::Html] writes it to the export directory as a page.
    /// Other levels write nothing.
    /// `facts` are the facts of the finished run,
    /// and `recorder` is the recorder of the telemetry, if the run recorded any.
    pub(super) fn document(
        &self,
        level: Report,
        facts: Option<&RunFacts>,
        recorder: Option<&Recorder>,
    ) -> Result<(), Diagnostic> {
        let title = self.title();
        let document = ReportDocument {
            title: &title,
            run: facts,
            telemetry: recorder,
        };
        match level {
            Report::Json => stdout::write(&format!("{}\n", document.json())),
            Report::Html => self.page(&document),
            _ => Ok(()),
        }
    }

    /// Writes `document` into the export directory as an HTML page,
    /// and prints the page's path to the error stream.
    fn page(&self, document: &ReportDocument<'_>) -> Result<(), Diagnostic> {
        let path = self.arguments.export_dir.join(REPORT_PAGE);
        let page = document.page(false);
        std::fs::create_dir_all(&self.arguments.export_dir)
            .and_then(|()| std::fs::write(&path, page))
            .map_err(|error| {
                CliError::ReportUnwritable {
                    path: path.display().to_string(),
                    reason: error.to_string(),
                }
                .diagnostic()
            })?;
        self.begin(Part::Summary);
        eprintln!("report: {}", path.display());
        Ok(())
    }

    /// Starts the live dashboard if `--dashboard` is given,
    /// and prints its address to the error stream.
    ///
    /// `recorder` must be given whenever `--dashboard` is.
    pub(super) fn dashboard(
        &self,
        recorder: Option<&Arc<Recorder>>,
    ) -> Result<Option<Dashboard>, Diagnostic> {
        let Some(port) = self.arguments.dashboard else {
            return Ok(None);
        };
        let recorder = recorder.expect("the dashboard implies recording");
        let dashboard =
            Dashboard::start(port, self.title(), Arc::clone(recorder)).map_err(|error| {
                CliError::DashboardUnavailable {
                    port,
                    reason: error.to_string(),
                }
                .diagnostic()
            })?;
        eprintln!("dashboard: {}", dashboard.url());
        Ok(Some(dashboard))
    }

    /// Gives the dashboard the finished run's facts, if there are any,
    /// and keeps it serving until the process is interrupted.
    pub(super) fn serve(dashboard: Dashboard, facts: Option<RunFacts>) -> ! {
        if let Some(facts) = facts {
            dashboard.finish(facts);
        }
        eprintln!(
            "dashboard: serving at {} until interrupted",
            dashboard.url()
        );
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }
}
