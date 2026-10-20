//! The observability document of a run,
//! the application-level facts beside the recorded telemetry,
//! and its destinations:
//! the standard output as JSON, the export directory as a page, the live dashboard.

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
    run::{Run, Times},
    stdout,
};

/// The name of the HTML report's file in the export directory.
pub(super) const REPORT_PAGE: &str = "report.html";

impl Run<'_> {
    /// Returns the run's title, the program paths as they were given.
    fn title(&self) -> String {
        let names: Vec<String> = self
            .arguments
            .programs
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        names.join(", ")
    }

    /// Returns the application-level facts of the finished run, for the
    /// observability document.
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
        // A predicate whose rows cannot be counted, or which the engine
        // does not store whole, is left out; the summary reported the
        // former.
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

    /// Delivers the document as the report level asks for it:
    /// as the JSON on the standard output, which the run kept for it,
    /// or rendered as the page written into the export directory.
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

    /// Writes the report page over a document into the export directory,
    /// and says where it went.
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
        eprintln!("report: {}", path.display());
        Ok(())
    }

    /// Starts the live dashboard if one was asked for, and says where it serves.
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

    /// Keeps the dashboard serving, the finished run's facts included,
    /// until the process is interrupted.
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
