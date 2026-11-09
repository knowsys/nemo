//! The global `tracing` subscriber of a run.
//!
//! It writes log lines to the error stream, passes the engine's telemetry stream to the
//! recorder, and writes the telemetry stream to the event log as JSON lines.

use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};

use nemo::telemetry::{Recorder, SinkLayer, TARGET};
use nemo_physical::diagnostics::Diagnostic;
use tracing_subscriber::{
    EnvFilter, Registry,
    filter::{LevelFilter, Targets},
    fmt,
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

/// One layer of the run's subscriber, over the shared registry.
type Boxed = Box<dyn Layer<Registry> + Send + Sync>;

use crate::{arguments::Arguments, error::CliError};

/// The environment variable that sets the default log filter.
pub(crate) const ENVIRONMENT: &str = "NMO_LOG";

/// Installs the run's subscriber.
///
/// The filter of the log lines comes from the first of these that is given:
/// - `--log`;
/// - `-v`, `-vv` or `-vvv`, for `info`, `debug` or `trace`;
/// - `--quiet`, for `error`;
/// - the `NMO_LOG` environment variable;
/// - otherwise `warn`.
///
/// `--log` and `NMO_LOG` accept full filter directives, such as `nemo::io=debug`.
/// The recorder, if given, receives the telemetry stream whatever the log filter,
/// and `--events` writes the same stream to a file as JSON lines.
pub(crate) fn install(
    arguments: &Arguments,
    recorder: Option<&Arc<Recorder>>,
) -> Result<(), Diagnostic> {
    let stderr = fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(filter(arguments)?);
    let mut layers: Vec<Boxed> = vec![Box::new(stderr)];
    if let Some(recorder) = recorder {
        let recording = SinkLayer::new(Arc::clone(recorder) as _).with_filter(SinkLayer::filter());
        layers.push(Box::new(recording));
    }
    if let Some(path) = &arguments.events {
        layers.push(events(path)?);
    }
    tracing_subscriber::registry().with(layers).init();
    Ok(())
}

/// Returns the filter of the log lines that the arguments select.
///
/// Fails for filter directives that do not parse.
fn filter(arguments: &Arguments) -> Result<EnvFilter, Diagnostic> {
    let directives = match (&arguments.log, arguments.verbose, arguments.quiet) {
        (Some(directives), _, _) => directives.clone(),
        (None, 1, _) => String::from("info"),
        (None, 2, _) => String::from("debug"),
        (None, 3.., _) => String::from("trace"),
        (None, 0, true) => String::from("error"),
        (None, 0, false) => match std::env::var(ENVIRONMENT) {
            Ok(directives) if !directives.is_empty() => directives,
            _ => String::from("warn"),
        },
    };
    EnvFilter::try_new(&directives).map_err(|error| {
        CliError::InvalidLogFilter {
            directives,
            reason: error.to_string(),
        }
        .diagnostic()
    })
}

/// Returns the layer that writes the telemetry stream to the file at `path` as JSON lines.
///
/// Fails if the file cannot be created.
fn events(path: &Path) -> Result<Boxed, Diagnostic> {
    let file = File::create(path).map_err(|error| {
        CliError::EventsUnwritable {
            path: path.display().to_string(),
            reason: error.to_string(),
        }
        .diagnostic()
    })?;
    let layer = fmt::layer()
        .json()
        .flatten_event(true)
        .with_target(false)
        .with_writer(Mutex::new(file))
        .with_filter(Targets::new().with_target(TARGET, LevelFilter::TRACE));
    Ok(Box::new(layer))
}
