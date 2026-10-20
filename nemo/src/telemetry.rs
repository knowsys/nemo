//! Execution telemetry: the engine's event stream and its consumers.
//!
//! The engine emits typed [EngineEvent]s into an installed [EventSink] as it loads and reasons:
//! imports finishing, strata starting, rule applications with their phase accounting,
//! iterations with their sizes, and the end of the run.
//! Nothing is emitted until a sink is [installed](crate::engine::Engine::set_telemetry).
//!
//! The [Recorder] is the ready-made consumer:
//! it aggregates the stream into per-rule statistics and downsampled curves,
//! snapshotted as JSON at any moment.
//! A [ReportDocument] combines a recorder snapshot with the [RunFacts] of a finished run
//! into one JSON document.
//! The JSON report and the HTML page are both built from it.
//! The [PhaseTable] lists the phase accounting under the names all reports use,
//! the text summary included.
//!
//! Events can also be forwarded to `tracing`:
//! the [TracingSink] emits each one as a `tracing` event under the
//! [`nemo::telemetry`](TARGET) target,
//! where a `tracing` subscriber receives it along with the engine's log lines.
//! Behind the `subscriber` feature, the `SinkLayer` turns these back into events for any sink,
//! the recorder included.
//! Without a global `tracing` subscriber, a sink is installed on the engine directly:
//!
//! ```
//! use std::sync::Arc;
//! use nemo::{engine::Engine, model::{FileId, Program}, telemetry::Recorder};
//!
//! let (program, diagnostics) = Program::parse(
//!     "edge(1, 2) . edge(2, 3) .
//!      reach(?x, ?y) :- edge(?x, ?y) .
//!      reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
//!     FileId::new(0),
//! );
//! assert!(diagnostics.is_empty());
//!
//! let recorder = Arc::new(Recorder::new());
//! let mut engine = Engine::new();
//! engine.set_telemetry(recorder.clone());
//! engine.load(program)?;
//! recorder.set_rules(engine.ran_rules().to_vec());
//! engine.reason()?;
//!
//! let snapshot = recorder.snapshot_json();
//! assert!(snapshot.contains("\"fixpoint\":true"));
//! assert!(snapshot.contains("reach(?x, ?y)"));
//! # Ok::<(), nemo_physical::diagnostics::Diagnostic>(())
//! ```

mod event;
mod event_sink;
mod recorder;
mod report;
mod sink_slot;
mod stream;
mod timings;

#[cfg(feature = "subscriber")]
pub use self::stream::SinkLayer;
pub use self::{
    event::EngineEvent,
    event_sink::EventSink,
    recorder::{Recorder, RuleSummary},
    report::{
        ErrorCount, NamedCount, PhaseTable, PredicateSize, ProgramText, ReportDocument, RunFacts,
    },
    stream::{TARGET, TracingSink},
    timings::EngineTimings,
};
pub use nemo_physical::stopwatch::Stopwatch;

pub(crate) use self::sink_slot::SinkSlot;
