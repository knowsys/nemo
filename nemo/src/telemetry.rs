//! Execution telemetry: the events that the engine emits, and the sinks that consume them.
//!
//! As it loads and reasons, the engine emits [EngineEvent]s into the [EventSink]
//! installed with [Engine::set_telemetry](crate::engine::Engine::set_telemetry).
//! Nothing is emitted while no sink is installed.
//! Two sinks are ready-made:
//! - The [Recorder] aggregates the events into statistics and curves over the run,
//!   which it writes as JSON snapshots.
//! - The [LogSink] emits each event as a `tracing` event under the [TARGET] target,
//!   where a `tracing` subscriber receives it along with the engine's log lines.
//!
//! A [ReportDocument] combines a recorder with the [RunFacts] of a finished run into one JSON
//! document, which it can also render as an HTML page.
//! The [PhaseTable] lists the phase accounting under the names that the documents use.
//!
//! A sink can be installed on the engine directly, without a `tracing` subscriber:
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

pub use self::{
    event::EngineEvent,
    event_sink::EventSink,
    recorder::{Recorder, RuleSummary},
    report::{
        ErrorCount, NamedCount, PhaseTable, PredicateSize, ProgramText, ReportDocument, RunFacts,
    },
    stream::{LogSink, TARGET},
    timings::EngineTimings,
};
pub use nemo_physical::stopwatch::Stopwatch;

pub(crate) use self::sink_slot::SinkSlot;
