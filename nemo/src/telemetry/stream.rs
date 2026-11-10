//! The engine's events on the `tracing` stream.
//!
//! Each engine event is one `tracing` event under the [TARGET] target,
//! with its data as flat named fields.
//! The field names are defined here and nowhere else.

use tracing::Level;

use crate::telemetry::{EngineEvent, EventSink};

/// The `tracing` target of every telemetry event, by which subscribers select the telemetry
/// stream.
pub const TARGET: &str = "nemo::telemetry";

/// A sink that emits every event as a `tracing` event under the [TARGET] target.
///
/// A `tracing` subscriber receives the events along with the engine's log lines.
/// The sink is [enabled](EventSink::enabled) exactly if some subscriber listens to the target
/// at trace level,
/// so an engine without a listener still skips the work of building its expensive events.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingSink;

impl EventSink<EngineEvent> for TracingSink {
    fn record(&self, event: &EngineEvent) {
        emit(event);
    }

    fn enabled(&self) -> bool {
        tracing::enabled!(target: TARGET, Level::TRACE)
    }
}

/// Emits an event on the `tracing` stream at trace level.
///
/// The field `kind` names the variant, and the other fields contain its data, by name.
/// Nothing is built if no subscriber listens to the target.
fn emit(event: &EngineEvent) {
    match event {
        EngineEvent::Imported { resource, rows } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "imported",
            resource = resource.as_str(),
            rows,
        ),
        EngineEvent::Fetched {
            rule,
            endpoint,
            requests,
            rows,
        } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "fetched",
            endpoint = endpoint.as_str(),
            rule = *rule as u64,
            requests,
            rows,
        ),
        EngineEvent::StratumStarted { stratum, rules } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "stratum_started",
            stratum = *stratum as u64,
            rules = *rules as u64,
        ),
        EngineEvent::RuleApplied {
            rule,
            step,
            duration,
            derived,
            dropped,
            execution,
            counts,
        } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "rule_applied",
            rule = *rule as u64,
            step,
            duration,
            derived,
            dropped,
            order = execution.order,
            join = execution.join,
            setup = execution.setup,
            merge = execution.merge,
            build = execution.build,
            encode = execution.encode,
            total = execution.total,
            joins = counts.joins,
            blocks = counts.blocks,
            rows = counts.rows,
            arrangements = counts.arrangements,
            arranged_rows = counts.arranged_rows,
            buffer = counts.buffer_bytes,
        ),
        EngineEvent::IterationFinished {
            stratum,
            derived,
            total,
            memory,
        } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "iteration_finished",
            stratum = *stratum as u64,
            derived,
            total,
            memory,
        ),
        EngineEvent::VariantObserved {
            rule,
            variant,
            delta_rows,
            leading_rows,
            delta_leads,
        } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "variant_observed",
            rule = *rule as u64,
            variant = *variant as u64,
            delta_rows,
            leading_rows,
            delta_leads,
        ),
        EngineEvent::Invalidated { tables, rows } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "invalidated",
            tables,
            rows,
        ),
        EngineEvent::Finished {
            fixpoint,
            derived,
            memory,
        } => tracing::event!(
            target: TARGET,
            Level::TRACE,
            kind = "finished",
            fixpoint,
            derived,
            memory,
        ),
    }
}
