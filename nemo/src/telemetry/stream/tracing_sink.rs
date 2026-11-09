//! The sink that puts the engine's events on the `tracing` stream.

use crate::telemetry::{EngineEvent, EventSink, stream::codec};

/// A sink that emits every event as a `tracing` event under the
/// [telemetry target](crate::telemetry::TARGET).
///
/// A `tracing` subscriber receives the events along with the engine's log lines.
/// The sink is [enabled](EventSink::enabled) exactly if some subscriber listens to the target
/// at trace level,
/// so an engine without a listener still skips the work of building its expensive events.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingSink;

impl EventSink<EngineEvent> for TracingSink {
    fn record(&self, event: &EngineEvent) {
        codec::emit(event);
    }

    fn enabled(&self) -> bool {
        tracing::enabled!(target: codec::TARGET, tracing::Level::TRACE)
    }
}
