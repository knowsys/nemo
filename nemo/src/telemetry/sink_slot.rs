//! The engine-side holder of an installed event sink.

use std::sync::Arc;

use crate::telemetry::{EngineEvent, EventSink};

/// The engine's installed event sink, if any.
///
/// Without one, events are dropped.
#[derive(Default, Clone)]
pub(crate) struct SinkSlot(Option<Arc<dyn EventSink<EngineEvent>>>);

impl SinkSlot {
    /// Installs a sink, which the events emitted from here on reach.
    pub(crate) fn install(&mut self, sink: Arc<dyn EventSink<EngineEvent>>) {
        self.0 = Some(sink);
    }

    /// Returns whether emitted events reach a sink.
    ///
    /// Events that are expensive to build are built only if this holds.
    pub(crate) fn enabled(&self) -> bool {
        self.0.as_ref().is_some_and(|sink| sink.enabled())
    }

    /// Hands one event to the installed sink, if any.
    pub(crate) fn record(&self, event: &EngineEvent) {
        if let Some(sink) = &self.0 {
            sink.record(event);
        }
    }
}

impl std::fmt::Debug for SinkSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            Some(_) => "SinkSlot(installed)",
            None => "SinkSlot(none)",
        })
    }
}
