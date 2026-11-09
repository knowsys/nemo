//! The engine's holder of its installed event sink.

use std::sync::Arc;

use crate::telemetry::{EngineEvent, EventSink};

/// The engine's installed event sink, if any.
///
/// Without one, events are dropped.
#[derive(Default, Clone)]
pub(crate) struct SinkSlot(Option<Arc<dyn EventSink<EngineEvent>>>);

impl SinkSlot {
    /// Installs a sink, which receives the events emitted from then on.
    pub(crate) fn install(&mut self, sink: Arc<dyn EventSink<EngineEvent>>) {
        self.0 = Some(sink);
    }

    /// Returns whether a sink is installed and [enabled](EventSink::enabled).
    ///
    /// Events that are expensive to build are built only if this is true.
    pub(crate) fn enabled(&self) -> bool {
        self.0.as_ref().is_some_and(|sink| sink.enabled())
    }

    /// Passes one event to the installed sink, if any.
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
