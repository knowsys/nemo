//! The subscriber side of the telemetry stream: a `tracing` layer
//! feeding an event sink.

use std::sync::Arc;

use tracing::Subscriber;
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    layer::{Context, Layer},
};

use crate::telemetry::{
    EngineEvent, EventSink,
    stream::codec::{self, FieldCollector},
};

/// A layer decoding the events a [TracingSink](crate::telemetry::TracingSink) put on the stream,
/// and handing them to a sink.
///
/// With a [Recorder](crate::telemetry::Recorder) as its sink,
/// a report is built from the same `tracing` stream that carries the log lines.
/// Compose it with [filter](Self::filter),
/// so that it sees the telemetry target regardless of the level the other layers show.
pub struct SinkLayer {
    /// Where decoded events go.
    sink: Arc<dyn EventSink<EngineEvent>>,
}

impl SinkLayer {
    /// Creates a layer feeding the sink.
    pub fn new(sink: Arc<dyn EventSink<EngineEvent>>) -> Self {
        Self { sink }
    }

    /// Returns the per-layer filter selecting the telemetry stream.
    pub fn filter() -> Targets {
        Targets::new().with_target(codec::TARGET, LevelFilter::TRACE)
    }
}

impl std::fmt::Debug for SinkLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SinkLayer")
    }
}

impl<S: Subscriber> Layer<S> for SinkLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != codec::TARGET {
            return;
        }
        let mut collector = FieldCollector::default();
        event.record(&mut collector);
        if let Some(event) = collector.decode() {
            self.sink.record(&event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nemo_physical::plan::{PhaseTimings, WorkCounts};
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::telemetry::TracingSink;

    /// A sink keeping every event it is given.
    #[derive(Default)]
    struct Collecting(Mutex<Vec<EngineEvent>>);

    impl EventSink<EngineEvent> for Collecting {
        fn record(&self, event: &EngineEvent) {
            self.0.lock().expect("no poisoning").push(event.clone());
        }
    }

    /// One test for both subscribers: `tracing` caches callsite interest per
    /// process across all live dispatchers, so the scenarios run in order,
    /// the one not listening to the telemetry target first.
    #[test]
    fn events_survive_the_round_trip_through_tracing() {
        // A layer filtered to another target does not listen to the
        // telemetry target, so the sink skips its work.
        let elsewhere = Targets::new().with_target("elsewhere", LevelFilter::TRACE);
        let not_listening = SinkLayer::new(Arc::new(Collecting::default())).with_filter(elsewhere);
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(not_listening),
            || {
                assert!(!TracingSink.enabled());
            },
        );

        let events = vec![
            EngineEvent::Imported {
                resource: String::from("data/p.csv"),
                rows: 3,
            },
            EngineEvent::Fetched {
                rule: 2,
                endpoint: String::from("https://x/sparql"),
                requests: 3,
                rows: 4,
            },
            EngineEvent::StratumStarted {
                stratum: 1,
                rules: 2,
            },
            EngineEvent::RuleApplied {
                rule: 4,
                step: 5,
                duration: 6,
                derived: 7,
                dropped: 8,
                execution: PhaseTimings {
                    order: 1,
                    join: 2,
                    setup: 3,
                    merge: 4,
                    build: 5,
                    encode: 6,
                    total: 21,
                },
                counts: WorkCounts {
                    joins: 1,
                    combinations: 2,
                    rebound: 3,
                    blocks: 4,
                    rows: 5,
                    arrangements: 7,
                    arranged_rows: 8,
                    buffer_bytes: 6,
                },
            },
            EngineEvent::IterationFinished {
                stratum: 1,
                derived: 9,
                total: 10,
                memory: 11,
            },
            EngineEvent::VariantObserved {
                rule: 4,
                variant: 0,
                delta_rows: 12,
                leading_rows: 13,
                delta_leads: false,
            },
            EngineEvent::Invalidated { tables: 1, rows: 2 },
            EngineEvent::Finished {
                fixpoint: true,
                derived: 9,
                memory: 11,
            },
        ];

        let collected = Arc::new(Collecting::default());
        let layer = SinkLayer::new(collected.clone()).with_filter(SinkLayer::filter());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            assert!(TracingSink.enabled(), "a listening layer enables the sink");
            for event in &events {
                TracingSink.record(event);
            }
            // An event of another producer on the target decodes to nothing.
            tracing::event!(target: codec::TARGET, tracing::Level::TRACE, kind = "other");
        });
        assert_eq!(*collected.0.lock().expect("no poisoning"), events);
    }
}
