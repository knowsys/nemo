//! The aggregating consumer of the engine's event stream.

mod curves;
mod orders;
mod rule_summary;
mod state;

pub use self::rule_summary::RuleSummary;

use std::sync::{
    Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU64, Ordering},
};

use nemo_physical::stopwatch::Stopwatch;
use serde::{Serialize, Serializer};

use self::state::RecorderState;
use crate::telemetry::{EngineEvent, EventSink};

/// A sink that aggregates the engine's events into a run summary:
/// per-rule statistics, derivation and memory curves, and phase totals.
///
/// It timestamps events relative to its own creation,
/// and can be snapshotted as JSON at any moment, also during a run.
#[derive(Debug)]
pub struct Recorder {
    /// The measurement origin every stamp is relative to.
    started: Stopwatch,
    /// The aggregated state.
    state: Mutex<RecorderState>,
    /// Bumped per change, so pollers can skip unchanged snapshots.
    version: AtomicU64,
}

impl Recorder {
    /// Creates a recorder, from which stamps count.
    pub fn new() -> Self {
        Self {
            started: Stopwatch::start(),
            state: Mutex::new(RecorderState::default()),
            version: AtomicU64::new(0),
        }
    }

    /// Sets the rules' rendered texts, by which the snapshot names the rules.
    ///
    /// These are typically the engine's [ran_rules](crate::engine::Engine::ran_rules),
    /// handed over once the program has loaded.
    pub fn set_rules(&self, texts: Vec<String>) {
        self.lock().texts = texts;
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Returns a counter bumped on every change,
    /// which pollers compare to skip snapshots nothing has happened between.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Returns the current state as one JSON object:
    /// the run's progress and totals, the imports, the rules,
    /// the timeline curves, and the phase and work tables.
    ///
    /// Times are nanoseconds and sizes bytes.
    pub fn snapshot_json(&self) -> String {
        serde_json::to_string(self).expect("a snapshot serializes")
    }

    /// Returns every rule's aggregate so far, in rule order.
    pub fn rules(&self) -> Vec<RuleSummary> {
        let state = self.lock();
        state
            .rules
            .iter()
            .enumerate()
            .map(|(index, stats)| RuleSummary {
                text: state.texts.get(index).cloned().unwrap_or_default(),
                applications: stats.applications,
                duration: stats.duration,
                derived: stats.derived,
                dropped: stats.dropped,
            })
            .collect()
    }

    /// Returns the state under its lock.
    ///
    /// A poisoning panic elsewhere does not take the telemetry down with it.
    fn lock(&self) -> MutexGuard<'_, RecorderState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSink<EngineEvent> for Recorder {
    fn record(&self, event: &EngineEvent) {
        let at = self.started.nanos();
        self.lock().absorb(at, event);
        self.version.fetch_add(1, Ordering::Release);
    }
}

/// A recorder serializes as its current state, which [snapshot_json](Recorder::snapshot_json)
/// writes.
impl Serialize for Recorder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.lock().serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use nemo_physical::plan::{PhaseTimings, WorkCounts};

    use super::{state::TIMELINE_POINTS, *};

    /// Returns a rule application event with the given number of derived rows.
    fn applied(rule: usize, derived: u64) -> EngineEvent {
        EngineEvent::RuleApplied {
            rule,
            step: 1,
            duration: 10,
            derived,
            dropped: 1,
            execution: PhaseTimings::default(),
            counts: WorkCounts::default(),
        }
    }

    #[test]
    fn events_aggregate_per_rule_and_in_total() {
        let recorder = Recorder::new();
        recorder.set_rules(vec!["a :- b .".to_string(), "c :- d .".to_string()]);
        recorder.record(&applied(0, 5));
        recorder.record(&applied(1, 2));
        recorder.record(&applied(0, 3));
        recorder.record(&EngineEvent::IterationFinished {
            stratum: 0,
            derived: 10,
            total: 12,
            memory: 640,
        });
        recorder.record(&EngineEvent::Finished {
            fixpoint: true,
            derived: 10,
            memory: 640,
        });

        let snapshot = recorder.snapshot_json();
        assert!(snapshot.contains("\"finished\":true"), "{snapshot}");
        assert!(snapshot.contains("\"fixpoint\":true"), "{snapshot}");
        assert!(snapshot.contains("\"derived\":10"), "{snapshot}");
        assert!(
            snapshot.contains(
                "{\"text\":\"a :- b .\",\"applications\":2,\"duration\":20,\"derived\":8"
            ),
            "{snapshot}"
        );
        assert!(recorder.version() >= 6);
    }

    #[test]
    fn timelines_downsample_but_keep_their_sums() {
        let recorder = Recorder::new();
        for _ in 0..(TIMELINE_POINTS * 3) {
            recorder.record(&applied(0, 1));
        }
        let state = recorder.lock();
        let timeline = state.timeline.entries();
        assert!(timeline.len() < TIMELINE_POINTS);
        let summed: u64 = timeline.iter().map(|point| point.derived).sum();
        assert_eq!(summed, (TIMELINE_POINTS * 3) as u64);
        assert!(timeline.is_sorted_by_key(|point| point.at));
    }

    #[test]
    fn downsampling_keeps_the_whole_run_in_view() {
        // Coarsening keeps every part of a long run represented:
        // each tenth of the run keeps at least one point.
        let mut state = RecorderState::default();
        let total: u64 = 200_000;
        for index in 0..total {
            // One application per synthetic microsecond.
            state.absorb(index * 1_000, &applied(0, 1));
        }

        let timeline = state.timeline.entries();
        let summed: u64 = timeline.iter().map(|point| point.derived).sum();
        assert_eq!(summed, total, "coarsening loses no derivations");
        assert!(timeline.len() < TIMELINE_POINTS);
        assert!(timeline.is_sorted_by_key(|point| point.at));

        let last = timeline.last().expect("points survive").at;
        for decile in 0..10 {
            let from = last / 10 * decile;
            let upto = last / 10 * (decile + 1);
            assert!(
                timeline
                    .iter()
                    .any(|point| point.at >= from && point.at <= upto),
                "decile {decile} of the run holds no point"
            );
        }
    }

    #[test]
    fn an_unknown_rule_index_still_counts_towards_the_run() {
        let recorder = Recorder::new();
        recorder.record(&applied(usize::MAX, 5));
        recorder.record(&EngineEvent::VariantObserved {
            rule: usize::MAX,
            variant: 0,
            delta_rows: 1,
            leading_rows: 2,
            delta_leads: false,
        });
        let state = recorder.lock();
        assert!(
            state.rules.is_empty(),
            "no rule entry is made for an unknown index"
        );
        assert_eq!(state.derived, 5, "the run's totals still count it");
        assert_eq!(state.orders.variants, 1);
        drop(state);

        // Once the rules are known, an index beyond them is unknown too.
        recorder.set_rules(vec!["a :- b .".to_string()]);
        recorder.record(&applied(1, 1));
        recorder.record(&applied(0, 1));
        let rules = recorder.rules();
        assert_eq!(rules.len(), 1, "{rules:?}");
        assert_eq!(rules[0].applications, 1);
    }

    #[test]
    fn fetches_add_up_under_their_endpoint() {
        let recorder = Recorder::new();
        recorder.record(&EngineEvent::Imported {
            resource: "https://x/sparql".to_string(),
            rows: 3,
        });
        recorder.record(&EngineEvent::Fetched {
            rule: 0,
            endpoint: "https://x/sparql".to_string(),
            requests: 2,
            rows: 4,
        });
        recorder.record(&EngineEvent::Fetched {
            rule: 1,
            endpoint: "https://y/sparql".to_string(),
            requests: 1,
            rows: 1,
        });
        let snapshot = recorder.snapshot_json();
        assert!(
            snapshot.contains("{\"resource\":\"https://x/sparql\",\"rows\":7}"),
            "{snapshot}"
        );
        assert!(
            snapshot.contains("{\"resource\":\"https://y/sparql\",\"rows\":1}"),
            "{snapshot}"
        );
    }

    /// The snapshot's shape is what the report page and the command line's JSON readers rely
    /// on: every key, in order, byte for byte, over one event of every kind,
    /// at synthetic stamps.
    #[test]
    fn snapshots_keep_their_shape() {
        let mut state = RecorderState::default();
        state.texts = vec!["a :- b .".to_string(), "c :- d .".to_string()];
        let execution = PhaseTimings {
            order: 1,
            join: 2,
            setup: 3,
            merge: 4,
            build: 5,
            encode: 6,
            total: 21,
        };
        let counts = WorkCounts {
            joins: 1,
            combinations: 2,
            rebound: 3,
            blocks: 4,
            rows: 5,
            arrangements: 7,
            arranged_rows: 8,
            buffer_bytes: 6,
        };
        let events = [
            EngineEvent::Imported {
                resource: "data.csv".to_string(),
                rows: 3,
            },
            EngineEvent::Fetched {
                rule: 0,
                endpoint: "https://x/sparql".to_string(),
                requests: 2,
                rows: 4,
            },
            EngineEvent::StratumStarted {
                stratum: 0,
                rules: 2,
            },
            EngineEvent::VariantObserved {
                rule: 0,
                variant: 0,
                delta_rows: 1,
                leading_rows: 8,
                delta_leads: false,
            },
            EngineEvent::VariantObserved {
                rule: 1,
                variant: 1,
                delta_rows: 0,
                leading_rows: 8,
                delta_leads: false,
            },
            EngineEvent::RuleApplied {
                rule: 0,
                step: 1,
                duration: 10,
                derived: 5,
                dropped: 1,
                execution,
                counts,
            },
            EngineEvent::RuleApplied {
                rule: 1,
                step: 2,
                duration: 20,
                derived: 2,
                dropped: 0,
                execution,
                counts,
            },
            EngineEvent::IterationFinished {
                stratum: 0,
                derived: 7,
                total: 12,
                memory: 640,
            },
            EngineEvent::Invalidated { tables: 1, rows: 2 },
            EngineEvent::Finished {
                fixpoint: true,
                derived: 7,
                memory: 640,
            },
        ];
        for (index, event) in events.iter().enumerate() {
            state.absorb(index as u64 + 1, event);
        }
        assert_eq!(
            serde_json::to_string(&state).expect("a snapshot serializes"),
            "{\"duration\":10,\"finished\":true,\"fixpoint\":true,\"derived\":7,\"total\":12,\
             \"memory\":640,\"invalidated\":2,\
             \"imports\":[{\"resource\":\"data.csv\",\"rows\":3},\
             {\"resource\":\"https://x/sparql\",\"rows\":4}],\
             \"strata\":[2],\
             \"rules\":[{\"text\":\"a :- b .\",\"applications\":1,\"duration\":10,\"derived\":5,\
             \"dropped\":1,\"variants\":1,\"deltaLed\":0,\"deltaEmpty\":0,\"deltaSmaller\":1,\
             \"deltaMuchSmaller\":1,\"ledRows\":8,\"deltaRows\":1},\
             {\"text\":\"c :- d .\",\"applications\":1,\"duration\":20,\"derived\":2,\
             \"dropped\":0,\"variants\":1,\"deltaLed\":0,\"deltaEmpty\":1,\"deltaSmaller\":0,\
             \"deltaMuchSmaller\":0,\"ledRows\":0,\"deltaRows\":0}],\
             \"orders\":{\"variants\":2,\"deltaLed\":0,\"deltaEmpty\":1,\"deltaSmaller\":1,\
             \"deltaMuchSmaller\":1,\"ledRows\":8,\"deltaRows\":1},\
             \"timeline\":[[6,5,10],[7,2,20]],\
             \"iterations\":[[8,0,7,12,640]],\
             \"phases\":{\"order\":2,\"join\":4,\"setup\":6,\"merge\":8,\"build\":10,\
             \"encode\":12,\"total\":42},\
             \"counts\":{\"joins\":2,\"combinations\":4,\"rebound\":6,\"blocks\":8,\"rows\":10,\
             \"arrangements\":14,\"arrangedRows\":16,\"buffer\":6}}"
        );
    }
}
