//! The wire form of events on the `tracing` stream, and its decoding.
//!
//! Each engine event is one `tracing` event, its data as flat named fields under the [TARGET]
//! target.
//! The field names are defined here and nowhere else.

use tracing::Level;

use crate::telemetry::EngineEvent;

/// The `tracing` target every telemetry event carries, by which subscribers select the stream.
pub const TARGET: &str = "nemo::telemetry";

/// The most numeric fields one event carries (a rule application's).
#[cfg(feature = "subscriber")]
const SLOTS: usize = 20;

/// Emits an event on the `tracing` stream at trace level.
///
/// The `kind` field names the variant, and the other fields carry its data by name.
/// Nothing is built if no subscriber listens to the target.
pub(crate) fn emit(event: &EngineEvent) {
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
            combinations = counts.combinations,
            rebound = counts.rebound,
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

/// The visitor collecting a `tracing` event's fields for decoding.
///
/// Fields arrive in the order [emit] declares them,
/// so the numeric ones land in slots by position.
/// No names are matched.
/// The `kind` field names the variant,
/// and the one text field (a resource or an endpoint) is copied,
/// the only allocation per event.
#[cfg(feature = "subscriber")]
#[derive(Debug, Default)]
pub(crate) struct FieldCollector {
    /// The variant, once the `kind` field arrived.
    kind: Option<EventKind>,
    /// The text field, if the variant has one.
    text: Option<String>,
    /// The numeric fields, flags as `0`/`1`, in arrival order.
    numbers: [u64; SLOTS],
    /// How many numeric fields arrived.
    count: usize,
}

/// The variants, as the `kind` field names them.
#[cfg(feature = "subscriber")]
#[derive(Debug, Clone, Copy)]
enum EventKind {
    /// [EngineEvent::Imported].
    Imported,
    /// [EngineEvent::Fetched].
    Fetched,
    /// [EngineEvent::StratumStarted].
    StratumStarted,
    /// [EngineEvent::RuleApplied].
    RuleApplied,
    /// [EngineEvent::IterationFinished].
    IterationFinished,
    /// [EngineEvent::VariantObserved].
    VariantObserved,
    /// [EngineEvent::Invalidated].
    Invalidated,
    /// [EngineEvent::Finished].
    Finished,
}

#[cfg(feature = "subscriber")]
impl EventKind {
    /// Returns the variant a `kind` field names.
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "imported" => EventKind::Imported,
            "fetched" => EventKind::Fetched,
            "stratum_started" => EventKind::StratumStarted,
            "rule_applied" => EventKind::RuleApplied,
            "iteration_finished" => EventKind::IterationFinished,
            "variant_observed" => EventKind::VariantObserved,
            "invalidated" => EventKind::Invalidated,
            "finished" => EventKind::Finished,
            _ => return None,
        })
    }
}

#[cfg(feature = "subscriber")]
impl FieldCollector {
    /// Takes one numeric field.
    fn number(&mut self, value: u64) {
        if self.count < SLOTS {
            self.numbers[self.count] = value;
            self.count += 1;
        }
    }

    /// Rebuilds the event from the collected fields.
    ///
    /// Returns `None` if the fields do not form an event:
    /// if the event comes from another producer on the `tracing` target,
    /// or if its fields are laid out differently than this version of nemo emits them.
    pub(crate) fn decode(self) -> Option<EngineEvent> {
        let n = &self.numbers;
        let count = |value: u64| usize::try_from(value).ok();
        let expects = |fields: usize| (self.count == fields).then_some(());
        Some(match self.kind? {
            EventKind::Imported => {
                expects(1)?;
                EngineEvent::Imported {
                    resource: self.text?,
                    rows: n[0],
                }
            }
            EventKind::Fetched => {
                expects(3)?;
                EngineEvent::Fetched {
                    rule: count(n[0])?,
                    endpoint: self.text?,
                    requests: n[1],
                    rows: n[2],
                }
            }
            EventKind::StratumStarted => {
                expects(2)?;
                EngineEvent::StratumStarted {
                    stratum: count(n[0])?,
                    rules: count(n[1])?,
                }
            }
            EventKind::RuleApplied => {
                expects(20)?;
                EngineEvent::RuleApplied {
                    rule: count(n[0])?,
                    step: n[1],
                    duration: n[2],
                    derived: n[3],
                    dropped: n[4],
                    execution: nemo_physical::plan::PhaseTimings {
                        order: n[5],
                        join: n[6],
                        setup: n[7],
                        merge: n[8],
                        build: n[9],
                        encode: n[10],
                        total: n[11],
                    },
                    counts: nemo_physical::plan::WorkCounts {
                        joins: n[12],
                        combinations: n[13],
                        rebound: n[14],
                        blocks: n[15],
                        rows: n[16],
                        arrangements: n[17],
                        arranged_rows: n[18],
                        buffer_bytes: n[19],
                    },
                }
            }
            EventKind::IterationFinished => {
                expects(4)?;
                EngineEvent::IterationFinished {
                    stratum: count(n[0])?,
                    derived: n[1],
                    total: n[2],
                    memory: n[3],
                }
            }
            EventKind::VariantObserved => {
                expects(5)?;
                EngineEvent::VariantObserved {
                    rule: count(n[0])?,
                    variant: count(n[1])?,
                    delta_rows: n[2],
                    leading_rows: n[3],
                    delta_leads: n[4] != 0,
                }
            }
            EventKind::Invalidated => {
                expects(2)?;
                EngineEvent::Invalidated {
                    tables: n[0],
                    rows: n[1],
                }
            }
            EventKind::Finished => {
                expects(3)?;
                EngineEvent::Finished {
                    fixpoint: n[0] != 0,
                    derived: n[1],
                    memory: n[2],
                }
            }
        })
    }
}

#[cfg(feature = "subscriber")]
impl tracing::field::Visit for FieldCollector {
    fn record_u64(&mut self, _field: &tracing::field::Field, value: u64) {
        self.number(value);
    }

    fn record_i64(&mut self, _field: &tracing::field::Field, value: i64) {
        self.number(u64::try_from(value).unwrap_or(0));
    }

    fn record_bool(&mut self, _field: &tracing::field::Field, value: bool) {
        self.number(u64::from(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "kind" {
            self.kind = EventKind::parse(value);
        } else {
            self.text = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}
