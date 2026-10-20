//! The aggregated run state under the recorder's lock.

use nemo_physical::plan::{PhaseTimings, WorkCounts};
use serde::{Serialize, Serializer};

use crate::telemetry::{
    EngineEvent,
    recorder::{
        curves::{BoundedCurve, IterationPoint, TimelinePoint},
        orders::VariantOrderStats,
    },
    report::tables::{NamedCount, NamedCounts, PhaseTable, WorkTable},
};

/// Timeline entries kept before the curve coarsens into wider time
/// windows.
pub(super) const TIMELINE_POINTS: usize = 8192;

/// Iteration entries kept before the iteration curve coarsens.
const ITERATION_POINTS: usize = 4096;

/// The rule indices accepted if the rule texts are unknown.
///
/// A program has nowhere near this many rules,
/// so an event with a larger index comes from another producer on the `tracing` target.
const MAX_RULES: usize = 1 << 20;

/// The aggregated run state: per-rule statistics, the derivation and memory curves,
/// and phase totals.
///
/// It is folded from the events as they arrive, and snapshotted as JSON at any moment.
#[derive(Debug, Default)]
pub(super) struct RecorderState {
    /// The rules' rendered texts, if passed to [set_rules](super::Recorder::set_rules).
    pub(super) texts: Vec<String>,
    /// Per-rule aggregates, indexed like the texts.
    pub(super) rules: Vec<RuleStats>,
    /// Active rules per stratum, in evaluation order.
    strata: Vec<usize>,
    /// Imported sources: resource name and contributed rows.
    imports: Vec<(String, u64)>,
    /// The timeline of rule applications.
    pub(super) timeline: BoundedCurve<TimelinePoint, TIMELINE_POINTS>,
    /// The curve of finished iterations.
    iterations: BoundedCurve<IterationPoint, ITERATION_POINTS>,
    /// Phase accounting summed over all applications.
    execution: PhaseTimings,
    /// Work counters summed over all applications.
    counts: WorkCounts,
    /// The variant observations, summed over the rules.
    pub(super) orders: VariantOrderStats,
    /// New rows derived so far.
    pub(super) derived: u64,
    /// Stored rows after the last iteration, loaded facts included.
    total: u64,
    /// Bytes across tables and dictionary at the last measurement.
    memory: u64,
    /// Stamp of the latest event.
    duration: u64,
    /// Whether reasoning has ended.
    finished: bool,
    /// Whether it ended at the fixpoint rather than cancelled.
    fixpoint: bool,
    /// Derived rows dropped because facts added after an earlier fixpoint invalidated them.
    invalidated: u64,
}

/// One rule's aggregate over its applications.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RuleStats {
    /// Applications run.
    pub(super) applications: u64,
    /// Wall time across them.
    pub(super) duration: u64,
    /// New rows stored.
    pub(super) derived: u64,
    /// Rows dropped because of evaluation errors.
    pub(super) dropped: u64,
    /// The observations of the rule's variants.
    orders: VariantOrderStats,
}

impl RecorderState {
    /// Returns the aggregate of the rule at an index, growing the table to reach it.
    ///
    /// Returns `None` for an index no rule has:
    /// beyond the texts supplied, or beyond [MAX_RULES] if none were.
    /// Such an event comes from another producer on the `tracing` target.
    /// Its numbers still count towards the run, but are attributed to no rule.
    fn rule(&mut self, index: usize) -> Option<&mut RuleStats> {
        let known = if self.texts.is_empty() {
            MAX_RULES
        } else {
            self.texts.len()
        };
        if index >= known {
            return None;
        }
        if self.rules.len() <= index {
            self.rules.resize(index + 1, RuleStats::default());
        }
        Some(&mut self.rules[index])
    }

    /// Folds one stamped event into the aggregate.
    pub(super) fn absorb(&mut self, at: u64, event: &EngineEvent) {
        self.duration = self.duration.max(at);
        match event {
            EngineEvent::Imported { resource, rows } => {
                self.imports.push((resource.clone(), *rows))
            }
            EngineEvent::Fetched { endpoint, rows, .. } => self.fetched(endpoint, *rows),
            EngineEvent::StratumStarted { rules, .. } => self.strata.push(*rules),
            EngineEvent::VariantObserved {
                rule,
                delta_rows,
                leading_rows,
                delta_leads,
                ..
            } => self.variant_observed(*rule, *delta_rows, *leading_rows, *delta_leads),
            EngineEvent::RuleApplied {
                rule,
                duration,
                derived,
                dropped,
                execution,
                counts,
                ..
            } => {
                self.execution.absorb(execution);
                self.counts.absorb(counts);
                self.rule_applied(at, *rule, *duration, *derived, *dropped);
            }
            EngineEvent::IterationFinished {
                stratum,
                derived,
                total,
                memory,
            } => self.iteration_finished(at, *stratum, *derived, *total, *memory),
            EngineEvent::Invalidated { rows, .. } => self.invalidated += rows,
            EngineEvent::Finished {
                fixpoint,
                derived,
                memory,
            } => self.finished(*fixpoint, *derived, *memory),
        }
    }

    /// Records the rows one fetch of a bound import returned, under its endpoint.
    fn fetched(&mut self, endpoint: &str, rows: u64) {
        match self
            .imports
            .iter_mut()
            .find(|(resource, _)| resource == endpoint)
        {
            Some(entry) => entry.1 += rows,
            None => self.imports.push((endpoint.to_owned(), rows)),
        }
    }

    /// Records an observed variant, for its rule and for the run.
    fn variant_observed(&mut self, rule: usize, delta_rows: u64, leading_rows: u64, leads: bool) {
        if let Some(stats) = self.rule(rule) {
            stats.orders.observe(delta_rows, leading_rows, leads);
        }
        self.orders.observe(delta_rows, leading_rows, leads);
    }

    /// Records a rule application: credited to its rule, counted for the run,
    /// and a point on the timeline.
    fn rule_applied(&mut self, at: u64, rule: usize, duration: u64, derived: u64, dropped: u64) {
        if let Some(stats) = self.rule(rule) {
            stats.applications += 1;
            stats.duration += duration;
            stats.derived += derived;
            stats.dropped += dropped;
        }
        self.derived += derived;
        self.timeline.push(TimelinePoint {
            at,
            derived,
            duration,
        });
    }

    /// Records a finished iteration: the sizes it left, and a point on the iteration curve.
    fn iteration_finished(
        &mut self,
        at: u64,
        stratum: usize,
        derived: u64,
        total: u64,
        memory: u64,
    ) {
        self.total = total;
        self.memory = memory;
        self.iterations.push(IterationPoint {
            at,
            stratum: stratum as u64,
            derived,
            total,
            memory,
        });
    }

    /// Records the end of reasoning: how it ended, and the final sizes.
    fn finished(&mut self, fixpoint: bool, derived: u64, memory: u64) {
        self.finished = true;
        self.fixpoint = fixpoint;
        self.derived = derived;
        self.memory = memory;
    }
}

/// One import, as the snapshot lists it.
#[derive(Serialize)]
struct ImportView<'a> {
    /// The resource read.
    resource: &'a str,
    /// The rows it contributed.
    rows: u64,
}

/// One rule's aggregate, as the snapshot lists it.
#[derive(Serialize)]
struct RuleView<'a> {
    /// The rule's text.
    text: &'a str,
    /// Its applications.
    applications: u64,
    /// The wall time of its applications.
    duration: u64,
    /// The rows it derived.
    derived: u64,
    /// The rows its evaluation errors dropped.
    dropped: u64,
    /// The observations of its variants.
    #[serde(flatten)]
    orders: NamedCounts<'a>,
}

/// The state as the snapshot lays it out.
///
/// The curves are compact:
/// a timeline point is `[at, derived, duration]`,
/// an iteration `[at, stratum, derived, total, memory]`.
#[derive(Serialize)]
struct RecorderStateView<'a> {
    /// The stamp of the latest event.
    duration: u64,
    /// Whether reasoning has ended.
    finished: bool,
    /// Whether it ended at the fixpoint.
    fixpoint: bool,
    /// New rows derived so far.
    derived: u64,
    /// Stored rows after the last iteration.
    total: u64,
    /// Bytes at the last measurement.
    memory: u64,
    /// Derived rows dropped because added facts invalidated them.
    invalidated: u64,
    /// The imports.
    imports: Vec<ImportView<'a>>,
    /// Active rules per stratum.
    strata: &'a [usize],
    /// The rules' aggregates.
    rules: Vec<RuleView<'a>>,
    /// The variant observations, summed over the rules.
    orders: NamedCounts<'a>,
    /// The timeline of rule applications.
    timeline: Vec<(u64, u64, u64)>,
    /// The curve of finished iterations.
    iterations: Vec<(u64, u64, u64, u64, u64)>,
    /// The phase table.
    phases: NamedCounts<'a>,
    /// The work table.
    counts: NamedCounts<'a>,
}

impl Serialize for RecorderState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let rule_orders: Vec<[NamedCount; 7]> =
            self.rules.iter().map(|stats| stats.orders.rows()).collect();
        let orders = self.orders.rows();
        let phases = PhaseTable::of_execution(&self.execution);
        let counts = WorkTable::of_execution(&self.counts);
        RecorderStateView {
            duration: self.duration,
            finished: self.finished,
            fixpoint: self.fixpoint,
            derived: self.derived,
            total: self.total,
            memory: self.memory,
            invalidated: self.invalidated,
            imports: self
                .imports
                .iter()
                .map(|(resource, rows)| ImportView {
                    resource,
                    rows: *rows,
                })
                .collect(),
            strata: &self.strata,
            rules: self
                .rules
                .iter()
                .zip(&rule_orders)
                .enumerate()
                .map(|(index, (stats, orders))| RuleView {
                    text: self.texts.get(index).map_or("", String::as_str),
                    applications: stats.applications,
                    duration: stats.duration,
                    derived: stats.derived,
                    dropped: stats.dropped,
                    orders: NamedCounts(orders),
                })
                .collect(),
            orders: NamedCounts(&orders),
            timeline: self
                .timeline
                .entries()
                .iter()
                .map(|point| (point.at, point.derived, point.duration))
                .collect(),
            iterations: self
                .iterations
                .entries()
                .iter()
                .map(|iteration| {
                    (
                        iteration.at,
                        iteration.stratum,
                        iteration.derived,
                        iteration.total,
                        iteration.memory,
                    )
                })
                .collect(),
            phases: NamedCounts(phases.rows()),
            counts: NamedCounts(counts.rows()),
        }
        .serialize(serializer)
    }
}
