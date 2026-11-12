//! The run state of an engine: what its runs have done, and where the next one continues.

use std::collections::{HashMap, HashSet};

use nemo_physical::{function::EvaluationContext, management::TableId, value::ErrorCounts};

use crate::{
    engine::evaluate::{
        application::{LoggedApplication, PlanSkeletons},
        contexts::RunContexts,
        reason::FetchTally,
    },
    telemetry::EngineTimings,
};

/// The run state of an engine.
#[derive(Debug, Default)]
pub(in crate::engine) struct RunState {
    /// The step counter, which is the revision that the next stored rows go into.
    ///
    /// Revision 0 contains the input facts of the first load.
    pub(in crate::engine) step: u64,
    /// Per rule of the compiled program, by index, the step of the rule's last application,
    /// or 0 before the first.
    ///
    /// A rule reads the rows that its body's tables gained since then.
    pub(in crate::engine) last_applied: Vec<u64>,
    /// The tables that gained input facts since the last run,
    /// or may gain facts through new rules.
    ///
    /// The next run first drops the derived facts that depend on them through negation or
    /// aggregation, so that it derives them again (see `drop_invalidated`).
    pub(in crate::engine) dirty: HashSet<TableId>,
    /// Whether the last run reached the fixpoint and nothing was added since.
    ///
    /// A cancelled run leaves it false.
    pub(in crate::engine) fixpoint: bool,
    /// The evaluation context of the next run, if a loading method or
    /// [import_rows](crate::engine::Engine::import_rows) chose it since the last run began.
    ///
    /// The first such call chooses it at its own instant,
    /// and it evaluates the `NOW()` of facts and option values in it.
    /// Later calls, and the next run, evaluate their `NOW()` in the same context.
    /// A call that fails or is discarded undoes its choice.
    pub(in crate::engine) next_context: Option<EvaluationContext>,
    /// The application log, with one entry per rule application that stored rows,
    /// in ascending order of step.
    ///
    /// It records which rule wrote which revision, for tracing.
    /// [reload](crate::engine::Engine::reload) clears it, since a reload drops every derived
    /// row.
    pub(in crate::engine) applications: Vec<LoggedApplication>,
    /// The evaluation context of each run.
    ///
    /// Tracing executes a run's rule applications again in the run's context.
    /// It is cleared together with the application log.
    pub(in crate::engine) contexts: RunContexts,
    /// Per table, the wall time in nanoseconds of the rule applications that stored rows
    /// into it.
    ///
    /// It is cleared together with the application log.
    pub(in crate::engine) derivation_time: HashMap<TableId, u64>,
    /// The plan skeletons of the compiled program's rules.
    pub(in crate::engine) skeletons: PlanSkeletons,
    /// Per rule of a bound import, by rule index, the tally of its fetches in the current run.
    ///
    /// The tallies are reported as warnings when the run ends.
    pub(in crate::engine) fetch_tallies: HashMap<usize, FetchTally>,
    /// The number of rule applications so far, including the fetches of bound imports.
    pub(in crate::engine) applied: u64,
    /// The evaluation errors of all rule applications, counted by kind.
    pub(in crate::engine) errors: ErrorCounts,
    /// The wall-clock times of the reasoning so far.
    pub(in crate::engine) timings: EngineTimings,
}

impl RunState {
    /// Forgets what the runs derived, for a reload, which drops every derived row.
    ///
    /// No rule was applied, no table is marked as having gained input facts,
    /// the fixpoint is not reached,
    /// and the application log, the run contexts and the derivation times are empty.
    /// The step counter goes on, and so do the counts and the times of the reasoning so far.
    pub(in crate::engine) fn forget_derivations(&mut self) {
        self.last_applied.clear();
        self.dirty.clear();
        self.fixpoint = false;
        self.applications.clear();
        self.contexts.clear();
        self.derivation_time.clear();
    }
}
