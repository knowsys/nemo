//! The engine's cumulative accounting of its reasoning.

use nemo_physical::plan::{PhaseTimings, WorkCounts};

/// The cumulative accounting of an engine's reasoning: where the time went, and the work done.
///
/// Times are in nanoseconds.
#[derive(Debug, Default, Clone, Copy)]
pub struct EngineTimings {
    /// The time spent inside plan executions, by phase.
    pub execution: PhaseTimings,
    /// The time of whole rule applications.
    ///
    /// The difference from `execution.total` is plan construction
    /// and other per-application work outside plan execution.
    pub apply: u64,
    /// The time spent storing derived rows into their tables.
    pub store: u64,
    /// The time spent merging the parts of stored tables that separate steps wrote,
    /// between iterations.
    pub consolidate: u64,
    /// The number of rows that those merges rewrote.
    ///
    /// This is the write amplification behind the [consolidate](Self::consolidate) time.
    pub consolidated_rows: u64,
    /// The number of distinct column orders stored, summed over the predicates.
    pub stored_orders: u64,
    /// The work counters, summed over all plan executions.
    pub counts: WorkCounts,
}
