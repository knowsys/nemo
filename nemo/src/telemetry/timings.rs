//! The engine's cumulative accounting of its reasoning.

use nemo_physical::plan::{PhaseTimings, WorkCounts};

/// The cumulative accounting of an engine's reasoning, times in nanoseconds:
/// the coarse split that tells where a run's time went, and the work behind it.
#[derive(Debug, Default, Clone, Copy)]
pub struct EngineTimings {
    /// Inside plan executions, by phase.
    pub execution: PhaseTimings,
    /// The time of whole rule applications.
    ///
    /// The difference from `execution.total` is plan construction
    /// and other per-application work outside plan execution.
    pub apply: u64,
    /// Storing derived rows into their tables.
    pub store: u64,
    /// Merging the parts of stored tables that separate steps wrote, between iterations.
    pub consolidate: u64,
    /// The rows those merges rewrote: the write amplification behind the `consolidate` time.
    pub consolidated_rows: u64,
    /// Distinct column orders stored, summed over the predicates.
    pub stored_orders: u64,
    /// Work counters across all plan executions.
    pub counts: WorkCounts,
}
