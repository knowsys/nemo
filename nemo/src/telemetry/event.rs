//! The events that the engine emits.

use nemo_physical::plan::{PhaseTimings, WorkCounts};

/// One observation of the engine, emitted into the installed event sink.
///
/// Times are in nanoseconds and sizes in bytes.
/// The events have no timestamps, so a sink that needs them stamps the events as they arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    /// An import finished loading.
    Imported {
        /// The resource that was read.
        resource: String,
        /// The number of rows it contributed, before deduplication.
        rows: u64,
    },
    /// One application of a bound SPARQL import, which is an import with a `where` clause,
    /// fetched its solutions.
    Fetched {
        /// The import's [position](crate::engine::Engine::compiled_rules) among the rules.
        rule: usize,
        /// The endpoint that was queried.
        endpoint: String,
        /// The number of requests sent.
        requests: u64,
        /// The number of solution rows received, before deduplication.
        rows: u64,
    },
    /// The evaluation of a stratum started.
    StratumStarted {
        /// The stratum's index, in evaluation order.
        stratum: usize,
        /// The number of rules active in it.
        rules: usize,
    },
    /// One rule application finished, and its derived rows were stored.
    RuleApplied {
        /// The rule's [position](crate::engine::Engine::compiled_rules).
        rule: usize,
        /// The engine step at which the application ran.
        ///
        /// The engine counts steps to order its writes to the stored tables.
        /// Step 0 is the loaded facts,
        /// and every rule application that runs gets a step of its own.
        step: u64,
        /// The wall time of the whole application, including storing.
        duration: u64,
        /// The number of new rows stored.
        derived: u64,
        /// The number of rows dropped because of evaluation errors.
        dropped: u64,
        /// Time per phase of the application's plan execution.
        execution: PhaseTimings,
        /// Work counters of the application's plan execution.
        counts: WorkCounts,
    },
    /// An iteration over a stratum finished.
    ///
    /// An iteration is one pass over the stratum's rules, each applied in turn.
    IterationFinished {
        /// The stratum's index, in evaluation order.
        stratum: usize,
        /// The number of new rows that the iteration's applications derived.
        derived: u64,
        /// The number of stored rows in total, including the loaded facts.
        total: u64,
        /// The bytes used by the stored tables and the dictionary.
        memory: u64,
    },
    /// One semi-naive variant of a rule application, compared with the rule's variable order
    /// before it ran.
    ///
    /// The leading atoms are the body atoms that bind the order's first variable.
    /// The event says whether the delta atom is a leading atom,
    /// and compares the rows it reads in this variant with those of the smallest leading atom.
    /// It is emitted only if the sink is [enabled](crate::telemetry::EventSink::enabled).
    VariantObserved {
        /// The rule's [position](crate::engine::Engine::compiled_rules).
        rule: usize,
        /// The variant, given as the body position of its delta atom.
        variant: usize,
        /// The number of rows that the delta atom reads in this variant.
        delta_rows: u64,
        /// The number of rows that the smallest leading atom reads in this variant.
        leading_rows: u64,
        /// Whether the delta atom is a leading atom.
        delta_leads: bool,
    },
    /// Facts added after an earlier fixpoint invalidated derived facts.
    ///
    /// Tables that depend on the added predicates through negation or aggregation
    /// dropped their derived rows before reasoning continued.
    Invalidated {
        /// The number of tables that dropped their derived rows.
        tables: u64,
        /// The number of stored rows removed.
        rows: u64,
    },
    /// A run ended at the fixpoint, or was cancelled.
    Finished {
        /// Whether the fixpoint was reached.
        ///
        /// `false` means the run was cancelled and can continue.
        fixpoint: bool,
        /// The number of facts derived in total, beyond the loaded ones.
        derived: u64,
        /// The bytes used by the stored tables and the dictionary.
        memory: u64,
    },
}
