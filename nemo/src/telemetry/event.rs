//! The engine's event vocabulary.

use nemo_physical::plan::{PhaseTimings, WorkCounts};

/// One engine observation.
///
/// Times are nanoseconds and sizes bytes.
/// Sinks that want wall-clock positions stamp events as they arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    /// An import source finished loading.
    Imported {
        /// The resource that was read.
        resource: String,
        /// The rows it contributed, before deduplication.
        rows: u64,
    },
    /// One application of a bound SPARQL import (an import with a `where` clause)
    /// fetched its solutions.
    Fetched {
        /// The import's [position](crate::engine::Engine::ran_rules) among the rules.
        rule: usize,
        /// The endpoint asked.
        endpoint: String,
        /// Requests sent.
        requests: u64,
        /// Solution rows received, before deduplication.
        rows: u64,
    },
    /// A stratum began evaluating.
    StratumStarted {
        /// The stratum's index, in evaluation order.
        stratum: usize,
        /// The rules active in it.
        rules: usize,
    },
    /// One rule application finished, its derivations stored.
    RuleApplied {
        /// The rule's [position](crate::engine::Engine::ran_rules).
        rule: usize,
        /// The engine step the application ran at.
        ///
        /// The engine counts steps to order its writes to the store:
        /// step 0 holds the loaded facts,
        /// and every rule application that runs takes a step of its own.
        step: u64,
        /// Wall time of the whole application, storing included.
        duration: u64,
        /// New rows stored.
        derived: u64,
        /// Rows dropped because of evaluation errors.
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
        /// New rows across the iteration's applications.
        derived: u64,
        /// Stored rows in total, loaded facts included.
        total: u64,
        /// Bytes across stored tables and the dictionary.
        memory: u64,
    },
    /// One semi-naive variant of a rule application, compared with the rule's variable order
    /// before it ran.
    ///
    /// The leading atoms are the body atoms that bind the order's first variable.
    /// The event says whether the delta atom is a leading atom,
    /// and compares the rows it reads in this variant with those of the smallest leading atom.
    /// It is emitted only if a sink listens.
    VariantObserved {
        /// The rule's [position](crate::engine::Engine::ran_rules).
        rule: usize,
        /// The variant: the body position of its delta atom.
        variant: usize,
        /// The rows the delta atom reads in this variant.
        delta_rows: u64,
        /// The rows the smallest leading atom reads in this variant.
        leading_rows: u64,
        /// Whether the delta atom is a leading atom.
        delta_leads: bool,
    },
    /// Facts added after an earlier fixpoint invalidated derived facts.
    ///
    /// Tables that depend on the added predicates through negation or aggregation
    /// dropped their derived rows before reasoning continued.
    Invalidated {
        /// Tables that dropped their derived rows.
        tables: u64,
        /// Stored rows removed.
        rows: u64,
    },
    /// Reasoning ended.
    Finished {
        /// Whether the fixpoint was reached.
        ///
        /// `false` means the run was cancelled and can continue.
        fixpoint: bool,
        /// New facts derived in total, beyond the loaded ones.
        derived: u64,
        /// Bytes across stored tables and the dictionary.
        memory: u64,
    },
}
