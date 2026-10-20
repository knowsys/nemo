//! Results: what an execution delivers, and the accounting it keeps along the way.

use crate::{trie::Trie, value::ErrorCounts};

/// The results of one plan execution.
#[derive(Debug)]
pub struct PlanResults {
    /// Per declared output, its rows as a trie, with the columns in the output node's marker order.
    ///
    /// [add_trie](crate::management::TableManager::add_trie) stores such a trie as it is.
    pub outputs: Vec<Trie>,
    /// The evaluation errors of all operations,
    /// counted by kind.
    ///
    /// Every erroring row was dropped.
    /// The counts are per evaluation, not per row:
    /// a value evaluated again in another combination of blocks, or in another cell, counts again.
    /// So they report the kinds of errors and their magnitude, not exact row counts.
    pub errors: ErrorCounts,
    /// The wall-clock time of the execution, by phase.
    pub timings: PhaseTimings,
    /// The work counts of the execution.
    pub counts: WorkCounts,
}

/// The wall-clock nanoseconds of plan executions, by phase.
///
/// Callers add up the timings of many executions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PhaseTimings {
    /// Building the column orders of stored tables the loads read.
    pub order: u64,
    /// Running the joins, sequentially or in cells.
    pub join: u64,
    /// Building the joins: their cursors and hooks.
    ///
    /// It is part of `join`, and the rest of that phase is the descent.
    pub setup: u64,
    /// Sorting, deduplicating and concatenating result rows.
    pub merge: u64,
    /// Building tries from materialized rows.
    pub build: u64,
    /// Adding computed values to the dictionary.
    pub encode: u64,
    /// The whole executions.
    ///
    /// The time outside the phases is bookkeeping.
    pub total: u64,
}

impl PhaseTimings {
    /// Adds the timings of another execution to these.
    pub fn absorb(&mut self, other: &PhaseTimings) {
        self.order += other.order;
        self.join += other.join;
        self.setup += other.setup;
        self.merge += other.merge;
        self.build += other.build;
        self.encode += other.encode;
        self.total += other.total;
    }
}

/// The work counts of plan executions: the volumes behind the phase timings.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkCounts {
    /// The joins executed, one per member of a unit,
    /// including those an empty input ends before any combination runs.
    pub joins: u64,
    /// The combinations run across all joins,
    /// each a join over one block of each joined input.
    pub combinations: u64,
    /// The combinations that rebound the join of the previous combination of their member
    /// instead of building their own.
    pub rebound: u64,
    /// The blocks collected across all joins, joined and subtracted alike.
    ///
    /// Only the joined ones multiply into the combinations.
    pub blocks: u64,
    /// The rows collected for materializing nodes, before the builds drop the repeats.
    pub rows: u64,
    /// The arrangements of stored subtables built for the loads,
    /// one per subtable that lacked the order a load reads.
    pub arrangements: u64,
    /// The rows written to build those arrangements.
    pub arranged_rows: u64,
    /// The allocated bytes of the largest row buffer seen,
    /// whether it collected a node's rows or a cell's.
    ///
    /// The accounting of the stored tables cannot show this peak.
    /// Cells running at once can hold more than any single buffer.
    pub buffer_bytes: u64,
}

impl WorkCounts {
    /// Adds the counts of another execution to these.
    ///
    /// The largest buffer becomes the larger of the two.
    pub fn absorb(&mut self, other: &WorkCounts) {
        self.joins += other.joins;
        self.combinations += other.combinations;
        self.rebound += other.rebound;
        self.blocks += other.blocks;
        self.rows += other.rows;
        self.arrangements += other.arrangements;
        self.arranged_rows += other.arranged_rows;
        self.buffer_bytes = self.buffer_bytes.max(other.buffer_bytes);
    }
}

/// The accounting of one execution in progress.
#[derive(Debug, Default)]
pub(super) struct Accounting {
    /// The evaluation errors counted so far, by kind.
    pub(super) errors: ErrorCounts,
    /// The wall-clock time so far, by phase.
    pub(super) timings: PhaseTimings,
    /// The work counted so far.
    pub(super) counts: WorkCounts,
}

impl Accounting {
    /// Records the allocated bytes of a row buffer, keeping the largest.
    pub(super) fn see_buffer(&mut self, bytes: usize) {
        self.counts.buffer_bytes = self.counts.buffer_bytes.max(bytes as u64);
    }

    /// Returns the results of the execution, with the given outputs.
    pub(super) fn into_results(self, outputs: Vec<Trie>) -> PlanResults {
        PlanResults {
            outputs,
            errors: self.errors,
            timings: self.timings,
            counts: self.counts,
        }
    }
}
