//! The tables of the engine's accounting: the time plan execution spent, and the work it counted.
//!
//! They list the rows in the order and under the names all reports use:
//! the keys of the JSON documents and the labels of the text summary.

use nemo_physical::plan::{PhaseTimings, WorkCounts};
use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::telemetry::EngineTimings;

/// One named number of a table.
pub type NamedCount = (&'static str, u64);

/// Named numbers serialized as one JSON object, in their order.
pub(crate) struct NamedCounts<'a>(pub(crate) &'a [NamedCount]);

impl Serialize for NamedCounts<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

/// The phase table: the time plan execution spent, phase by phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseTable(Vec<NamedCount>);

impl PhaseTable {
    /// Returns the phases of plan execution, and its total.
    pub(crate) fn of_execution(timings: &PhaseTimings) -> Self {
        let mut rows = Self::named(timings);
        rows.push(("total", timings.total));
        Self(rows)
    }

    /// Returns the phases of a whole run: plan execution's, what it spent outside
    /// the named phases, and the engine's stages around it.
    pub fn of_run(timings: &EngineTimings) -> Self {
        let execution = &timings.execution;
        let mut rows = Self::named(execution);
        rows.extend([
            ("glue", Self::glue(execution)),
            ("plan", timings.apply.saturating_sub(execution.total)),
            ("store", timings.store),
            ("consolidate", timings.consolidate),
        ]);
        Self(rows)
    }

    /// Returns the phases the executor names, in order.
    ///
    /// `setup` is measured within `join`.
    fn named(timings: &PhaseTimings) -> Vec<NamedCount> {
        vec![
            ("order", timings.order),
            ("join", timings.join),
            ("setup", timings.setup),
            ("merge", timings.merge),
            ("build", timings.build),
            ("encode", timings.encode),
        ]
    }

    /// Returns what plan execution spent in no named phase: block lookup,
    /// combination enumeration, projections, result assembly.
    fn glue(timings: &PhaseTimings) -> u64 {
        timings.total.saturating_sub(
            timings.order + timings.join + timings.merge + timings.build + timings.encode,
        )
    }

    /// Returns the rows, in order.
    pub fn rows(&self) -> &[NamedCount] {
        &self.0
    }
}

/// The work table: what plan execution counted, the peak buffer last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkTable(Vec<NamedCount>);

impl WorkTable {
    /// Returns the counters of plan execution.
    pub(crate) fn of_execution(counts: &WorkCounts) -> Self {
        let mut rows = Self::counted(counts);
        rows.push(("buffer", counts.buffer_bytes));
        Self(rows)
    }

    /// Returns the counters of a whole run: plan execution's, then the engine's
    /// around it.
    pub(crate) fn of_run(timings: &EngineTimings) -> Self {
        let mut rows = Self::counted(&timings.counts);
        rows.extend([
            ("consolidated", timings.consolidated_rows),
            ("storedOrders", timings.stored_orders),
            ("buffer", timings.counts.buffer_bytes),
        ]);
        Self(rows)
    }

    /// Returns the counters the executor keeps, in order.
    fn counted(counts: &WorkCounts) -> Vec<NamedCount> {
        vec![
            ("joins", counts.joins),
            ("combinations", counts.combinations),
            ("rebound", counts.rebound),
            ("blocks", counts.blocks),
            ("rows", counts.rows),
            ("arrangements", counts.arrangements),
            ("arrangedRows", counts.arranged_rows),
        ]
    }

    /// Returns the rows, in order.
    pub(crate) fn rows(&self) -> &[NamedCount] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An accounting with every number distinct, so a row swapped with
    /// another shows.
    fn timings() -> EngineTimings {
        EngineTimings {
            execution: PhaseTimings {
                order: 11,
                join: 12,
                setup: 13,
                merge: 14,
                build: 15,
                encode: 16,
                total: 100,
            },
            apply: 150,
            store: 17,
            consolidate: 18,
            consolidated_rows: 19,
            stored_orders: 22,
            counts: WorkCounts {
                joins: 31,
                combinations: 32,
                rebound: 33,
                blocks: 34,
                rows: 35,
                arrangements: 20,
                arranged_rows: 21,
                buffer_bytes: 36,
            },
        }
    }

    #[test]
    fn the_run_phases_add_the_glue_and_the_engine_stages() {
        let table = PhaseTable::of_run(&timings());
        // The glue leaves `setup` out of the sum: it is measured within
        // `join`.
        assert_eq!(
            table.rows(),
            [
                ("order", 11),
                ("join", 12),
                ("setup", 13),
                ("merge", 14),
                ("build", 15),
                ("encode", 16),
                ("glue", 32),
                ("plan", 50),
                ("store", 17),
                ("consolidate", 18),
            ]
        );
    }

    #[test]
    fn the_execution_phases_end_with_their_total() {
        let table = PhaseTable::of_execution(&timings().execution);
        assert_eq!(table.rows().len(), 7);
        assert_eq!(table.rows()[6], ("total", 100));
    }

    #[test]
    fn the_work_tables_keep_the_buffer_last() {
        let execution = WorkTable::of_execution(&timings().counts);
        assert_eq!(
            execution.rows(),
            [
                ("joins", 31),
                ("combinations", 32),
                ("rebound", 33),
                ("blocks", 34),
                ("rows", 35),
                ("arrangements", 20),
                ("arrangedRows", 21),
                ("buffer", 36),
            ]
        );
        let run = WorkTable::of_run(&timings());
        assert_eq!(run.rows()[..7], execution.rows()[..7]);
        assert_eq!(
            run.rows()[7..],
            [("consolidated", 19), ("storedOrders", 22), ("buffer", 36),]
        );
    }
}
