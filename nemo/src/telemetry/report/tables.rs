//! The tables of the engine's accounting: the time that plan execution spent,
//! and the work that it counted.
//!
//! The tables list the rows in the order and under the names that all reports use.

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

/// The phase table, which lists the time that plan execution spent, phase by phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseTable(Vec<NamedCount>);

impl PhaseTable {
    /// Returns the table of the phases of plan execution, followed by its total.
    pub(crate) fn of_execution(timings: &PhaseTimings) -> Self {
        let mut rows = Self::named(timings);
        rows.push(("total", timings.total));
        Self(rows)
    }

    /// Returns the table of the phases of a whole run.
    ///
    /// It lists the phases of plan execution, the time spent outside them (`glue`),
    /// the rest of the rule applications (`plan`), and the storing and consolidating around
    /// them.
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

    /// Returns the phases that the executor names, in order.
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

    /// Returns the time that plan execution spent outside the named phases,
    /// such as in block lookup, projections and result assembly.
    ///
    /// `setup` is left out of the sum, since it is measured within `join`.
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

/// The work table, which lists what plan execution counted, with the peak buffer size last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkTable(Vec<NamedCount>);

impl WorkTable {
    /// Returns the table of the counters of plan execution.
    pub(crate) fn of_execution(counts: &WorkCounts) -> Self {
        let mut rows = Self::counted(counts);
        rows.push(("buffer", counts.buffer_bytes));
        Self(rows)
    }

    /// Returns the table of the counters of a whole run:
    /// those of plan execution, then the engine's own.
    pub(crate) fn of_run(timings: &EngineTimings) -> Self {
        let mut rows = Self::counted(&timings.counts);
        rows.extend([
            ("consolidated", timings.consolidated_rows),
            ("storedOrders", timings.stored_orders),
            ("buffer", timings.counts.buffer_bytes),
        ]);
        Self(rows)
    }

    /// Returns the counters that the executor keeps, in order.
    fn counted(counts: &WorkCounts) -> Vec<NamedCount> {
        vec![
            ("joins", counts.joins),
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

    /// Returns an accounting with every number distinct, so that a row swapped with another
    /// shows.
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
                blocks: 34,
                rows: 35,
                arrangements: 20,
                arranged_rows: 21,
                buffer_bytes: 36,
            },
        }
    }

    /// A run's phase table adds the glue, the plan, and the storing and consolidating
    /// to the phases of plan execution.
    #[test]
    fn the_run_phases_add_the_glue_and_the_engine_stages() {
        let table = PhaseTable::of_run(&timings());
        // The glue leaves `setup` out of the sum, since it is measured within `join`.
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

    /// The phase table of plan execution ends with its total.
    #[test]
    fn the_execution_phases_end_with_their_total() {
        let table = PhaseTable::of_execution(&timings().execution);
        assert_eq!(table.rows().len(), 7);
        assert_eq!(table.rows()[6], ("total", 100));
    }

    /// Both work tables end with the peak buffer size,
    /// and the run's table adds the engine's counters before it.
    #[test]
    fn the_work_tables_keep_the_buffer_last() {
        let execution = WorkTable::of_execution(&timings().counts);
        assert_eq!(
            execution.rows(),
            [
                ("joins", 31),
                ("blocks", 34),
                ("rows", 35),
                ("arrangements", 20),
                ("arrangedRows", 21),
                ("buffer", 36),
            ]
        );
        let run = WorkTable::of_run(&timings());
        assert_eq!(run.rows()[..5], execution.rows()[..5]);
        assert_eq!(
            run.rows()[5..],
            [("consolidated", 19), ("storedOrders", 22), ("buffer", 36),]
        );
    }
}
