//! Tracing every stored row of some predicates in batches,
//! which add up to one response, handed out as one part per batch.

use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::DataValue};

use crate::{
    engine::{
        Engine,
        trace::proof::{Batch, BatchedTrace},
    },
    model::Fact,
    trace::{TracePart, TracedFact},
};

/// The number of rows traced per batch.
///
/// The search writes to the store, such as arrangements and computed values,
/// while the lifting reads it, so each batch is searched first and lifted afterwards.
/// A batch's nodes are converted to the response types together,
/// so a batch also bounds the memory that the response uses,
/// apart from the proofs, which are kept until the trace ends.
const BATCH: usize = 1 << 10;

impl Engine {
    /// Traces every stored row of `predicates`, then `facts`,
    /// passes one part of the response to `sink` per batch,
    /// and returns `facts` with their nodes.
    pub(in crate::engine) fn trace_in_parts(
        &mut self,
        predicates: &[String],
        facts: &[Fact],
        mut sink: impl FnMut(TracePart) -> Result<(), Diagnostic>,
    ) -> Result<Vec<TracedFact>, Diagnostic> {
        self.traceable()?;
        let named = self.ground_facts(facts)?;
        let mut trace = BatchedTrace::default();
        for predicate in predicates {
            let (table, _) = self.whole_predicate(predicate)?;
            let mut cursor = self.cursor(predicate)?;
            loop {
                let chunk = self.read_chunk(&mut cursor, BATCH)?;
                let rows: Vec<(TableId, Vec<DataValue>)> =
                    chunk.rows().map(|row| (table, row)).collect();
                if rows.is_empty() {
                    break;
                }
                let part = trace.batch(self, &rows)?.part;
                sink(part)?;
            }
        }
        let Batch {
            part,
            facts: traced,
        } = trace.batch(self, &named)?;
        sink(part)?;
        Ok(traced)
    }
}
