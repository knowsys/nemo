//! Tracing every stored row of predicates in batches,
//! which add up to one response handed out a part per batch.

use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::DataValue};

use crate::{
    engine::{Engine, trace::proof::Trace},
    model::Fact,
    trace::{TracePart, TracedFact},
};

/// The rows traced per batch.
///
/// A batch's nodes are converted to the protocol together, so a batch bounds what the response
/// holds besides the proofs; the proofs are kept until the trace ends.
const BATCH: usize = 1 << 10;

impl Engine {
    /// Traces every stored row of the predicates, then the facts,
    /// handing a part of the response to the sink per batch,
    /// and returns the facts with their nodes.
    pub(in crate::engine) fn trace_in_parts(
        &mut self,
        predicates: &[String],
        facts: &[Fact],
        mut sink: impl FnMut(TracePart) -> Result<(), Diagnostic>,
    ) -> Result<Vec<TracedFact>, Diagnostic> {
        self.traceable()?;
        let named = self.ground_facts(facts)?;
        let mut trace = Trace::default();
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
                let (part, _) = trace.batch(self, &rows)?;
                sink(part)?;
            }
        }
        let (part, traced) = trace.batch(self, &named)?;
        sink(part)?;
        Ok(traced)
    }
}
