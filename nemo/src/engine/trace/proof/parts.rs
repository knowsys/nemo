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
    /// Traces every stored row of the given predicates, and then the given facts,
    /// as [trace](Self::trace) does.
    ///
    /// Returns the traced facts, each with its node.
    /// The response is passed to `sink` in [parts](crate::trace::TracePart) as it grows,
    /// one batch of rows at a time.
    /// Each fact is proven once and becomes one node,
    /// which is listed after the nodes of its premises.
    /// Every proof is kept in memory until the trace ends.
    ///
    /// # Errors
    ///
    /// Returns what [trace](Self::trace) returns for the facts,
    /// what [cursor](Self::cursor) returns for a predicate,
    /// and the first error of `sink`, which ends the tracing.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo::{engine::Engine, model::Program};
    ///
    /// let (program, _) = Program::parse("edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) .");
    /// let mut engine = Engine::new();
    /// engine.load(program)?;
    /// engine.reason()?;
    ///
    /// let mut parts = Vec::new();
    /// engine.trace_predicates(&["reach".to_owned()], &[], |part| {
    ///     parts.push(part);
    ///     Ok(())
    /// })?;
    ///
    /// // Node ids continue across the parts.
    /// let nodes: Vec<_> = parts.iter().flat_map(|part| &part.nodes).collect();
    /// let node = nodes
    ///     .iter()
    ///     .find(|node| node.fact == "reach(1, 2)")
    ///     .expect("the row is traced");
    /// let derivation = node.derivation.as_ref().expect("a rule derives the fact");
    /// let premises = derivation.premises.iter().map(|&premise| nodes[premise].fact.as_str());
    /// assert_eq!(node.inference(premises), "reach(1, 2) :- edge(1, 2) .");
    /// # Ok::<(), nemo::diagnostics::Diagnostic>(())
    /// ```
    pub fn trace_predicates(
        &mut self,
        predicates: &[String],
        facts: &[Fact],
        mut sink: impl FnMut(TracePart) -> Result<(), Diagnostic>,
    ) -> Result<Vec<TracedFact>, Diagnostic> {
        self.traceable()?;
        let named = self.ground_facts(facts)?;
        let mut trace = BatchedTrace::default();
        for predicate in predicates {
            let table = self.whole_predicate(predicate)?.table;
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
