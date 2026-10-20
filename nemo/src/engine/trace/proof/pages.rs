//! Tracing a predicate's stored rows a page at a time,
//! so that every fact of a predicate is traced without holding every proof at once.

use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::DataValue};

use crate::{
    engine::{Engine, trace::proof::run},
    trace::TraceResponse,
};

impl Engine {
    /// Traces the rows of one page of a predicate.
    pub(in crate::engine) fn trace_page(
        &mut self,
        predicate: &str,
        start: usize,
        budget: usize,
    ) -> Result<TraceResponse, Diagnostic> {
        self.traceable()?;
        let (table, _) = self.whole_predicate(predicate)?;
        let mut cursor = self.cursor(predicate)?;
        cursor.position = start.min(cursor.position + cursor.remaining());
        let chunk = self.read_chunk(&mut cursor, budget.max(1))?;
        let facts: Vec<(TableId, Vec<DataValue>)> = chunk.rows().map(|row| (table, row)).collect();
        run(self, facts)
    }

    /// Traces the rows of predicates page by page, handing each page to the sink.
    pub(in crate::engine) fn trace_in_pages(
        &mut self,
        predicates: &[String],
        budget: usize,
        mut sink: impl FnMut(TraceResponse) -> Result<(), Diagnostic>,
    ) -> Result<(), Diagnostic> {
        let budget = budget.max(1);
        for predicate in predicates {
            let mut start = 0;
            loop {
                let response = self.trace_rows(predicate, start, budget)?;
                let traced = response.facts.len();
                if traced == 0 {
                    break;
                }
                sink(response)?;
                start += traced;
                if traced < budget {
                    break;
                }
            }
        }
        Ok(())
    }
}
