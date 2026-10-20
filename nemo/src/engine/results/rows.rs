//! Reading a predicate back: whole, paged through a cursor, or
//! counted.

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{ColumnOrder, Revision},
};

use crate::{
    diagnostics::EngineError,
    engine::{Engine, PredicateCursor, RowChunk},
};

impl Engine {
    /// Opens a cursor over a predicate stored whole.
    pub(in crate::engine) fn open_cursor(
        &self,
        predicate: &str,
    ) -> Result<PredicateCursor, Diagnostic> {
        let (table, arity) = self.whole_predicate(predicate)?;
        let range = Revision(0)..Revision(self.step.max(1));
        let rows = self.manager.row_count(table, range.clone());
        let blocks = self.manager.blocks(table);
        Ok(PredicateCursor {
            table,
            range,
            arity,
            position: 0,
            rows,
            blocks,
        })
    }

    /// Decodes the next rows of a current cursor into a chunk.
    pub(in crate::engine) fn decode_chunk(
        &mut self,
        cursor: &mut PredicateCursor,
        budget: usize,
    ) -> Result<RowChunk, Diagnostic> {
        let mut chunk = RowChunk::new(cursor.arity);
        if cursor.is_done() || budget == 0 {
            return Ok(chunk);
        }
        if !self.is_current(cursor) {
            return Err(EngineError::CursorInvalidated.diagnostic());
        }
        let order = ColumnOrder::identity(cursor.arity);
        self.manager
            .ensure_order(cursor.table, cursor.range.clone(), &order);

        // The walk enters at the cursor's position and decoding stops
        // at the budget.
        let mut decoded = Vec::with_capacity(cursor.arity);
        self.manager.for_each_row_from(
            cursor.table,
            cursor.range.clone(),
            &order,
            cursor.position,
            |row| {
                decoded.clear();
                decoded.extend(row.iter().map(|&value| {
                    self.dictionary
                        .decode(value)
                        .expect("stored values decode against their dictionary")
                        .to_owned()
                }));
                chunk.push(&decoded);
                cursor.position += 1;
                if chunk.len() < budget {
                    std::ops::ControlFlow::Continue(())
                } else {
                    std::ops::ControlFlow::Break(())
                }
            },
        );
        Ok(chunk)
    }
}
