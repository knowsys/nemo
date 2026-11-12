//! Reading a predicate's stored rows through a cursor.

use std::ops::ControlFlow;

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{ColumnOrder, Revision},
};

use crate::{
    diagnostics::EngineError,
    engine::{Engine, PredicateCursor, RowChunk, stores::Registered},
};

impl Engine {
    /// Opens a cursor over a predicate's stored rows, for [read_chunk](Self::read_chunk) to read.
    ///
    /// # Errors
    ///
    /// Returns [NO_PROGRAM](crate::codes::NO_PROGRAM) if no program is loaded,
    /// [UNKNOWN_PREDICATE](crate::codes::UNKNOWN_PREDICATE) for a predicate the program does not
    /// have, and [NOT_STORED_WHOLE](crate::codes::NOT_STORED_WHOLE) for one the engine does not
    /// store whole, naming what is left out.
    pub fn cursor(&self, predicate: &str) -> Result<PredicateCursor, Diagnostic> {
        let Registered { table, arity } = self.whole_predicate(predicate)?;
        let range = Revision(0)..Revision(self.run.step.max(1));
        let rows = self.stores.manager.row_count(table, range.clone());
        let blocks = self.stores.manager.blocks(table);

        Ok(PredicateCursor {
            table,
            range,
            arity,
            position: 0,
            rows,
            blocks,
        })
    }

    /// Returns the next rows of a cursor, at most `budget` of them, decoded into a [RowChunk].
    ///
    /// The chunk is empty once the cursor has delivered every row.
    /// The cursor holds no borrow of the engine, and each read borrows the engine only while it
    /// runs.
    ///
    /// # Errors
    ///
    /// Returns [CURSOR_INVALIDATED](crate::codes::CURSOR_INVALIDATED) for a cursor whose table
    /// changed after the cursor was opened, since its remaining rows would no longer match the
    /// table.
    /// [is_valid](Self::is_valid) checks for this beforehand.
    pub fn read_chunk(
        &mut self,
        cursor: &mut PredicateCursor,
        budget: usize,
    ) -> Result<RowChunk, Diagnostic> {
        let mut chunk = RowChunk::new(cursor.arity);
        if cursor.is_done() || budget == 0 {
            return Ok(chunk);
        }
        if !self.is_valid(cursor) {
            return Err(EngineError::CursorInvalidated.diagnostic());
        }

        let order = ColumnOrder::identity(cursor.arity);
        self.stores
            .manager
            .ensure_order(cursor.table, cursor.range.clone(), &order);

        // The walk enters at the cursor's position and decoding stops
        // at the budget.
        let mut decoded = Vec::with_capacity(cursor.arity);
        self.stores.manager.for_each_row_from(
            cursor.table,
            cursor.range.clone(),
            &order,
            cursor.position,
            |row| {
                decoded.clear();
                decoded.extend(row.iter().map(|&value| {
                    self.stores
                        .dictionary
                        .decode(value)
                        .expect("stored values decode against their dictionary")
                        .to_owned()
                }));
                chunk.push(&decoded);
                cursor.position += 1;
                if chunk.len() < budget {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            },
        );
        Ok(chunk)
    }
}
