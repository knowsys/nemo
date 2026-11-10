//! Imported rows: checking a block of rows given through
//! [import_rows](crate::engine::Engine::import_rows) against the program,
//! and encoding it for its predicate's table.

use nemo_physical::{diagnostics::Diagnostic, management::TableId, trie::RowBuffer};

use crate::engine::{
    compile::translation::Translator,
    imported_rows::{RowBlock, row_note},
};

impl<'a> Translator<'a> {
    /// Checks a block of imported rows against the program,
    /// seeds the type analysis with its column types,
    /// and returns its predicate's table with the rows encoded for it.
    ///
    /// The rows pass the checks that a fact of the program passes,
    /// and the first row that a fact would be refused for is refused with the same diagnostic,
    /// with a note that names the row.
    /// A translation that reads no data encodes nothing, and returns `None`.
    pub(crate) fn imported_rows(
        &mut self,
        block: &RowBlock,
    ) -> Result<Option<(TableId, RowBuffer)>, Diagnostic> {
        let name = block.predicate();
        let first = |problem: Diagnostic| problem.with_note(row_note(name, block.first_row()));
        let layout = self.row_layout(name, block.arity(), None).map_err(first)?;
        let table = self
            .table_named(name.to_owned(), layout.delivered(), None)
            .map_err(first)?;
        // A first load that a pipeline stage rewrote checks the rows twice:
        // for the written program, and here for the rewritten one,
        // since a stage may change the declarations that the rows are checked against.
        self.check_imported_rows(block)?;
        self.seed(table, &layout.deliver(block.column_types().to_vec()));
        if !self.purpose.reads() {
            return Ok(None);
        }

        let mut rows = RowBuffer::new(layout.delivered());
        let mut encoded = Vec::with_capacity(layout.delivered());
        for row in block.rows() {
            encoded.clear();
            for (kept, value) in row.iter().enumerate() {
                if layout.is_pruned(kept) {
                    continue;
                }
                let value = self
                    .encode_value(value.as_ref(), None)
                    .map_err(|problem| problem.with_note(row_note(name, row)))?;
                encoded.push(value);
            }
            rows.push_row(&encoded);
        }
        Ok(Some((table, rows)))
    }

    /// Checks the values of a block as the values of facts are checked
    /// ([check_value](Self::check_value)).
    ///
    /// Only the columns whose type the check does not [accept](Self::accepts_column) whole
    /// are read value by value.
    /// The first refused value in row order is reported, as for facts.
    fn check_imported_rows(&self, block: &RowBlock) -> Result<(), Diagnostic> {
        let name = block.predicate();
        let read: Vec<bool> = block
            .column_types()
            .iter()
            .enumerate()
            .map(|(column, column_type)| !self.accepts_column(name, column, column_type))
            .collect();
        if !read.contains(&true) {
            return Ok(());
        }
        for row in block.rows() {
            for (position, value) in row.iter().enumerate() {
                if read[position] {
                    self.check_value(name, position, value.as_ref(), None)
                        .map_err(|problem| problem.with_note(row_note(name, row)))?;
                }
            }
        }
        Ok(())
    }
}
