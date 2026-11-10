//! Imported rows: checking a block of rows given through
//! [import_rows](crate::engine::Engine::import_rows) against the program,
//! and encoding it for its predicate's table.

use nemo_physical::{
    diagnostics::Diagnostic,
    management::TableId,
    trie::RowBuffer,
    value::{DomainSet, Type},
};

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::translation::Translator,
        imported_rows::{RowBlock, row_note},
    },
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
        self.check_imported_rows(block)?;

        let delivered = layout.deliver(block.column_types().to_vec());
        let seeds = self
            .seeds
            .entry(table)
            .or_insert_with(|| vec![Type::EMPTY; delivered.len()]);
        for (seed, column_type) in seeds.iter_mut().zip(&delivered) {
            *seed = seed.union(column_type);
        }
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
                let Some(value) = self.dictionary.encode(value.as_ref()) else {
                    return Err(TranslationError::Unrepresentable
                        .at(None)
                        .with_note(row_note(name, row)));
                };
                encoded.push(value);
            }
            rows.push_row(&encoded);
        }
        Ok(Some((table, rows)))
    }

    /// Checks the values of a block as the values of facts are checked:
    /// the structures against the `@type` of their labels,
    /// and every value against the declaration of its column.
    ///
    /// Only the columns that the checks can refuse are read value by value:
    /// those with structures, and those whose type is not shown to lie within the declaration.
    /// The first refused value in row order is reported, as for facts.
    fn check_imported_rows(&self, block: &RowBlock) -> Result<(), Diagnostic> {
        let name = block.predicate();
        let declared = self
            .declarations
            .get(name)
            .map(|declaration| declaration.types.as_slice())
            .unwrap_or_default();
        let read: Vec<bool> = block
            .column_types()
            .iter()
            .enumerate()
            .map(|(column, column_type)| {
                column_type
                    .domains()
                    .intersects(DomainSet::TUPLE | DomainSet::MAP)
                    || declared
                        .get(column)
                        .is_some_and(|declared| !column_type.is_subset(declared, &self.label_types))
            })
            .collect();
        if !read.contains(&true) {
            return Ok(());
        }
        for row in block.rows() {
            for (position, value) in row.iter().enumerate() {
                if !read[position] {
                    continue;
                }
                self.check_value_types(value.as_ref(), None)
                    .and_then(|()| {
                        self.check_fact_declaration(name, position, value.as_ref(), None)
                    })
                    .map_err(|problem| problem.with_note(row_note(name, row)))?;
            }
        }
        Ok(())
    }
}
