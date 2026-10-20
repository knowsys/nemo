//! The check, in debug builds, that every value stored in a table lies within the types the
//! analysis inferred for its columns.
//!
//! A program runs on the analysis's proof that every write fits its declarations,
//! so a stored value outside the inferred types is a bug in the analysis.
//! Debug builds hold every stored block to the inferred types,
//! and every stored value carrying a label with a `@type` to that type,
//! so that every test that stores rows also tests the analysis.

use std::collections::HashMap;

use nemo_physical::{
    management::TableId,
    storage::StorageValue,
    trie::{RowBuffer, Trie},
    value::{LabelTypes, Type, ValueRef},
};

use crate::engine::Engine;

impl Engine {
    /// Panics if a block of rows about to be stored in a table holds a value outside the
    /// table's column types, or a labeled value outside the `@type` of its label.
    ///
    /// A table without column types, such as a scratch table, is not checked.
    pub(in crate::engine) fn assert_stored_types(
        &self,
        column_types: &HashMap<TableId, Vec<Type>>,
        table: TableId,
        rows: &RowBuffer,
    ) {
        for index in 0..rows.len() {
            self.assert_stored_row(column_types, table, rows.row(index));
        }
    }

    /// Panics as [assert_stored_types](Self::assert_stored_types) does, for the rows of a trie.
    pub(in crate::engine) fn assert_stored_trie(
        &self,
        column_types: &HashMap<TableId, Vec<Type>>,
        table: TableId,
        trie: &Trie,
    ) {
        trie.for_each_row(|row| self.assert_stored_row(column_types, table, row));
    }

    /// Panics if one row about to be stored in a table holds a value outside its column types,
    /// or a labeled value outside the `@type` of its label.
    fn assert_stored_row(
        &self,
        column_types: &HashMap<TableId, Vec<Type>>,
        table: TableId,
        row: &[StorageValue],
    ) {
        let Some(types) = column_types.get(&table) else {
            return;
        };
        let labels = &self.translation.label_types;
        for (column, (&stored, inferred)) in row.iter().zip(types).enumerate() {
            let value = self
                .dictionary
                .decode(stored)
                .expect("a stored value decodes");
            assert!(
                inferred.admits(value, labels),
                "the analysis inferred {inferred:?} for column {column} of {table:?}, \
                 but {value:?} is stored there"
            );
            assert!(
                fits_its_labels(value, labels),
                "{value:?} is stored in column {column} of {table:?}, \
                 but it carries a label whose `@type` it does not fit"
            );
        }
    }
}

/// Returns whether every structure in a value that carries a label with a `@type` fits that type.
fn fits_its_labels(value: ValueRef<'_>, labels: &LabelTypes) -> bool {
    let fits = |label: Option<&str>| {
        label
            .and_then(|label| labels.get(label))
            .is_none_or(|declared| declared.admits(value, labels))
    };
    match value {
        ValueRef::Tuple(tuple) => {
            fits(tuple.label())
                && tuple
                    .elements()
                    .all(|element| fits_its_labels(element, labels))
        }
        ValueRef::Map(map) => {
            fits(map.label())
                && map.pairs().all(|(key, value)| {
                    fits_its_labels(key, labels) && fits_its_labels(value, labels)
                })
        }
        _ => true,
    }
}
