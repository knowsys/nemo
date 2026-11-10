//! Rows given through [import_rows](crate::engine::Engine::import_rows):
//! blocks of values, one per predicate, with the types of their columns.
//!
//! The rows are kept apart from the program's statements,
//! so the pipeline stages see only the names of their predicates.
//! The translation checks a block against the program as a whole,
//! reading values one by one only where the column types do not already show that they pass.

use std::collections::HashMap;

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{DataValue, DomainSet, Type, ValueRef},
};

use crate::{
    diagnostics::{EngineError, TranslationError},
    model::{Identifier, Term},
};

/// The rows given to one predicate, row-major, with the union of each column's value types.
#[derive(Debug, Clone)]
pub(in crate::engine) struct RowBlock {
    /// The predicate's resolved name.
    predicate: String,
    /// The number of values per row.
    arity: usize,
    /// The number of rows, which the values alone do not give for rows without values.
    rows: usize,
    /// The rows' values, row-major.
    values: Vec<DataValue>,
    /// Per column, the union of the types of its values.
    column_types: Vec<Type>,
}

impl RowBlock {
    /// Reads the rows given to a predicate into a block, or returns `None` if there are none.
    ///
    /// Every row needs as many values as the first, or as `arity` if it is given.
    /// A row with another number of values is an error, and so is a row that contains a null,
    /// since no term denotes a null.
    pub(in crate::engine) fn read(
        predicate: &str,
        rows: impl IntoIterator<Item = Vec<DataValue>>,
        arity: Option<usize>,
    ) -> Result<Option<Self>, Diagnostic> {
        let mut rows = rows.into_iter().peekable();
        let Some(first) = rows.peek() else {
            return Ok(None);
        };
        let arity = arity.unwrap_or(first.len());
        let mut values = Vec::with_capacity(arity * rows.size_hint().0);
        let mut columns = vec![ColumnType::new(); arity];
        let mut count = 0;
        for row in rows {
            if row.iter().any(|value| contains_null(value.as_ref())) {
                return Err(EngineError::NullInRow {
                    predicate: predicate.to_owned(),
                }
                .diagnostic());
            }
            if row.len() != arity {
                return Err(TranslationError::ArityMismatch {
                    name: Identifier::bare(predicate.to_owned()).to_string(),
                    arity: row.len().into(),
                    registered: arity.into(),
                }
                .at(None)
                .with_note(row_note(predicate, &row)));
            }
            for (column, value) in columns.iter_mut().zip(&row) {
                column.add(value.as_ref());
            }
            values.extend(row);
            count += 1;
        }
        Ok(Some(Self {
            predicate: predicate.to_owned(),
            arity,
            rows: count,
            values,
            column_types: columns.into_iter().map(ColumnType::finish).collect(),
        }))
    }

    /// Returns the predicate's resolved name.
    pub(in crate::engine) fn predicate(&self) -> &str {
        &self.predicate
    }

    /// Returns the number of values per row.
    pub(in crate::engine) fn arity(&self) -> usize {
        self.arity
    }

    /// Returns the type of each column: the union of the types of its values.
    pub(in crate::engine) fn column_types(&self) -> &[Type] {
        &self.column_types
    }

    /// Returns the rows, in the order in which they were given.
    pub(in crate::engine) fn rows(&self) -> impl Iterator<Item = &[DataValue]> {
        let arity = self.arity;
        (0..self.rows).map(move |row| &self.values[row * arity..(row + 1) * arity])
    }

    /// Returns the first row.
    pub(in crate::engine) fn first_row(&self) -> &[DataValue] {
        &self.values[..self.arity]
    }

    /// Adds the rows of another block of the same predicate and arity.
    fn append(&mut self, other: RowBlock) {
        debug_assert_eq!(
            self.arity, other.arity,
            "the blocks of a predicate share an arity"
        );
        self.rows += other.rows;
        self.values.extend(other.values);
        for (column, added) in self.column_types.iter_mut().zip(&other.column_types) {
            *column = column.union(added);
        }
    }
}

/// The union of the types of a column's values, collected value by value.
#[derive(Debug, Clone)]
struct ColumnType {
    /// The domains of the values that are not structures.
    domains: DomainSet,
    /// The union of the types of the tuples and maps.
    structures: Option<Type>,
}

impl ColumnType {
    /// Creates the type of a column without values.
    fn new() -> Self {
        Self {
            domains: DomainSet::EMPTY,
            structures: None,
        }
    }

    /// Adds a value's type.
    fn add(&mut self, value: ValueRef<'_>) {
        match value {
            ValueRef::Tuple(_) | ValueRef::Map(_) => {
                let added = Type::of_value(value);
                self.structures = Some(match self.structures.take() {
                    Some(structures) => structures.union(&added),
                    None => added,
                });
            }
            other => self.domains |= DomainSet::of(other.domain()),
        }
    }

    /// Returns the union of the added types.
    fn finish(self) -> Type {
        let scalars = Type::of_domains(self.domains);
        match self.structures {
            Some(structures) => scalars.union(&structures),
            None => scalars,
        }
    }
}

/// The rows given through [import_rows](crate::engine::Engine::import_rows) before a program
/// is loaded, one block per predicate, in the order of the first call for each.
#[derive(Debug, Default)]
pub(in crate::engine) struct UnloadedRows {
    /// The blocks.
    blocks: Vec<RowBlock>,
    /// The position of each predicate's block in [blocks](Self::blocks).
    positions: HashMap<String, usize>,
}

impl UnloadedRows {
    /// Returns the number of values per row of a predicate's rows,
    /// or `None` if none were given.
    pub(in crate::engine) fn arity(&self, predicate: &str) -> Option<usize> {
        self.positions
            .get(predicate)
            .map(|&position| self.blocks[position].arity)
    }

    /// Adds a block to the rows of its predicate, which must have its arity.
    pub(in crate::engine) fn add(&mut self, block: RowBlock) {
        match self.positions.get(block.predicate()) {
            Some(&position) => self.blocks[position].append(block),
            None => {
                self.positions
                    .insert(block.predicate.clone(), self.blocks.len());
                self.blocks.push(block);
            }
        }
    }

    /// Returns the blocks, one per predicate.
    pub(in crate::engine) fn blocks(&self) -> &[RowBlock] {
        &self.blocks
    }

    /// Returns the names of the predicates that were given rows.
    pub(in crate::engine) fn predicates(&self) -> Vec<String> {
        self.blocks
            .iter()
            .map(|block| block.predicate.clone())
            .collect()
    }
}

/// Returns the note that names a row given to [import_rows](crate::engine::Engine::import_rows),
/// which has no source position to point at.
pub(in crate::engine) fn row_note(predicate: &str, row: &[DataValue]) -> String {
    let terms: Vec<String> = row
        .iter()
        .map(|value| Term::value(value).map_or_else(|| value.to_string(), |term| term.to_string()))
        .collect();
    format!(
        "the row given to `import_rows` is `{}({})`",
        Identifier::Bare(predicate.to_owned()),
        terms.join(", ")
    )
}

/// Returns whether a value is a null or a structure that contains one.
fn contains_null(value: ValueRef<'_>) -> bool {
    match value {
        ValueRef::Null(_) => true,
        ValueRef::Tuple(tuple) => tuple.elements().any(contains_null),
        ValueRef::Map(map) => map
            .pairs()
            .any(|(key, value)| contains_null(key) || contains_null(value)),
        _ => false,
    }
}
