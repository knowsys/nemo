use crate::physical::datatypes::DataTypeName;
use std::fmt::Debug;

/// Type that stores the datatype used in each column of the table.
pub type TableColumnTypes = Vec<DataTypeName>;

/// Contains information about a column in table.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TableSchemaEntry {
    /// The data type in which the data is stored for this column.
    type_name: DataTypeName,
    /// Whether the entries in the column are the key for some dictionary.
    dict: bool,
    /// Whether this column may contain nulls.
    nullable: bool,
}

/// Schema for a particular relation (table).
/// Each column has a datatype.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct TableSchema {
    entries: Vec<TableSchemaEntry>,
}

impl TableSchema {
    /// Constructs new (empty) [`TableSchema`].
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Constructs new (empty) [`TableSchema`] with reserved space.
    pub fn reserve(arity: usize) -> Self {
        let entries = Vec::with_capacity(arity);

        Self { entries }
    }

    /// Constructs new [`TableSchema`] with the given entries.
    pub fn from_vec(entries: Vec<TableSchemaEntry>) -> Self {
        Self { entries }
    }

    /// Add new entry to the schema.
    pub fn add_entry(&mut self, type_name: DataTypeName, dict: bool, nullable: bool) {
        self.entries.push(TableSchemaEntry {
            type_name,
            dict,
            nullable,
        });
    }

    /// The arity of the table.
    pub fn arity(&self) -> usize {
        self.entries.len()
    }
}
