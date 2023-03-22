use crate::physical::{
    datatypes::DataTypeName, tabular::operations::triescan_project::ProjectReordering,
    util::mapping::permutation::Permutation,
};
use std::fmt::Debug;

/// Type that stores the datatype used in each column of the table.
pub type TableColumnTypes = Vec<DataTypeName>;

/// Contains information about a column in table.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TableSchemaEntry {
    /// The data type in which the data is stored for this column.
    pub type_name: DataTypeName,
    /// Whether the entries in the column are the key for some dictionary.
    pub dict: bool,
    /// Whether this column may contain nulls.
    pub nullable: bool,
}

impl TableSchemaEntry {
    /// How is a String internally represented
    /// TODO: Reconsider this after typesystem got revised
    pub(crate) fn string() -> Self {
        Self {
            type_name: DataTypeName::U64,
            dict: true,
            nullable: false,
        }
    }
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

    /// Construct a new [`TableSchema`]  which is a reordered version of this schema
    pub fn reordered(&self, reordering: &ProjectReordering) -> Self {
        Self {
            entries: reordering.transform(&self.entries),
        }
    }

    /// Contruct a new [`TableSchema`] which is a permuted version of this schema.
    pub fn permuted(&self, permutation: &Permutation) -> Self {
        Self {
            entries: permutation.permute(&self.entries),
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

    /// Add new entry to the schema by cloning it.
    pub fn add_entry_cloned(&mut self, entry: &TableSchemaEntry) {
        self.entries.push(*entry);
    }

    /// The arity of the table.
    pub fn arity(&self) -> usize {
        self.entries.len()
    }

    /// The arity of the table.
    pub fn is_empty(&self) -> bool {
        self.arity() == 0
    }

    /// Returns the [`TableSchemaEntry`] associated with the column of the given index.
    pub fn get_entry(&self, index: usize) -> &TableSchemaEntry {
        &self.entries[index]
    }

    /// Returns the [`TableSchemaEntry`] associated with the column of the given index.
    pub fn get_entry_mut(&mut self, index: usize) -> &mut TableSchemaEntry {
        &mut self.entries[index]
    }

    /// Return the vector of [`TableSchemaEntry`]s which defines a [`TableSchema`].
    pub fn get_entries(&self) -> &Vec<TableSchemaEntry> {
        &self.entries
    }

    /// Return the types of the associated columns.
    pub fn get_column_types(&self) -> TableColumnTypes {
        self.entries.iter().map(|e| e.type_name).collect()
    }
}
