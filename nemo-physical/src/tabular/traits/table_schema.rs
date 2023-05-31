use std::{
    ops::{Index, IndexMut},
    slice::Iter,
};

use crate::{
    datatypes::{DataTypeName, StorageTypeName},
    tabular::operations::triescan_project::ProjectReordering,
    util::mapping::permutation::Permutation,
};

// TODO: move file somewhere else; there is no trait here

/// Type that stores the datatype used in each column of the table.
#[repr(transparent)]
#[derive(Clone, Debug, Default)]
pub struct TableSchema(Vec<DataTypeName>);

impl TableSchema {
    /// Constructs new (empty) [`TableSchema`].
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Turns Vector into [`TableSchema`].
    pub fn from_vec(vec: Vec<DataTypeName>) -> Self {
        Self(vec)
    }

    /// Construct a new [`TableSchema`]  which is a reordered version of this schema
    pub fn reordered(&self, reordering: &ProjectReordering) -> Self {
        Self(reordering.transform(&self.0))
    }

    /// Contruct a new [`TableSchema`] which is a permuted version of this schema.
    pub fn permuted(&self, permutation: &Permutation) -> Self {
        Self(permutation.permute(&self.0))
    }

    /// Constructs new (empty) [`TableSchema`] with reserved space.
    pub fn with_capacity(arity: usize) -> Self {
        Self(Vec::with_capacity(arity))
    }

    /// Constructs new (empty) [`TableSchema`] with reserved space.
    pub fn reserve(arity: usize) -> Self {
        Self::with_capacity(arity)
    }

    /// Push to the underlying vector.
    pub fn push(&mut self, type_name: DataTypeName) {
        self.0.push(type_name);
    }

    /// Add new entry to the schema.
    pub fn add_entry(&mut self, type_name: DataTypeName) {
        self.push(type_name);
    }

    /// Add new entry to the schema by cloning it.
    pub fn add_entry_cloned(&mut self, entry: &DataTypeName) {
        self.push(*entry);
    }

    /// The length of the underlying vector.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The arity of the table.
    pub fn arity(&self) -> usize {
        self.len()
    }

    /// Bool indicating if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the [`DataTypeName`] associated with the column of the given index.
    pub fn get_entry(&self, index: usize) -> &DataTypeName {
        &self[index]
    }

    /// Returns the [`DataTypeName`] associated with the column of the given index.
    pub fn get_entry_mut(&mut self, index: usize) -> &mut DataTypeName {
        &mut self[index]
    }

    /// Returns an iterator for the underlying vector
    pub fn iter(&self) -> Iter<DataTypeName> {
        self.0.iter()
    }

    /// return fitting storage types for schema
    pub fn get_storage_types(&self) -> Vec<StorageTypeName> {
        self.iter()
            .map(DataTypeName::to_storage_type_name)
            .collect()
    }
}

impl Index<usize> for TableSchema {
    type Output = DataTypeName;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for TableSchema {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}
