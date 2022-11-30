use core::hash::Hash;
use std::collections::{hash_map::Entry, HashMap};

use bytesize::ByteSize;

use crate::physical::tabular::table_types::trie::Trie;

use super::ByteSized;

/// Type which represents table id
pub type TableId = usize;
/// Traits which have to be satisfied by a TableKey type
pub trait TableKeyType: Eq + Hash + Clone {}

/// Represents a collection of tables
#[derive(Debug)]
pub struct DatabaseInstance<TableKey: TableKeyType> {
    /// Structure which owns all the tries; accessed through Id
    id_to_table: HashMap<TableId, (TableKey, Trie)>,
    /// Alternative access scheme through a TableKey
    key_to_tableid: HashMap<TableKey, TableId>,

    /// Lowest unused TableId
    /// Will be incremented for each new table and will never be reused
    current_id: usize,
    /// Memory currently consumed by all the tables
    memory_consumption: ByteSize,
}

impl<TableKey: TableKeyType> DatabaseInstance<TableKey> {
    /// Create new [`DatabaseInstance`]
    pub fn new() -> Self {
        Self {
            id_to_table: HashMap::new(),
            key_to_tableid: HashMap::new(),
            current_id: 0,
            memory_consumption: ByteSize::b(0),
        }
    }

    /// Return the id of a table given a key, if it exists
    pub fn get_id(&self, key: &TableKey) -> Option<TableId> {
        self.key_to_tableid.get(key).copied()
    }

    /// Return the key of a table given its id, if it exists
    pub fn get_key(&self, id: TableId) -> Option<TableKey> {
        Some(self.id_to_table.get(&id)?.0.clone())
    }

    /// Given a id return a reference to the corresponding trie, if it exists
    pub fn get_by_id(&self, id: TableId) -> Option<&Trie> {
        self.id_to_table.get(&id).map(|t| &t.1)
    }

    /// Given a id return a mutable reference to the corresponding trie, if it exists
    pub fn get_by_id_mut(&mut self, id: TableId) -> Option<&mut Trie> {
        self.id_to_table.get_mut(&id).map(|t| &mut t.1)
    }

    /// Given a [`TableKey`] return a reference to the corresponding trie, if it exists
    pub fn get_by_key(&self, key: &TableKey) -> Option<&Trie> {
        let id = self.get_id(key)?;
        self.id_to_table.get(&id).map(|t| &t.1)
    }

    /// Given a [`TableKey`] return a mutable reference to the corresponding trie, if it exists
    pub fn get_by_key_mut(&mut self, key: &TableKey) -> Option<&mut Trie> {
        let id = self.get_id(key)?;
        self.id_to_table.get_mut(&id).map(|t| &mut t.1)
    }

    /// Add the given trie to the instance under the given table key
    /// Returns the id of the new table or None if a table with such a key was already present
    pub fn add(&mut self, key: TableKey, trie: Trie) -> Option<TableId> {
        if self.get_id(&key).is_some() {
            return None;
        }

        self.memory_consumption += trie.size_bytes();

        self.id_to_table
            .insert(self.current_id, (key.clone(), trie));
        self.key_to_tableid.insert(key, self.current_id);

        self.current_id += 1;
        Some(self.current_id)
    }

    /// Replace a trie (searched by key) with another
    /// Will add a new trie if the key does not exist
    /// Returns true if the key existed
    pub fn update_by_key(&mut self, key: TableKey, trie: Trie) -> bool {
        let id = if let Some(some_id) = self.get_id(&key) {
            some_id
        } else {
            return false;
        };

        self.id_to_table.get_mut(&id).unwrap().1 = trie;

        true
    }

    /// Replace a trie (searched by id) with another
    /// Will add a new trie if the id does not exist
    /// Returns true if the id existed
    pub fn update_by_id(&mut self, id: TableId, trie: Trie) -> bool {
        if let Some(current_trie) = self.id_to_table.get_mut(&id) {
            current_trie.1 = trie;
            return true;
        }

        false
    }

    /// Delete a trie given its key
    /// Returns true if the key existed
    pub fn delete_by_key(&mut self, key: TableKey) -> bool {
        if let Entry::Occupied(key_entry) = self.key_to_tableid.entry(key) {
            let id = *key_entry.get();
            key_entry.remove();

            if let Entry::Occupied(id_entry) = self.id_to_table.entry(id) {
                self.memory_consumption = ByteSize(
                    self.memory_consumption.as_u64() - id_entry.get().1.size_bytes().as_u64(),
                );

                id_entry.remove();
            } else {
                // Both HashMap should contain the table
                unreachable!()
            }

            true
        } else {
            false
        }
    }

    /// Delete a trie given its id
    /// Returns true if the id existed
    pub fn delete_by_id(&mut self, id: TableId) -> bool {
        if let Entry::Occupied(entry) = self.id_to_table.entry(id) {
            self.memory_consumption =
                ByteSize(self.memory_consumption.as_u64() - entry.get().1.size_bytes().as_u64());

            self.key_to_tableid.remove(&entry.get().0);
            entry.remove();

            true
        } else {
            false
        }
    }
}

impl<TableKey: TableKeyType> ByteSized for DatabaseInstance<TableKey> {
    fn size_bytes(&self) -> ByteSize {
        self.memory_consumption
    }
}
