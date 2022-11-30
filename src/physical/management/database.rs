use core::hash::Hash;
use std::collections::HashMap;

use crate::physical::tabular::tries::Trie;

/// Type which represents table id
pub type TableId = usize;

/// Represents a collection of tables
#[derive(Debug)]
pub struct DatabaseInstance<TableKey: Eq + Hash + Clone> {
    id_to_table: HashMap<TableId, (TableKey, Trie)>,
    key_to_tableid: HashMap<TableKey, TableId>,

    current_id: usize,
}

impl<TableKey: Eq + Hash + Clone> DatabaseInstance<TableKey> {
    /// Create new [`DatabaseInstance`]
    pub fn new() -> Self {
        Self {
            id_to_table: HashMap::new(),
            key_to_tableid: HashMap::new(),
            current_id: 0,
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
    pub fn delete_by_key(&mut self, key: &TableKey) -> bool {
        let id = if let Some(some_id) = self.get_id(&key) {
            some_id
        } else {
            return false;
        };

        self.id_to_table.remove(&id);
        self.key_to_tableid.remove(key);

        true
    }

    /// Delete a trie given its id
    /// Returns true if the id existed
    pub fn delete_by_id(&mut self, id: TableId) -> bool {
        let key = if let Some(key_trie_pair) = self.id_to_table.get(&id) {
            &key_trie_pair.0
        } else {
            return false;
        };

        self.key_to_tableid.remove(key);
        self.id_to_table.remove(&id);

        true
    }
}
