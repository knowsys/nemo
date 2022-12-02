use core::hash::Hash;
use std::{
    borrow::Borrow,
    collections::{hash_map::Entry, HashMap},
};

use bytesize::ByteSize;

use crate::physical::{
    datatypes::DataTypeName,
    tabular::{
        operations::{
            materialize::{materialize, materialize_subset},
            TrieScanJoin, TrieScanMinus, TrieScanProject, TrieScanSelectEqual, TrieScanSelectValue,
            TrieScanUnion,
        },
        table_types::trie::{Trie, TrieScanGeneric, TrieSchema, TrieSchemaEntry},
        traits::{
            table_schema::TableSchema,
            triescan::{TrieScan, TrieScanEnum},
        },
    },
};

use super::{
    execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult},
    ByteSized, ExecutionPlan,
};

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

    /// Executes a given [`ExecutionPlan`]
    /// Returns true if a new (non-empty) table has been saved permanently
    pub fn execute_plan(&mut self, plan: &ExecutionPlan<TableKey>) -> bool {
        let mut new_table = false;
        let mut temp_tries = HashMap::<TableId, Option<Trie>>::new();

        for tree in &plan.trees {
            let iter_option = self.get_iterator_node(tree.root(), &temp_tries);

            if let Some(mut iter) = iter_option {
                let new_trie_opt =
                    if let ExecutionResult::TempSubset(_, picked_columns) = tree.result() {
                        materialize_subset(&mut iter, picked_columns.clone())
                    } else {
                        materialize(&mut iter)
                    };

                match tree.result() {
                    ExecutionResult::Temp(id) | ExecutionResult::TempSubset(id, _) => {
                        temp_tries.insert(*id, new_trie_opt);
                    }
                    ExecutionResult::Save(key) => {
                        if let Some(new_trie) = new_trie_opt {
                            self.add(key.clone(), new_trie);
                            new_table = true;
                        }
                    }
                }
            }
        }

        new_table
    }

    /// Given a Node in the execution tree returns the trie iterator
    /// that if materialized will turn into the resulting trie of the represented computation
    fn get_iterator_node<'a>(
        &'a self,
        node: ExecutionNodeRef,
        temp_tries: &'a HashMap<TableId, Option<Trie>>,
    ) -> Option<TrieScanEnum> {
        if let Some(self_rc) = node.0.upgrade() {
            let node_ref = &*self_rc.as_ref().borrow();
            let node_operation = node_ref.borrow();

            return match node_operation {
                ExecutionNode::FetchTemp(id) => Some(TrieScanEnum::TrieScanGeneric(
                    TrieScanGeneric::new(temp_tries.get(id)?.as_ref()?),
                )),
                ExecutionNode::FetchTable(id) => {
                    let trie = self.get_by_id(*id)?;
                    let interval_triescan = TrieScanGeneric::new(trie);
                    Some(TrieScanEnum::TrieScanGeneric(interval_triescan))
                }
                ExecutionNode::Join(subtables, bindings) => {
                    let mut subiterators: Vec<TrieScanEnum> = subtables
                        .iter()
                        .map(|s| self.get_iterator_node(s.clone(), temp_tries))
                        .filter(|s| s.is_some())
                        .flatten()
                        .collect();

                    // If subtables contain an empty table, then the join is empty
                    if subiterators.len() != subtables.len() {
                        return None;
                    }

                    // If it only contains one table, then we dont need the join
                    if subiterators.len() == 1 {
                        return Some(subiterators.remove(0));
                    }

                    let mut datatype_map = HashMap::<usize, DataTypeName>::new();
                    for (atom_index, binding) in bindings.iter().enumerate() {
                        for (term_index, variable) in binding.iter().enumerate() {
                            datatype_map.insert(
                                *variable,
                                subiterators[atom_index].get_schema().get_type(term_index),
                            );
                        }
                    }

                    let mut attributes = Vec::new();
                    let mut variable: usize = 0;
                    while let Some(datatype) = datatype_map.get(&variable) {
                        attributes.push(TrieSchemaEntry {
                            label: 0, // TODO: This should get perhaps a new label
                            datatype: *datatype,
                        });
                        variable += 1;
                    }

                    let schema = TrieSchema::new(attributes);

                    Some(TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
                        subiterators,
                        bindings,
                        schema,
                    )))
                }
                ExecutionNode::Union(subtables) => {
                    let mut subiterators: Vec<TrieScanEnum> = subtables
                        .iter()
                        .map(|s| self.get_iterator_node(s.clone(), temp_tries))
                        .filter(|s| s.is_some())
                        .flatten()
                        .collect();

                    // The union of empty tables is empty
                    if subiterators.is_empty() {
                        return None;
                    }

                    // If it only contains one table, then we dont need the join
                    if subiterators.len() == 1 {
                        return Some(subiterators.remove(0));
                    }

                    let union_scan = TrieScanUnion::new(subiterators);

                    Some(TrieScanEnum::TrieScanUnion(union_scan))
                }
                ExecutionNode::Minus(subtable_left, subtable_right) => {
                    let left_scan = self.get_iterator_node(subtable_left.clone(), temp_tries)?;
                    let right_scan_option =
                        self.get_iterator_node(subtable_right.clone(), temp_tries);

                    if let Some(right_scan) = right_scan_option {
                        Some(TrieScanEnum::TrieScanMinus(TrieScanMinus::new(
                            left_scan, right_scan,
                        )))
                    } else {
                        Some(left_scan)
                    }
                }
                ExecutionNode::Project(id, sorting) => {
                    let tmp_trie = temp_tries.get(id)?.as_ref()?;
                    let project_scan = TrieScanProject::new(tmp_trie, sorting.clone());

                    Some(TrieScanEnum::TrieScanProject(project_scan))
                }
                ExecutionNode::SelectValue(subtable, assignments) => {
                    let subiterator = self.get_iterator_node(subtable.clone(), temp_tries)?;
                    let select_scan = TrieScanSelectValue::new(subiterator, assignments);

                    Some(TrieScanEnum::TrieScanSelectValue(select_scan))
                }
                ExecutionNode::SelectEqual(subtable, classes) => {
                    let subiterator = self.get_iterator_node(subtable.clone(), temp_tries)?;
                    let select_scan = TrieScanSelectEqual::new(subiterator, classes);

                    Some(TrieScanEnum::TrieScanSelectEqual(select_scan))
                }
            };
        } else {
            unreachable!("We never delete nodes from the plan");
        };
    }
}

impl<TableKey: TableKeyType> ByteSized for DatabaseInstance<TableKey> {
    fn size_bytes(&self) -> ByteSize {
        self.memory_consumption
    }
}
