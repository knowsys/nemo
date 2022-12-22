use core::hash::Hash;
use std::{
    borrow::Borrow,
    collections::{hash_map::Entry, HashMap},
};

use bytesize::ByteSize;

use crate::{
    error::Error,
    meta::{
        logging::{log_empty_trie, log_executing_plan, log_save_trie_perm, log_save_trie_temp},
        TimedCode,
    },
    physical::{
        dictionary::PrefixedStringDictionary,
        tabular::{
            operations::{
                materialize::{materialize, materialize_subset},
                TrieScanJoin, TrieScanMinus, TrieScanProject, TrieScanSelectEqual,
                TrieScanSelectValue, TrieScanUnion,
            },
            table_types::trie::{Trie, TrieScanGeneric},
            traits::{table::Table, table_schema::TableSchema, triescan::TrieScanEnum},
        },
    },
};

use super::{
    execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult},
    ByteSized, ExecutionPlan,
};

/// Type which represents table id.
pub type TableId = usize;
/// Traits which have to be satisfied by a TableKey type.
pub trait TableKeyType: Eq + Hash + Clone {}

/// Struct that contains useful information about a trie
/// as well as the actual owner of the trie.
#[derive(Debug)]
struct TableInfo<TableKey: TableKeyType> {
    /// The trie.
    pub trie: Trie,
    /// Associated key.
    pub key: TableKey,
    /// The schema of the table
    pub schema: TableSchema,
}

impl<TableKey: TableKeyType> TableInfo<TableKey> {
    /// Create new [`TableInfo`].
    pub fn new(trie: Trie, key: TableKey, schema: TableSchema) -> Self {
        Self { trie, key, schema }
    }
}

/// Struct that contains useful information about a trie
/// that is only available temporarily.
#[derive(Debug)]
struct TempTableInfo {
    /// The trie.
    pub trie: Trie,
    /// The schema of the table
    pub schema: TableSchema,
}

impl TempTableInfo {
    /// Create new [`TempTableInfo`].
    pub fn new(trie: Trie, schema: TableSchema) -> Self {
        Self { trie, schema }
    }
}

/// Represents a collection of tables
#[derive(Debug)]
pub struct DatabaseInstance<TableKey: TableKeyType> {
    /// Structure which owns all the tries; accessed through Id.
    id_to_table: HashMap<TableId, TableInfo<TableKey>>,
    /// Alternative access scheme through a TableKey.
    key_to_tableid: HashMap<TableKey, TableId>,

    /// Dictionary which stores the strings associates with abstract constants
    dict_constants: PrefixedStringDictionary,

    /// The lowest unused TableId.
    /// Will be incremented for each new table and will never be reused.
    current_id: usize,
}

impl<TableKey: TableKeyType> DatabaseInstance<TableKey> {
    /// Create new [`DatabaseInstance`]
    pub fn new(dict_constants: PrefixedStringDictionary) -> Self {
        Self {
            id_to_table: HashMap::new(),
            key_to_tableid: HashMap::new(),
            dict_constants,
            current_id: 0,
        }
    }

    /// Return whether a given [`TableKey`] is associated with a table.
    pub fn table_exists(&self, key: &TableKey) -> bool {
        self.key_to_tableid.contains_key(key)
    }

    /// Return the current number of tables.
    pub fn num_tables(&self) -> usize {
        self.id_to_table.len()
    }

    /// Return the id of a table given a key.
    /// Panics if the key does not exist.
    pub fn get_id(&self, key: &TableKey) -> TableId {
        self.key_to_tableid.get(key).copied().unwrap()
    }

    /// Return the key of a table given its id.
    /// Panics if the id does not exist.
    pub fn get_key(&self, id: TableId) -> TableKey {
        self.id_to_table.get(&id).unwrap().key.clone()
    }

    /// Given a id return a reference to the corresponding trie.
    /// Panics if the id does not exist.
    pub fn get_by_id(&self, id: TableId) -> &Trie {
        self.id_to_table.get(&id).map(|t| &t.trie).unwrap()
    }

    /// Given a id return a mutable reference to the corresponding trie.
    /// Panics if the id does not exist.
    pub fn get_by_id_mut(&mut self, id: TableId) -> &mut Trie {
        self.id_to_table.get_mut(&id).map(|t| &mut t.trie).unwrap()
    }

    /// Given a [`TableKey`] return a reference to the corresponding trie.
    /// Panics if the key does not exist.
    pub fn get_by_key(&self, key: &TableKey) -> &Trie {
        let id = self.get_id(key);
        self.id_to_table.get(&id).map(|t| &t.trie).unwrap()
    }

    /// Given a [`TableKey`] return a mutable reference to the corresponding trie.
    /// Panics if the key does not exist.
    pub fn get_by_key_mut(&mut self, key: &TableKey) -> &mut Trie {
        let id = self.get_id(key);
        self.id_to_table.get_mut(&id).map(|t| &mut t.trie).unwrap()
    }

    /// Add the given trie to the instance under the given table key.
    /// Panics if the key is already used.
    pub fn add(&mut self, key: TableKey, trie: Trie, schema: TableSchema) -> TableId {
        let used_id = self.current_id;

        let insert_result = self
            .id_to_table
            .insert(self.current_id, TableInfo::new(trie, key.clone(), schema));

        if insert_result.is_some() {
            panic!("A table key should not be used twice");
        }

        self.key_to_tableid.insert(key, self.current_id);

        self.current_id += 1;
        used_id
    }

    /// Replace a trie (searched by key) with another.
    /// Will add a new trie if the key does not exist.
    /// Panics if the key is not already used.
    pub fn update_by_key(&mut self, key: TableKey, trie: Trie) {
        let id = self.get_id(&key);
        self.id_to_table.get_mut(&id).unwrap().trie = trie;
    }

    /// Replace a trie (searched by id) with another
    /// Will add a new trie if the id does not exist
    /// Returns true if the id existed
    pub fn update_by_id(&mut self, id: TableId, trie: Trie) {
        self.id_to_table.get_mut(&id).unwrap().trie = trie;
    }

    /// Delete a trie given its key.
    /// Panics if the table does not exist.
    pub fn delete_by_key(&mut self, key: &TableKey) {
        if let Entry::Occupied(key_entry) = self.key_to_tableid.entry(key.clone()) {
            let id = *key_entry.get();
            key_entry.remove();

            if let Entry::Occupied(id_entry) = self.id_to_table.entry(id) {
                id_entry.remove();
            } else {
                // Both HashMap should contain the table
                unreachable!()
            }
        } else {
            panic!("Table to be deleted should exist.");
        }
    }

    /// Delete a trie given its id.
    /// Panics if the table does not exist.
    pub fn delete_by_id(&mut self, id: TableId) {
        if let Entry::Occupied(entry) = self.id_to_table.entry(id) {
            self.key_to_tableid.remove(&entry.get().key);
            entry.remove();
        } else {
            panic!("Table to be deleted should exist.");
        }
    }

    /// Returns a mutable reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants_mut(&mut self) -> &mut PrefixedStringDictionary {
        &mut self.dict_constants
    }

    /// Returns a reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants(&self) -> &PrefixedStringDictionary {
        &self.dict_constants
    }

    /// Executes a given [`ExecutionPlan`].
    /// Returns the [`TableKey`]s of all new non-empty tables.
    /// This may fail if certain operations are performed on tries with incompatible types
    /// or if the plan references tries that do not exist.
    pub fn execute_plan(&mut self, plan: &ExecutionPlan<TableKey>) -> Result<Vec<TableKey>, Error> {
        let mut new_tables = Vec::<TableKey>::new();
        let mut temp_tries = HashMap::<TableId, Option<TempTableInfo>>::new();

        for tree in &plan.trees {
            let timed_string = format!("Reasoning/Execution/{}", tree.name());
            TimedCode::instance().sub(&timed_string).start();
            log_executing_plan(tree);

            // TODO: Properly determine this
            let schema = TableSchema::new();

            // Calculate the new trie
            let new_trie_opt = if let Some(root) = tree.root() {
                let iter_opt = self.get_iterator_node(root, &temp_tries)?;

                if let Some(mut iter) = iter_opt {
                    if let ExecutionResult::TempSubset(_, picked_columns) = tree.result() {
                        materialize_subset(&mut iter, picked_columns.clone())
                    } else {
                        materialize(&mut iter)
                    }
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(new_trie) = new_trie_opt {
                if new_trie.row_num() == 0 {
                    panic!("Empty trie that is not None");
                }

                // Add new trie to the appropriate place
                match tree.result() {
                    ExecutionResult::Temp(id) | ExecutionResult::TempSubset(id, _) => {
                        log_save_trie_temp(&new_trie);
                        temp_tries.insert(*id, Some(TempTableInfo::new(new_trie, schema)));
                    }
                    ExecutionResult::Save(key) => {
                        log_save_trie_perm(&new_trie);
                        self.add(key.clone(), new_trie, schema);
                        new_tables.push(key.clone());
                    }
                }
            } else {
                match tree.result() {
                    ExecutionResult::Temp(id) | ExecutionResult::TempSubset(id, _) => {
                        temp_tries.insert(*id, None);
                    }
                    _ => {}
                }

                log_empty_trie();
            }

            TimedCode::instance().sub(&timed_string).stop();
        }

        Ok(new_tables)
    }

    /// Given a Node in the execution tree returns the trie iterator
    /// that if materialized will turn into the resulting trie of the represented computation
    fn get_iterator_node<'a>(
        &'a self,
        node: ExecutionNodeRef<TableKey>,
        temp_tries: &'a HashMap<TableId, Option<TempTableInfo>>,
    ) -> Result<Option<TrieScanEnum>, Error> {
        if let Some(self_rc) = node.0.upgrade() {
            let node_ref = &*self_rc.as_ref().borrow();
            let node_operation = node_ref.borrow();

            return match node_operation {
                ExecutionNode::FetchTemp(id) => {
                    if let Some(info_opt) = temp_tries.get(id) {
                        if let Some(info) = info_opt {
                            Ok(Some(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(
                                &info.trie,
                            ))))
                        } else {
                            // Referenced trie is empty
                            Ok(None)
                        }
                    } else {
                        // References trie does not exist
                        Err(Error::InvalidExecutionPlan)
                    }
                }
                ExecutionNode::FetchTable(key) => {
                    if !self.table_exists(key) {
                        return Ok(None);
                    }

                    let trie = self.get_by_key(key);
                    if trie.row_num() == 0 {
                        return Ok(None);
                    }

                    let interval_triescan = TrieScanGeneric::new(trie);
                    Ok(Some(TrieScanEnum::TrieScanGeneric(interval_triescan)))
                }
                ExecutionNode::Join(subtables, bindings) => {
                    let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
                    for subtable in subtables {
                        let subiterator_opt =
                            self.get_iterator_node(subtable.clone(), temp_tries)?;

                        if let Some(subiterator) = subiterator_opt {
                            subiterators.push(subiterator);
                        } else {
                            // If subtables contain an empty table, then the join is empty
                            return Ok(None);
                        }
                    }

                    // If it only contains one table, then we dont need the join
                    if subiterators.len() == 1 {
                        return Ok(Some(subiterators.remove(0)));
                    }

                    Ok(Some(TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
                        subiterators,
                        bindings,
                    ))))
                }
                ExecutionNode::Union(subtables) => {
                    let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
                    for subtable in subtables {
                        let subiterator_opt =
                            self.get_iterator_node(subtable.clone(), temp_tries)?;

                        if let Some(subiterator) = subiterator_opt {
                            subiterators.push(subiterator);
                        }
                    }

                    // The union of empty tables is empty
                    if subiterators.is_empty() {
                        return Ok(None);
                    }

                    // If it only contains one table, then we dont need the join
                    if subiterators.len() == 1 {
                        return Ok(Some(subiterators.remove(0)));
                    }

                    let union_scan = TrieScanUnion::new(subiterators);

                    Ok(Some(TrieScanEnum::TrieScanUnion(union_scan)))
                }
                ExecutionNode::Minus(subtable_left, subtable_right) => {
                    if let Some(left_scan) =
                        self.get_iterator_node(subtable_left.clone(), temp_tries)?
                    {
                        if let Some(right_scan) =
                            self.get_iterator_node(subtable_right.clone(), temp_tries)?
                        {
                            Ok(Some(TrieScanEnum::TrieScanMinus(TrieScanMinus::new(
                                left_scan, right_scan,
                            ))))
                        } else {
                            Ok(Some(left_scan))
                        }
                    } else {
                        Ok(None)
                    }
                }
                ExecutionNode::Project(subnode, sorting) => {
                    if let Some(subnode_rc) = subnode.0.upgrade() {
                        let subnode_ref = &*subnode_rc.as_ref().borrow();
                        let subnode_operation = subnode_ref.borrow();

                        let trie = match subnode_operation {
                            ExecutionNode::FetchTable(key) => self.get_by_key(key),
                            ExecutionNode::FetchTemp(id) => {
                                if let Some(temp_trie_info) = temp_tries
                                    .get(id)
                                    .expect("Referenced temporary table should exist.")
                                {
                                    &temp_trie_info.trie
                                } else {
                                    return Ok(None);
                                }
                            }
                            _ => {
                                panic!("Project node has to have a Fetch node as its child.");
                            }
                        };

                        let project_scan = TrieScanProject::new(trie, sorting.clone());

                        Ok(Some(TrieScanEnum::TrieScanProject(project_scan)))
                    } else {
                        panic!("Accessed non-existing tree node.");
                    }
                }
                ExecutionNode::SelectValue(subtable, assignments) => {
                    let subiterator_opt = self.get_iterator_node(subtable.clone(), temp_tries)?;

                    if let Some(subiterator) = subiterator_opt {
                        let select_scan = TrieScanSelectValue::new(subiterator, assignments);
                        Ok(Some(TrieScanEnum::TrieScanSelectValue(select_scan)))
                    } else {
                        Ok(None)
                    }
                }
                ExecutionNode::SelectEqual(subtable, classes) => {
                    let subiterator_opt = self.get_iterator_node(subtable.clone(), temp_tries)?;

                    if let Some(subiterator) = subiterator_opt {
                        let select_scan = TrieScanSelectEqual::new(subiterator, classes);
                        Ok(Some(TrieScanEnum::TrieScanSelectEqual(select_scan)))
                    } else {
                        Ok(None)
                    }
                }
            };
        } else {
            unreachable!("We never delete nodes from the plan");
        };
    }
}

impl<TableKey: TableKeyType> ByteSized for DatabaseInstance<TableKey> {
    fn size_bytes(&self) -> ByteSize {
        self.id_to_table
            .iter()
            .fold(ByteSize(0), |acc, (_, info)| acc + info.trie.size_bytes())
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        datatypes::DataTypeName,
        dictionary::PrefixedStringDictionary,
        management::ByteSized,
        tabular::{
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
        util::make_column_with_intervals_t,
    };

    use super::{DatabaseInstance, TableKeyType};

    type StringKeyType = String;
    impl TableKeyType for StringKeyType {}

    #[test]
    fn basic_add_delete() {
        let column_a = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_b = make_column_with_intervals_t(&[1, 2, 3, 4, 5, 6], &[0]);

        let trie_a = Trie::new(vec![column_a]);
        let trie_b = Trie::new(vec![column_b]);

        let mut instance =
            DatabaseInstance::<StringKeyType>::new(PrefixedStringDictionary::default());

        let mut schema_a = TableSchema::new();
        schema_a.add_entry(DataTypeName::U64, false, false);

        assert_eq!(instance.add(String::from("A"), trie_a.clone(), schema_a), 0);
        assert_eq!(instance.get_key(0), String::from("A"));
        assert_eq!(instance.get_id(&String::from("A")), 0);
        // TODO: Look into catch_unwind and PrefixedStringDictionary
        // assert!(std::panic::catch_unwind(|| instance.get_key(1)).is_err());
        // assert!(std::panic::catch_unwind(|| instance.get_id(&String::from("C"))).is_err());
        assert_eq!(instance.get_by_id(0).row_num(), 3);

        let last_size = instance.size_bytes();

        let mut schema_b = TableSchema::new();
        schema_b.add_entry(DataTypeName::U64, false, false);

        assert_eq!(instance.add(String::from("B"), trie_b, schema_b), 1);
        assert!(instance.size_bytes() > last_size);
        assert_eq!(instance.get_by_key(&String::from("B")).row_num(), 6);

        let last_size = instance.size_bytes();

        instance.update_by_key(String::from("B"), trie_a);
        assert_eq!(instance.get_by_key(&String::from("B")).row_num(), 3);
        assert!(instance.size_bytes() < last_size);

        let last_size = instance.size_bytes();

        instance.delete_by_key(&String::from("A"));
        // // assert!(std::panic::catch_unwind(|| instance.get_id(&String::from("A"))).is_err());
        assert!(instance.size_bytes() < last_size);
    }
}
