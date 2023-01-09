use core::hash::Hash;
use std::collections::{hash_map::Entry, HashMap};

use bytesize::ByteSize;

use crate::{
    error::Error,
    meta::{
        logging::{
            log_empty_trie, log_execution_title, log_execution_tree, log_save_trie_perm,
            log_save_trie_temp,
        },
        TimedCode,
    },
    physical::{
        dictionary::PrefixedStringDictionary,
        tabular::{
            operations::{
                materialize::materialize, TrieScanJoin, TrieScanMinus, TrieScanProject,
                TrieScanSelectEqual, TrieScanSelectValue, TrieScanUnion,
            },
            table_types::trie::{Trie, TrieScanGeneric},
            traits::{table::Table, table_schema::TableSchema, triescan::TrieScanEnum},
        },
    },
};

use super::{
    execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult},
    type_analysis::{TypeTree, TypeTreeNode},
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
    /// TODO: Use this
    #[allow(dead_code)]
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
pub(super) struct TempTableInfo {
    /// The trie.
    pub trie: Trie,
}

impl TempTableInfo {
    /// Create new [`TempTableInfo`].
    pub fn new(trie: Trie) -> Self {
        Self { trie }
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
    pub fn get_by_key<'a>(&'a self, key: &TableKey) -> &'a Trie {
        let id = self.get_id(key);
        self.id_to_table.get(&id).map(|i| &i.trie).unwrap()
    }

    /// Given a [`TableKey`] return a mutable reference to the corresponding trie.
    /// Panics if the key does not exist.
    pub fn get_by_key_mut<'a>(&'a mut self, key: &TableKey) -> &'a mut Trie {
        let id = self.get_id(key);
        self.id_to_table.get_mut(&id).map(|t| &mut t.trie).unwrap()
    }

    /// Return the schema of a table identified by the given [`TableKey`].
    /// /// Panics if the key does not exist.
    pub fn get_schema<'a>(&'a self, key: &TableKey) -> &'a TableSchema {
        let id = self.get_id(key);
        self.id_to_table.get(&id).map(|i| &i.schema).unwrap()
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
        let mut temp_types = HashMap::<TableId, TableSchema>::new();

        for tree in &plan.trees {
            let timed_string = format!("Reasoning/Execution/{}", tree.name());
            TimedCode::instance().sub(&timed_string).start();
            log_execution_title(tree);

            let type_tree = TypeTree::from_execution_tree(&self, &temp_types, tree)?;
            let schema = type_tree.schema.clone();
            if let ExecutionResult::Temp(id) = tree.result() {
                temp_types.insert(*id, schema.clone());
            }

            // Calculate the new trie
            let new_trie_opt = if let Some(root) = tree.root() {
                log_execution_tree(&self.get_iterator_string(root.clone(), &temp_tries));

                let iter_opt = self.get_iterator_node(root, &type_tree, &temp_tries)?;

                if let Some(mut iter) = iter_opt {
                    materialize(&mut iter)
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
                    ExecutionResult::Temp(id) => {
                        log_save_trie_temp(&new_trie);
                        temp_tries.insert(*id, Some(TempTableInfo::new(new_trie)));
                    }
                    ExecutionResult::Save(key) => {
                        log_save_trie_perm(&new_trie);
                        self.add(key.clone(), new_trie, schema);
                        new_tables.push(key.clone());
                    }
                }
            } else {
                if let ExecutionResult::Temp(id) = tree.result() {
                    temp_tries.insert(*id, None);
                }

                log_empty_trie();
            }

            TimedCode::instance().sub(&timed_string).stop();
        }

        Ok(new_tables)
    }

    /// Given a node in the execution tree returns the trie iterator
    /// that if materialized will turn into the resulting trie of the represented computation
    fn get_iterator_node<'a>(
        &'a self,
        execution_node: ExecutionNodeRef<TableKey>,
        type_node: &'a TypeTreeNode,
        temp_tries: &'a HashMap<TableId, Option<TempTableInfo>>,
    ) -> Result<Option<TrieScanEnum>, Error> {
        if let Some(node_rc) = execution_node.0.upgrade() {
            let node_ref = &*node_rc.as_ref().borrow();

            return match node_ref {
                ExecutionNode::FetchTemp(id) => {
                    if let Some(Some(info)) = temp_tries.get(id) {
                        Ok(Some(TrieScanEnum::TrieScanGeneric(
                            TrieScanGeneric::new_cast(
                                &info.trie,
                                type_node.schema.get_column_types(),
                            ),
                        )))
                    } else {
                        // Referenced trie is empty or does not exist
                        // The latter is not an error, since it could happen that the referenced trie
                        // would have been produced by this plan but was just empty
                        Ok(None)
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
                    let schema = type_node.schema.get_column_types();

                    let interval_triescan = TrieScanGeneric::new_cast(trie, schema);
                    Ok(Some(TrieScanEnum::TrieScanGeneric(interval_triescan)))
                }
                ExecutionNode::Join(subtables, bindings) => {
                    let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
                    for (table_index, subtable) in subtables.iter().enumerate() {
                        let subiterator_opt = self.get_iterator_node(
                            subtable.clone(),
                            &type_node.subnodes[table_index],
                            temp_tries,
                        )?;

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
                    for (table_index, subtable) in subtables.iter().enumerate() {
                        let subiterator_opt = self.get_iterator_node(
                            subtable.clone(),
                            &type_node.subnodes[table_index],
                            temp_tries,
                        )?;

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
                    if let Some(left_scan) = self.get_iterator_node(
                        subtable_left.clone(),
                        &type_node.subnodes[0],
                        temp_tries,
                    )? {
                        if let Some(right_scan) = self.get_iterator_node(
                            subtable_right.clone(),
                            &type_node.subnodes[1],
                            temp_tries,
                        )? {
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

                        let trie = match subnode_ref {
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
                    let subiterator_opt = self.get_iterator_node(
                        subtable.clone(),
                        &type_node.subnodes[0],
                        temp_tries,
                    )?;

                    if let Some(subiterator) = subiterator_opt {
                        let select_scan = TrieScanSelectValue::new(subiterator, assignments);
                        Ok(Some(TrieScanEnum::TrieScanSelectValue(select_scan)))
                    } else {
                        Ok(None)
                    }
                }
                ExecutionNode::SelectEqual(subtable, classes) => {
                    let subiterator_opt = self.get_iterator_node(
                        subtable.clone(),
                        &type_node.subnodes[0],
                        temp_tries,
                    )?;

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

    fn get_iterator_string_sub<'a>(
        &'a self,
        operation: &str,
        subnodes: &Vec<&ExecutionNodeRef<TableKey>>,
        temp_tries: &'a HashMap<TableId, Option<TempTableInfo>>,
    ) -> String {
        let mut result = String::from(operation);
        result += "(";

        for (index, &sub) in subnodes.iter().enumerate() {
            result += &self.get_iterator_string(sub.clone(), temp_tries);

            if index < subnodes.len() - 1 {
                result += ", ";
            }
        }

        result += ")";

        result
    }

    /// Return a string which represents a execution tree (given its root).
    fn get_iterator_string<'a>(
        &'a self,
        node: ExecutionNodeRef<TableKey>,
        temp_tries: &'a HashMap<TableId, Option<TempTableInfo>>,
    ) -> String {
        if let Some(node_rc) = node.0.upgrade() {
            let node_ref = &*node_rc.as_ref().borrow();

            match node_ref {
                ExecutionNode::FetchTemp(id) => {
                    if let Some(Some(info)) = temp_tries.get(id) {
                        info.trie.row_num().to_string()
                    } else {
                        // Referenced trie is empty or does not exist
                        String::from("")
                    }
                }
                ExecutionNode::FetchTable(key) => {
                    if !self.table_exists(key) {
                        return String::from("");
                    }

                    let trie = self.get_by_key(key);

                    trie.row_num().to_string()
                }
                ExecutionNode::Join(sub, _) => {
                    self.get_iterator_string_sub("Join", &sub.iter().collect(), temp_tries)
                }
                ExecutionNode::Union(sub) => {
                    self.get_iterator_string_sub("Union", &sub.iter().collect(), temp_tries)
                }
                ExecutionNode::Minus(left, right) => {
                    self.get_iterator_string_sub("Minus", &vec![left, right], temp_tries)
                }
                ExecutionNode::Project(subnode, _) => {
                    self.get_iterator_string_sub("Project", &vec![subnode], temp_tries)
                }
                ExecutionNode::SelectValue(sub, _) => {
                    self.get_iterator_string_sub("SelectValue", &vec![sub], temp_tries)
                }
                ExecutionNode::SelectEqual(sub, _) => {
                    self.get_iterator_string_sub("SelectEqual", &vec![sub], temp_tries)
                }
            }
        } else {
            unreachable!("We never delete nodes from the plan")
        }
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
        columnar::{column_types::interval::ColumnWithIntervalsT, traits::column::Column},
        datatypes::{DataTypeName, DataValueT},
        dictionary::PrefixedStringDictionary,
        management::{
            execution_plan::{ExecutionResult, ExecutionTree},
            ByteSized, ExecutionPlan,
        },
        tabular::{
            table_types::trie::Trie,
            traits::{
                table::Table,
                table_schema::{TableSchema, TableSchemaEntry},
            },
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

    fn schema_entry(type_name: DataTypeName) -> TableSchemaEntry {
        TableSchemaEntry {
            type_name,
            dict: false,
            nullable: false,
        }
    }

    fn test_casting_execution_plan() -> ExecutionPlan<StringKeyType> {
        // ExecutionPlan:
        // Union
        //  -> Minus
        //      -> Trie_x [U64, U32, U64]
        //      -> Trie_y [U32, U64, U32]
        //  -> Join [[0, 1], [1, 2]]
        //      -> Union
        //          -> Trie_a [U32, U32]
        //          -> Trie_b [U32, U64]
        //      -> Union
        //          -> Trie_b [U32, U64]
        //          -> Trie_c [U32, U32]

        let mut execution_tree = ExecutionTree::<StringKeyType>::new(
            String::from("Test"),
            ExecutionResult::Save(String::from("TableResult")),
        );

        let node_load_a = execution_tree.fetch_table(String::from("TableA"));
        let node_load_b_1 = execution_tree.fetch_table(String::from("TableB"));
        let node_load_b_2 = execution_tree.fetch_table(String::from("TableB"));
        let node_load_c = execution_tree.fetch_table(String::from("TableC"));
        let node_load_x = execution_tree.fetch_table(String::from("TableX"));
        let node_load_y = execution_tree.fetch_table(String::from("TableY"));

        let node_minus = execution_tree.minus(node_load_x, node_load_y);

        let node_left_union = execution_tree.union(vec![node_load_a, node_load_b_1]);
        let node_right_union = execution_tree.union(vec![node_load_b_2, node_load_c]);

        let node_join = execution_tree.join(
            vec![node_left_union, node_right_union],
            vec![vec![0, 1], vec![1, 2]],
        );

        let node_root = execution_tree.union(vec![node_join, node_minus]);
        execution_tree.set_root(node_root);

        let mut execution_plan = ExecutionPlan::<StringKeyType>::new();
        execution_plan.push(execution_tree);

        execution_plan
    }

    #[test]
    fn test_casting() {
        let trie_a = Trie::from_rows(vec![vec![DataValueT::U32(1), DataValueT::U32(2)]]);
        let trie_b = Trie::from_rows(vec![
            vec![DataValueT::U32(2), DataValueT::U64(1 << 35)],
            vec![DataValueT::U32(3), DataValueT::U64(2)],
        ]);
        let trie_c = Trie::from_rows(vec![vec![DataValueT::U32(2), DataValueT::U64(4)]]);
        let trie_x = Trie::from_rows(vec![
            vec![DataValueT::U64(1), DataValueT::U32(2), DataValueT::U64(4)],
            vec![DataValueT::U64(3), DataValueT::U32(2), DataValueT::U64(4)],
            vec![
                DataValueT::U64(1 << 36),
                DataValueT::U32(6),
                DataValueT::U64(12),
            ],
        ]);
        let trie_y = Trie::from_rows(vec![
            vec![DataValueT::U32(1), DataValueT::U64(2), DataValueT::U32(4)],
            vec![
                DataValueT::U32(2),
                DataValueT::U64(1 << 37),
                DataValueT::U32(7),
            ],
        ]);

        let schema_a = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_b = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_c = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_x = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_y = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);

        let mut instance =
            DatabaseInstance::<StringKeyType>::new(PrefixedStringDictionary::default());
        instance.add(String::from("TableA"), trie_a, schema_a);
        instance.add(String::from("TableB"), trie_b, schema_b);
        instance.add(String::from("TableC"), trie_c, schema_c);
        instance.add(String::from("TableX"), trie_x, schema_x);
        instance.add(String::from("TableY"), trie_y, schema_y);

        let plan = test_casting_execution_plan();
        let result = instance.execute_plan(&plan);
        assert!(result.is_ok());

        let result_trie = instance.get_by_key(&String::from("TableResult"));

        let result_col_first = if let ColumnWithIntervalsT::U64(col) = result_trie.get_column(0) {
            col
        } else {
            assert!(false);
            return;
        };
        let result_col_second = if let ColumnWithIntervalsT::U32(col) = result_trie.get_column(1) {
            col
        } else {
            assert!(false);
            return;
        };
        let result_col_third = if let ColumnWithIntervalsT::U64(col) = result_trie.get_column(2) {
            col
        } else {
            assert!(false);
            return;
        };

        assert_eq!(
            result_col_first
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 3, 1 << 36]
        );
        assert_eq!(
            result_col_first
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            result_col_second
                .get_data_column()
                .iter()
                .collect::<Vec<u32>>(),
            vec![2, 2, 6]
        );
        assert_eq!(
            result_col_second
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 2]
        );

        assert_eq!(
            result_col_third
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![4, 1 << 35, 4, 1 << 35, 12],
        );
        assert_eq!(
            result_col_third
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 4]
        );
    }
}
