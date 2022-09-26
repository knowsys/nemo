//! Managing of tables

use super::execution_plan::{ExecutionNode, ExecutionOperation, ExecutionResult};
use super::model::{DataSource, Identifier};
use super::ExecutionSeries;

use crate::physical::datatypes::DataTypeName;
use crate::physical::tables::{
    materialize, IntervalTrieScan, Table, Trie, TrieDifference, TrieJoin, TrieProject, TrieScan,
    TrieScanEnum, TrieSchema, TrieSchemaEntry, TrieSelectEqual, TrieSelectValue, TrieUnion,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Range;
use superslice::*;

/// Type which represents a variable ordering
pub type ColumnOrder = Vec<usize>;

/// Type which represents table ids
pub type TableId = usize;

/// Information which identfies a table
/// Serves as the key to hashmap
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableKey {
    /// ID of the predicate
    pub predicate_id: Identifier,
    /// Range of step this table covers
    /// Refers to the normalized ranges
    pub step_range: Range<usize>,
}

/// Represents the storage status of a table
#[derive(Debug)]
pub enum TableStatus {
    /// Table is not materialized and has to be generated from another table through reordering
    Derived,
    /// Table has to be read from disk
    OnDisk(DataSource),
    /// Table is present in main memory in materialized form
    InMemory(Trie),
    /// Table can be removed
    Deleted,
}

/// Represents the table management strategy
#[derive(Debug, Copy, Clone)]
pub enum TableManagerStrategy {
    /// No restrictions
    Unlimited,
    /// Memory is restricted to a certain amount
    Limited(u64),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct TablePriority {
    table_id: TableId,
    priority: u64,
}

impl Ord for TablePriority {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

impl PartialOrd for TablePriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Contains all the relevant information to a table stored in the [`TableManager`]
#[derive(Debug)]
pub struct TableInfo {
    /// Storage location of the table
    pub status: TableStatus,

    // Redundant information, TODO: Get rid of this?
    column_order: ColumnOrder,
    key: TableKey,

    priority: u64,
    #[allow(dead_code)]
    space: u64,
}

/// Object for keeping track of all the tables
#[derive(Debug)]
pub struct TableManager {
    #[allow(dead_code)]
    strategy: TableManagerStrategy,
    tables: Vec<TableInfo>,
    entries: HashMap<TableKey, Vec<(ColumnOrder, TableId)>>,

    predicate_to_steps: HashMap<Identifier, Vec<usize>>,
    priority_heap: BinaryHeap<TablePriority>,
    current_id: TableId,

    #[allow(dead_code)]
    space_consumed: u64,
}

/// Encodes the things that can go wrong while asking for a table
#[derive(Debug, Copy, Clone)]
pub enum GetTableError {
    /// Manager does not contain a table with that predicate and step range
    NoTable,
    /// Manager contains the table but in a wrong column order
    WrongOrder,
}

impl TableManager {
    /// Create new [`TableManager`]
    pub fn new(strategy: TableManagerStrategy) -> Self {
        Self {
            strategy,
            tables: Vec::new(),
            entries: HashMap::new(),
            priority_heap: BinaryHeap::new(),
            predicate_to_steps: HashMap::new(),
            space_consumed: 0,
            current_id: 0,
        }
    }

    fn normalize_range(&self, predicate: Identifier, range: &Range<usize>) -> Range<usize> {
        // debug_assert!(range.end > 0);

        let step_vec = self
            .predicate_to_steps
            .get(&predicate)
            .expect("Just don't put unknown predicates here");

        let left = step_vec.lower_bound(&range.start);
        let right = step_vec.lower_bound(&range.end);

        left..right
    }

    /// Given a range, returns all the block-numbers available for a predicate
    pub fn get_blocks_within_range(&self, predicate: Identifier, range: &Range<usize>) -> &[usize] {
        match self.predicate_to_steps.get(&predicate) {
            Some(vec) => &vec[self.normalize_range(predicate, range)],
            None => &[],
        }
    }

    /// Iterator for all the different variable orders a table is stored in
    pub fn get_tables_by_key(
        &self,
        predicate_id: Identifier,
        step_range: &Range<usize>,
    ) -> impl Iterator<Item = &(ColumnOrder, TableId)> {
        self.entries
            .get(&TableKey {
                predicate_id,
                step_range: self.normalize_range(predicate_id, step_range),
            })
            // TODO: Replace this unwrap with something that returns an empty iterator?
            .unwrap()
            .iter()
    }

    /// Searches for table given predicate, step range and variable order
    pub fn get_table(
        &self,
        predicate_id: Identifier,
        step_range: &Range<usize>,
        column_order: &ColumnOrder,
    ) -> Result<TableId, GetTableError> {
        let orders = self
            .entries
            .get(&TableKey {
                predicate_id,
                step_range: self.normalize_range(predicate_id, step_range),
            })
            .ok_or(GetTableError::NoTable)?;

        for (order, id) in orders {
            if TableManager::orders_equal(column_order, order) {
                return Ok(*id);
            }
        }

        Err(GetTableError::WrongOrder)
    }

    fn translate_order(order_from: &ColumnOrder, order_to: &ColumnOrder) -> Vec<usize> {
        let mut result = Vec::new();

        for &to_variable in order_to {
            result.push(order_from.iter().position(|&x| x == to_variable).unwrap());
        }

        result
    }

    fn find_materialized_table(
        &self,
        key: &TableKey,
        id: TableId,
    ) -> Option<(&ColumnOrder, &Trie)> {
        for (base_order, base_id) in self.entries.get(key).unwrap() {
            if *base_id == id {
                continue;
            }

            if let TableStatus::InMemory(base_trie) = &self.tables[*base_id].status {
                return Some((base_order, base_trie));
            }
        }

        None
    }

    fn materialize_required_tables(&mut self, tables: &HashSet<TableId>) {
        for &table_id in tables {
            let info = &self.tables[table_id];
            if let TableStatus::Derived = info.status {
                let (base_order, base_trie) =
                    self.find_materialized_table(&info.key, table_id).unwrap();

                let project_iter = TrieProject::new(
                    base_trie,
                    TableManager::translate_order(base_order, &info.column_order),
                );

                let reordered_trie = materialize(&mut TrieScanEnum::TrieProject(project_iter));

                self.tables[table_id].status = TableStatus::InMemory(reordered_trie);
            }
        }
    }

    /// This was for doing the memory management, needs some thinking
    /*
    fn update_status(&mut self, id: TableId) {
        let table_info = &mut self.tables[id];

        if table_info.preferred_status == TableStatus::InMemory
            && table_info.current_status != TableStatus::InMemory
        {
            let mut move_into_memory = true;

            if let TableManagerStrategy::Limited(limit) = self.strategy {
                // If we have limited space we have to check if space is available
                let estimated_space = match table_info.current_status {
                    TableStatus::Derived => self.estimate_space(table_info.plan.as_ref().unwrap()),
                    TableStatus::InMemory => unreachable!(),
                    TableStatus::OnDisk => 0, //TODO: How to do this
                    TableStatus::Deleted => unreachable!(),
                };

                self.priority_heap.push(TablePriority {
                    table_id: id,
                    priority: table_info.priority,
                });

                // TODO: Not clear what to do here
                if estimated_space > limit {
                    unimplemented!()
                }

                // Not enough space, hence we need to free low priority tables
                while self.space_consumed + estimated_space > limit && self.priority_heap.len() > 0
                {
                    let lowest_element = self.priority_heap.peek().unwrap();
                    if lowest_element.priority < table_info.priority {
                        move_into_memory = false;
                        break;
                    }

                    self.priority_heap.pop();

                    // This should free memory, right?
                    self.tables[lowest_element.table_id].table = None;

                    self.space_consumed -= self.tables[lowest_element.table_id].space;
                    self.tables[lowest_element.table_id].space = 0;
                }
            }

            if move_into_memory {
                match table_info.current_status {
                    TableStatus::Derived => {
                        let iterator = self.get_iterator(table_info.plan.as_ref().unwrap());
                        table_info.table = Some(materialize(&mut iterator));
                        table_info.space = 0; // TODO: Calculate this
                    }
                    // TODO: Implement this case
                    TableStatus::OnDisk => {}
                    _ => unreachable!(),
                }

                table_info.current_status = TableStatus::InMemory;
                self.space_comsumed += self.tables[id].space;
            }
        } else {
            unimplemented!();
        }
    } */

    fn add_table_helper(&mut self, key: TableKey, order: ColumnOrder, priority: u64) -> TableId {
        let new_id = self.current_id;
        self.current_id += 1;

        let table_list = self.entries.entry(key).or_insert_with(Vec::new);

        // If the table list contained only one table then this means it couldnt have been deleted until now
        // and therefore wasnt in the priority queue before, so we add it now
        if table_list.len() == 1 {
            let only_table_id = table_list[0].1;
            let only_table_priority = self.tables[only_table_id].priority;

            self.priority_heap.push(TablePriority {
                table_id: only_table_id,
                priority: only_table_priority,
            });
        }

        if !table_list.is_empty() {
            self.priority_heap.push(TablePriority {
                table_id: new_id,
                priority,
            });
        }

        table_list.push((order, new_id));

        new_id
    }

    /// Add a table that is stored on disk to the manager
    pub fn add_edb(
        &mut self,
        data_source: DataSource,
        predicate: Identifier,
        column_order: ColumnOrder,
        priority: u64,
    ) -> TableId {
        let table_list = self
            .predicate_to_steps
            .entry(predicate)
            .or_insert_with(Vec::new);
        table_list.push(0);

        let key = TableKey {
            predicate_id: predicate,
            step_range: (0..1),
        };

        self.tables.push(TableInfo {
            status: TableStatus::OnDisk(data_source),
            column_order: column_order.clone(),
            key: key.clone(),
            priority,
            space: 0, //TODO: How to do this
        });

        self.add_table_helper(key, column_order, priority)
    }

    /// Add a computed table to the manager
    // pub fn add_idb(
    //     &mut self,
    //     predicate: Identifier,
    //     absolute_step_range: Range<usize>,
    //     column_order: ColumnOrder,
    //     priority: u64,
    //     plan: ExecutionPlan,
    // ) -> Option<TableId> {
    //     let trie = self.execute_plan(plan)?;

    //     self.add_trie(predicate, absolute_step_range, column_order, priority, trie)
    // }

    /// Add trie to the table manager
    pub fn add_trie(
        &mut self,
        predicate: Identifier,
        absolute_step_range: Range<usize>,
        column_order: ColumnOrder,
        priority: u64,
        trie: Trie,
    ) -> Option<TableId> {
        if trie.row_num() == 0 {
            return None;
        }

        // Tables only covers a single step
        if absolute_step_range.end - absolute_step_range.start == 1 {
            let table_list = self
                .predicate_to_steps
                .entry(predicate)
                .or_insert_with(Vec::new);
            table_list.push(absolute_step_range.start);
        }

        let key = TableKey {
            predicate_id: predicate,
            step_range: self.normalize_range(predicate, &absolute_step_range),
        };
        self.tables.push(TableInfo {
            status: TableStatus::InMemory(trie),
            column_order: column_order.clone(),
            key: key.clone(),
            priority,
            space: 0, //TODO: How to do this
        });

        Some(self.add_table_helper(key, column_order, priority))
    }

    fn orders_equal(order_left: &ColumnOrder, order_right: &ColumnOrder) -> bool {
        if order_left.len() != order_right.len() {
            return false;
        }

        for index in 0..order_left.len() {
            if order_left[index] != order_right[index] {
                return false;
            }
        }

        true
    }

    /// Checks if the given table is empty
    pub fn table_is_empty(&self, table_id: TableId) -> bool {
        let table_info = &self.tables[table_id];

        match &table_info.status {
            TableStatus::InMemory(trie) => trie.row_num() == 0,
            TableStatus::Derived => false,
            TableStatus::OnDisk(_) => false, // TODO: Whats the best way to determine this?
            TableStatus::Deleted => true,
        }
    }

    /// Returns information about a table given its id
    pub fn get_info_mut(&mut self, table_id: TableId) -> &mut TableInfo {
        &mut self.tables[table_id]
    }

    /// Returns information about a table given its id
    pub fn get_info(&self, table_id: TableId) -> &TableInfo {
        &self.tables[table_id]
    }

    /// Estimates the amount of time executing a [`ExecutionSeries`] would take
    /// TODO: Returns just 0 for now
    pub fn estimate_runtime_costs(_series: &ExecutionSeries) -> u64 {
        0
    }

    /// Estimate the potential space the materialization of a [`ExecutionPlan`] would take
    pub fn estimate_space(plan: &ExecutionNode) -> u64 {
        match &plan.operation {
            // TODO:
            ExecutionOperation::Fetch(_, _, _) => 0,
            // TODO: This is tricky
            ExecutionOperation::Join(_subtables, _bindings) => 0,
            ExecutionOperation::Union(subtables) => {
                subtables.iter().map(Self::estimate_space).sum()
            }
            ExecutionOperation::Minus(left, _right) => Self::estimate_space(&left),
            ExecutionOperation::Project(_id, _sorting) => {
                // Should be easy to calculate
                0
            }
            ExecutionOperation::Temp(_) => 0,
            ExecutionOperation::SelectValue(subtable, _) => Self::estimate_space(subtable),
            ExecutionOperation::SelectEqual(subtable, _) => Self::estimate_space(subtable),
        }
    }

    /// Add table and mark its contents as derived
    pub fn add_derived(
        &mut self,
        predicate: Identifier,
        absolute_step_range: Range<usize>,
        column_order: ColumnOrder,
        priority: u64,
    ) -> TableId {
        let key = TableKey {
            predicate_id: predicate,
            step_range: self.normalize_range(predicate, &absolute_step_range),
        };

        self.tables.push(TableInfo {
            status: TableStatus::Derived,
            column_order: column_order.clone(),
            key: key.clone(),
            priority,
            space: 0, //TODO: How to do this
        });

        self.add_table_helper(key, column_order, priority)
    }

    /// Executes an [`ExecutionSeries`] and adds the resulting tables
    /// Returns true if a non-empty table was added, false otherwise
    pub fn execute_series(&mut self, series: ExecutionSeries) -> bool {
        let mut new_table = false;
        let mut temp_tries = HashMap::<TableId, Option<Trie>>::new();

        for plan in series.plans {
            let mut table_ids = HashSet::new();
            for leave_node in plan.leaves {
                if let ExecutionOperation::Fetch(predicate, absolute_step_range, column_order) =
                    &leave_node.operation
                {
                    match self.get_table(*predicate, absolute_step_range, column_order) {
                        Err(error) => match error {
                            GetTableError::NoTable => {}
                            GetTableError::WrongOrder => {
                                table_ids.insert(self.add_derived(
                                    *predicate,
                                    absolute_step_range.clone(),
                                    column_order.clone(),
                                    0,
                                ));
                            }
                        },

                        Ok(id) => {
                            table_ids.insert(id);
                        }
                    };
                } else {
                    unreachable!();
                }
            }

            // TODO: Materializig should check memory and so on...
            self.materialize_required_tables(&table_ids);

            let iter_option = self.get_iterator_node(&plan.root, &temp_tries);
            if let Some(mut iter) = iter_option {
                // TODO: Materializig should check memory and so on...
                let new_trie = materialize(&mut iter);
                match plan.result {
                    ExecutionResult::Temp(id) => {
                        if new_trie.row_num() > 0 {
                            temp_tries.insert(id, Some(new_trie));
                        } else {
                            temp_tries.insert(id, None);
                        }
                    }
                    ExecutionResult::Save(pred, range, order, priority) => {
                        if new_trie.row_num() > 0 {
                            new_table = true;

                            self.add_trie(pred, range, order, priority, new_trie);
                        }
                    }
                }
            }
        }

        new_table
    }

    /// Returns None if the TrieScan would be empty
    // TODO: Remove pub
    pub fn get_iterator_node<'a>(
        &'a self,
        node: &'a ExecutionNode,
        temp_tries: &'a HashMap<TableId, Option<Trie>>,
    ) -> Option<TrieScanEnum> {
        match &node.operation {
            ExecutionOperation::Temp(id) => Some(TrieScanEnum::IntervalTrieScan(
                IntervalTrieScan::new(temp_tries.get(id)?.as_ref()?),
            )),
            ExecutionOperation::Fetch(predicate, absolute_step_range, column_order) => {
                let table_info = &self.tables[self
                    .get_table(*predicate, absolute_step_range, column_order)
                    .ok()?];
                if let TableStatus::InMemory(trie) = &table_info.status {
                    let interval_trie_scan = IntervalTrieScan::new(trie);

                    Some(TrieScanEnum::IntervalTrieScan(interval_trie_scan))
                } else {
                    panic!("Base tables are supposed to be materialized");
                }
            }
            ExecutionOperation::Join(subtables, bindings) => {
                let subiterators: Vec<TrieScanEnum> = subtables
                    .iter()
                    .map(|s| self.get_iterator_node(s, temp_tries))
                    .filter(|s| s.is_some())
                    .flatten()
                    .collect();

                if subiterators.len() != subtables.len() {
                    return None;
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

                Some(TrieScanEnum::TrieJoin(TrieJoin::new(
                    subiterators,
                    bindings,
                    schema,
                )))
            }
            ExecutionOperation::Union(subtables) => {
                let subtables: Vec<TrieScanEnum> = subtables
                    .iter()
                    .map(|s| self.get_iterator_node(s, temp_tries))
                    .filter(|s| s.is_some())
                    .flatten()
                    .collect();

                if subtables.is_empty() {
                    return None;
                }

                let union_scan = TrieUnion::new(subtables);

                Some(TrieScanEnum::TrieUnion(union_scan))
            }
            ExecutionOperation::Minus(subtable_left, subtable_right) => {
                let left_scan = self.get_iterator_node(&subtable_left, temp_tries);
                let right_scan = self.get_iterator_node(&subtable_right, temp_tries);

                if let Some(left) = left_scan {
                    if let Some(right) = right_scan {
                        let difference_scan = TrieDifference::new(left, right);
                        Some(TrieScanEnum::TrieDifference(difference_scan))
                    } else {
                        Some(left)
                    }
                } else {
                    None
                }
            }
            ExecutionOperation::Project(id, sorting) => {
                let tmp_trie = temp_tries.get(id)?.as_ref()?;
                let project_scan = TrieProject::new(tmp_trie, sorting.clone());

                Some(TrieScanEnum::TrieProject(project_scan))
            }
            ExecutionOperation::SelectValue(subtable, assignments) => {
                let subiterator = self.get_iterator_node(subtable, temp_tries)?;
                let select_scan = TrieSelectValue::new(subiterator, assignments.clone());

                Some(TrieScanEnum::TrieSelectValue(select_scan))
            }
            ExecutionOperation::SelectEqual(subtable, classes) => {
                let subiterator = self.get_iterator_node(subtable, temp_tries)?;
                let select_scan = TrieSelectEqual::new(subiterator, classes.clone());

                Some(TrieScanEnum::TrieSelectEqual(select_scan))
            }
        }
    }
}

#[cfg(test)]
mod test {}
