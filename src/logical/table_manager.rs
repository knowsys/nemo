//! Managing of tables

use super::execution_plan::{ExecutionNode, ExecutionOperation, ExecutionResult};
use super::model::{DataSource, Identifier};
use super::ExecutionSeries;

use crate::io::csv::read;
use crate::meta::TimedCode;
use crate::physical::datatypes::DataTypeName;
use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};
use crate::physical::tabular::tables::Table;
use crate::physical::tabular::tries::{Trie, TrieSchema, TrieSchemaEntry};
use crate::physical::tabular::triescans::{
    materialize, TrieScan, TrieScanEnum, TrieScanGeneric, TrieScanJoin, TrieScanMinus,
    TrieScanProject, TrieScanSelectEqual, TrieScanSelectValue, TrieScanUnion,
};
use crate::physical::util::cover_interval;
use csv::ReaderBuilder;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
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
#[derive(Clone, Debug)]
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

    // The vector is assumed to be sorted, because of the order data is put in here
    predicate_to_steps: HashMap<Identifier, Vec<usize>>,
    // Ranges need to be sorted by their first entry when inserting
    predicate_to_ranges: HashMap<Identifier, Vec<Range<usize>>>,
    // What step the last big table has been computed in
    predicate_to_bigtable_step: HashMap<Identifier, usize>,

    priority_heap: BinaryHeap<TablePriority>,
    current_id: TableId,

    #[allow(dead_code)]
    space_consumed: u64,

    /// Dictionary containing all the relevant strings
    pub dictionary: PrefixedStringDictionary,
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
    pub fn new(strategy: TableManagerStrategy, dictionary: PrefixedStringDictionary) -> Self {
        Self {
            strategy,
            tables: Vec::new(),
            entries: HashMap::new(),
            priority_heap: BinaryHeap::new(),
            predicate_to_steps: HashMap::new(),
            predicate_to_ranges: HashMap::new(),
            predicate_to_bigtable_step: HashMap::new(),
            space_consumed: 0,
            current_id: 0,
            dictionary,
        }
    }

    fn normalize_range(&self, predicate: Identifier, range: &Range<usize>) -> Option<Range<usize>> {
        // debug_assert!(range.end > 0);

        let step_vec = self.predicate_to_steps.get(&predicate)?;

        let left = step_vec.lower_bound(&range.start);
        let right = step_vec.lower_bound(&range.end);

        Some(left..right)
    }

    /// Returns the step, in wich the last big table was computed
    pub fn get_bigtable_step(&self, predicate: &Identifier) -> usize {
        *self.predicate_to_bigtable_step.get(predicate).unwrap_or(&0)
    }

    /// Set the step number in which a big table for a given predicate has been computed
    pub fn set_bigtable_step(&mut self, predicate: &Identifier, step: usize) {
        self.predicate_to_bigtable_step.insert(*predicate, step);
    }

    /// Given a range, returns all the block-numbers available for a predicate
    pub fn get_blocks_within_range(
        &self,
        predicate: Identifier,
        range: &Range<usize>,
    ) -> Vec<Range<usize>> {
        let absolute_steps = match self.predicate_to_steps.get(&predicate) {
            Some(s) => s,
            None => return Vec::new(),
        };

        match self.predicate_to_ranges.get(&predicate) {
            Some(vec) => cover_interval(vec, &self.normalize_range(predicate, range).unwrap())
                .iter()
                .map(|r| absolute_steps[r.start]..(absolute_steps[r.end - 1] + 1))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Iterator for all the different variable orders a table is stored in
    pub fn get_tables_by_key(
        &self,
        predicate_id: Identifier,
        step_range: &Range<usize>,
    ) -> &[(ColumnOrder, TableId)] {
        if let Some(step_range) = self.normalize_range(predicate_id, step_range) {
            return self
                .entries
                .get(&TableKey {
                    predicate_id,
                    step_range,
                })
                .map_or(&[], |p| p);
        }

        &[]
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
                step_range: self
                    .normalize_range(predicate_id, step_range)
                    .ok_or(GetTableError::NoTable)?,
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

    fn materialize_on_disk(&mut self, source: &DataSource, order: &ColumnOrder) -> Trie {
        let (trie, name) = match source {
            DataSource::CsvFile(file) => {
                // Using fallback solution to treat eveything as string for now (storing as u64 internally)
                let datatypes: Vec<Option<DataTypeName>> = (0..order.len()).map(|_| None).collect();

                let mut reader = ReaderBuilder::new()
                    .delimiter(b',')
                    .has_headers(false)
                    .from_reader(File::open(file.as_path()).unwrap());

                let col_table = read(&datatypes, &mut reader, &mut self.dictionary).unwrap();

                let schema = TrieSchema::new(
                    (0..col_table.len())
                        .map(|i| TrieSchemaEntry {
                            label: i,
                            datatype: DataTypeName::U64,
                        })
                        .collect(),
                );

                let trie = Trie::from_cols(schema, col_table);
                (
                    trie,
                    file.file_name()
                        .expect("Filename should be valid")
                        .to_str()
                        .unwrap(),
                )
            }
            DataSource::RdfFile(_) => todo!(),
            DataSource::SparqlQuery(_) => todo!(),
        };

        log::info!(
            "Loaded table {} from disk in order {:?} ({} elements)",
            name,
            &order,
            trie.row_num()
        );

        trie
    }

    fn find_materialized_table(&self, key: &TableKey, id: TableId) -> Option<TableId> {
        for (_, base_id) in self.entries.get(key).unwrap() {
            if *base_id == id {
                continue;
            }

            let status = &self.tables[*base_id].status;

            if matches!(status, TableStatus::InMemory(_))
                || matches!(status, TableStatus::OnDisk(_))
            {
                return Some(*base_id);
            }
        }

        None
    }

    fn materialize_required_tables(&mut self, tables: &HashSet<TableId>) {
        for &table_id in tables {
            let info = &self.tables[table_id];
            let info_order = info.column_order.clone(); // TODO: Clones because of borrow checker
            let table_name = self
                .dictionary
                .entry(info.key.predicate_id.0)
                .unwrap_or_else(|| "<Unkown table>".to_string());

            if let TableStatus::Derived = info.status {
                let base_id = self.find_materialized_table(&info.key, table_id).unwrap();
                let base_order = &self.tables[base_id].column_order.clone(); // TODO: Clones because of borrow checker

                let base_trie = if let TableStatus::InMemory(trie) = &self.tables[base_id].status {
                    trie
                } else if let TableStatus::OnDisk(source) = self.tables[base_id].status.clone() {
                    let trie = self.materialize_on_disk(&source, &info_order);
                    self.tables[table_id].status = TableStatus::InMemory(trie);
                    if let TableStatus::InMemory(trie) = &self.tables[table_id].status {
                        trie
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                };

                let project_iter = TrieScanProject::new(
                    base_trie,
                    TableManager::translate_order(base_order, &info_order),
                );

                let reordered_trie = materialize(&mut TrieScanEnum::TrieScanProject(project_iter));
                self.tables[table_id].status = TableStatus::InMemory(reordered_trie);

                log::info!(
                    "Materializing required table {} by reordering {:?} -> {:?}",
                    table_name,
                    info_order,
                    base_order
                );
            } else if let TableStatus::OnDisk(source) = info.status.clone() {
                let trie = self.materialize_on_disk(&source, &info_order);
                self.tables[table_id].status = TableStatus::InMemory(trie);
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

    fn add_table_helper(
        &mut self,
        predicate: Identifier,
        absolute_range: Range<usize>,
        order: ColumnOrder,
        priority: u64,
        derived: bool,
    ) -> Option<TableId> {
        let new_id = self.current_id;
        self.current_id += 1;

        if !derived && absolute_range.len() == 1 {
            let step_list = self
                .predicate_to_steps
                .entry(predicate)
                .or_insert_with(Vec::new);
            step_list.push(absolute_range.start);
        }

        let normalized_range = self.normalize_range(predicate, &absolute_range)?;

        if !derived {
            let range_list = self
                .predicate_to_ranges
                .entry(predicate)
                .or_insert_with(Vec::new);
            range_list.push(normalized_range.clone());
            range_list.sort_by(|a, b| match a.start.cmp(&b.start) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => a.end.cmp(&b.end),
                Ordering::Greater => Ordering::Greater,
            });
        }

        let table_list = self
            .entries
            .entry(TableKey {
                predicate_id: predicate,
                step_range: normalized_range,
            })
            .or_insert_with(Vec::new);

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

        // For debugging, check if arity is equal
        if !table_list.is_empty() {
            debug_assert!(table_list[0].0.len() == order.len());
        }

        table_list.push((order, new_id));

        Some(new_id)
    }

    /// Add a table that is stored on disk to the manager
    pub fn add_edb(
        &mut self,
        data_source: DataSource,
        predicate: Identifier,
        column_order: ColumnOrder,
        priority: u64,
    ) -> TableId {
        let key = TableKey {
            predicate_id: predicate,
            step_range: (0..1),
        };

        let trie = self.materialize_on_disk(&data_source, &column_order);
        self.tables.push(TableInfo {
            status: TableStatus::InMemory(trie),
            column_order: column_order.clone(),
            key,
            priority,
            space: 0, //TODO: How to do this
        });

        self.add_table_helper(predicate, 0..1, column_order, priority, false)
            .unwrap()
    }

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

        let new_id = self.add_table_helper(
            predicate,
            absolute_step_range.clone(),
            column_order.clone(),
            priority,
            false,
        )?;

        let key = TableKey {
            predicate_id: predicate,
            step_range: self.normalize_range(predicate, &absolute_step_range)?,
        };

        self.tables.push(TableInfo {
            status: TableStatus::InMemory(trie),
            column_order,
            key,
            priority,
            space: 0, //TODO: How to do this
        });

        Some(new_id)
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
            ExecutionOperation::Minus(left, _right) => Self::estimate_space(left),
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
        let new_id = self
            .add_table_helper(
                predicate,
                absolute_step_range.clone(),
                column_order.clone(),
                priority,
                true,
            )
            .unwrap();

        let key = TableKey {
            predicate_id: predicate,
            step_range: self
                .normalize_range(predicate, &absolute_step_range)
                .unwrap(),
        };

        self.tables.push(TableInfo {
            status: TableStatus::Derived,
            column_order,
            key,
            priority,
            space: 0, //TODO: How to do this
        });

        new_id
    }

    /// Executes an [`ExecutionSeries`] and adds the resulting tables
    /// Returns true if a non-empty table was added, false otherwise
    pub fn execute_series(&mut self, series: ExecutionSeries) -> bool {
        TimedCode::instance().sub("Reasoning/Materialize").start();

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
                            GetTableError::NoTable => {
                                log::warn!("expected a table for predicate {:?} with range {absolute_step_range:?} and order {column_order:?}", self.dictionary.entry(predicate.0).unwrap_or_else(|| "<Unkown predicate>".to_string()));
                            }
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

            TimedCode::instance()
                .sub("Reasoning/Materialize/Reorder")
                .start();

            // TODO: Materializig should check memory and so on...
            self.materialize_required_tables(&table_ids);

            TimedCode::instance()
                .sub("Reasoning/Materialize/Reorder")
                .stop();

            TimedCode::instance()
                .sub("Reasoning/Materialize/Pipeline")
                .start();

            const TMP_NAMES: &[&str] = &[
                "Body Join",
                "Head Join",
                "Body Frontier",
                "Head Frontier",
                "Existential Diff",
            ];

            let code_string = match &plan.result {
                ExecutionResult::Temp(tmp_id) => {
                    if *tmp_id < TMP_NAMES.len() {
                        TMP_NAMES[*tmp_id]
                    } else {
                        "Head Projection"
                    }
                }
                ExecutionResult::Save(_, range, _, _) => {
                    if range.len() == 1 {
                        "Duplicates & Head Union"
                    } else {
                        "Big union"
                    }
                }
            };

            TimedCode::instance()
                .sub("Reasoning/Materialize/Pipeline")
                .sub(code_string)
                .start();

            log::info!(
                "Plan: {}",
                self.get_iterator_string(&plan.root, &temp_tries)
            );

            let iter_option = self.get_iterator_node(&plan.root, &temp_tries);
            if let Some(mut iter) = iter_option {
                // TODO: Materializig should check memory and so on...
                let new_trie = materialize(&mut iter);
                let new_trie_len = new_trie.row_num();

                match plan.result {
                    ExecutionResult::Temp(id) => {
                        if new_trie_len > 0 {
                            temp_tries.insert(id, Some(new_trie));
                        } else {
                            temp_tries.insert(id, None);
                        }

                        log::info!("Temporary table {} ({} entries)", code_string, new_trie_len);
                    }
                    ExecutionResult::Save(pred, range, order, priority) => {
                        let pred_string = self
                            .dictionary
                            .entry(pred.0)
                            .unwrap_or_else(|| "<Unknown predicate>".to_string());
                        log::info!(
                            "Permament table {} with order {:?} ({} entries)",
                            pred_string,
                            order,
                            new_trie_len
                        );

                        if new_trie_len > 0 {
                            if range.len() == 1 {
                                new_table = true;
                            }

                            self.add_trie(pred, range, order, priority, new_trie);
                        }
                    }
                }
            } else {
                log::info!("Trie iterator is empty");
            }

            let duration = TimedCode::instance()
                .sub("Reasoning/Materialize/Pipeline")
                .sub(code_string)
                .stop();

            log::info!("{code_string}: {} ms", duration.as_millis());

            TimedCode::instance()
                .sub("Reasoning/Materialize/Pipeline")
                .stop();
        }

        TimedCode::instance().sub("Reasoning/Materialize").stop();

        new_table
    }

    fn get_iterator_string_sub<'a>(
        &'a self,
        operation: &str,
        subnodes: &Vec<&ExecutionNode>,
        temp_tries: &'a HashMap<TableId, Option<Trie>>,
    ) -> String {
        let mut result = String::from(operation);
        result += "(";

        for (index, sub) in subnodes.iter().enumerate() {
            result += &self.get_iterator_string(sub, temp_tries);

            if index < subnodes.len() - 1 {
                result += ", ";
            }
        }

        result += ")";

        result
    }

    fn get_iterator_string<'a>(
        &'a self,
        node: &'a ExecutionNode,
        temp_tries: &'a HashMap<TableId, Option<Trie>>,
    ) -> String {
        match &node.operation {
            ExecutionOperation::Temp(id) => {
                if let Some(tmp_trie_option) = temp_tries.get(id) {
                    if let Some(tmp_trie) = tmp_trie_option {
                        return tmp_trie.row_num().to_string();
                    } else {
                        return "0".to_string();
                    }
                }

                "Temp(Unknown id)".to_string()
            }
            ExecutionOperation::Fetch(predicate, absolute_step_range, column_order) => {
                if let Ok(table_id) = self.get_table(*predicate, absolute_step_range, column_order)
                {
                    let table_info = &self.tables[table_id];

                    if let TableStatus::InMemory(trie) = &table_info.status {
                        trie.row_num().to_string()
                    } else {
                        panic!("Base tables are supposed to be materialized");
                    }
                } else {
                    "Unknown table".to_string()
                }
            }
            ExecutionOperation::Join(sub, _) => {
                self.get_iterator_string_sub("Join", &sub.iter().collect(), temp_tries)
            }
            ExecutionOperation::Union(sub) => {
                self.get_iterator_string_sub("Union", &sub.iter().collect(), temp_tries)
            }
            ExecutionOperation::Minus(left, right) => {
                self.get_iterator_string_sub("Minus", &vec![left, right], temp_tries)
            }
            ExecutionOperation::Project(id, _) => {
                if let Some(tmp_trie_option) = temp_tries.get(id) {
                    if let Some(tmp_trie) = tmp_trie_option {
                        return format!("Project({})", tmp_trie.num_elements());
                    } else {
                        return "Project(0)".to_string();
                    }
                }

                "Project(Unknown id)".to_string()
            }
            ExecutionOperation::SelectValue(sub, _) => {
                self.get_iterator_string_sub("SelectValue", &vec![sub], temp_tries)
            }
            ExecutionOperation::SelectEqual(sub, _) => {
                self.get_iterator_string_sub("SelectEqual", &vec![sub], temp_tries)
            }
        }
    }

    /// Returns None if the TrieScan would be empty
    fn get_iterator_node<'a>(
        &'a self,
        node: &'a ExecutionNode,
        temp_tries: &'a HashMap<TableId, Option<Trie>>,
    ) -> Option<TrieScanEnum> {
        match &node.operation {
            ExecutionOperation::Temp(id) => Some(TrieScanEnum::TrieScanGeneric(
                TrieScanGeneric::new(temp_tries.get(id)?.as_ref()?),
            )),
            ExecutionOperation::Fetch(predicate, absolute_step_range, column_order) => {
                let table_info = &self.tables[self
                    .get_table(*predicate, absolute_step_range, column_order)
                    .ok()?];
                if let TableStatus::InMemory(trie) = &table_info.status {
                    if trie.row_num() == 0 {
                        return None;
                    }

                    let interval_trie_scan = TrieScanGeneric::new(trie);
                    Some(TrieScanEnum::TrieScanGeneric(interval_trie_scan))
                } else {
                    panic!("Base tables are supposed to be materialized");
                }
            }
            ExecutionOperation::Join(subtables, bindings) => {
                let mut subiterators: Vec<TrieScanEnum> = subtables
                    .iter()
                    .map(|s| self.get_iterator_node(s, temp_tries))
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
            ExecutionOperation::Union(subtables) => {
                let mut subiterators: Vec<TrieScanEnum> = subtables
                    .iter()
                    .map(|s| self.get_iterator_node(s, temp_tries))
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
            ExecutionOperation::Minus(subtable_left, subtable_right) => {
                let left_scan = self.get_iterator_node(subtable_left, temp_tries);
                let right_scan = self.get_iterator_node(subtable_right, temp_tries);

                left_scan.map(|ls| match right_scan {
                    Some(rs) => TrieScanEnum::TrieScanMinus(TrieScanMinus::new(ls, rs)),
                    None => ls,
                })
            }
            ExecutionOperation::Project(id, sorting) => {
                let tmp_trie = temp_tries.get(id)?.as_ref()?;
                let project_scan = TrieScanProject::new(tmp_trie, sorting.clone());

                Some(TrieScanEnum::TrieScanProject(project_scan))
            }
            ExecutionOperation::SelectValue(subtable, assignments) => {
                let subiterator = self.get_iterator_node(subtable, temp_tries)?;
                let select_scan = TrieScanSelectValue::new(subiterator, assignments.clone());

                Some(TrieScanEnum::TrieScanSelectValue(select_scan))
            }
            ExecutionOperation::SelectEqual(subtable, classes) => {
                let subiterator = self.get_iterator_node(subtable, temp_tries)?;
                let select_scan = TrieScanSelectEqual::new(subiterator, classes.clone());

                Some(TrieScanEnum::TrieScanSelectEqual(select_scan))
            }
        }
    }
}

#[cfg(test)]
mod test {}
