use super::execution_plan::{ExecutionOperation, ExecutionPlan};
use super::model::{DataSource, Identifier};
use crate::physical::tables::materialize::scan_is_empty;
use crate::physical::tables::{
    materialize, IntervalTrieScan, Table, Trie, TrieDifference, TrieJoin, TrieProject,
    TrieScanEnum, TrieUnion,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Range;

/// Type which represents a variable ordering
pub type VariableOrder = Vec<usize>;

/// Type which represents table ids
pub type TableId = usize;

/// Information which identfies a table
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    pub predicate_id: Identifier,
    pub step_range: Range<usize>,
    pub variable_order: VariableOrder,
}

/// Represents the storage status of a table
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TableStatus {
    /// Table is not materialized and has to be generated from an iterator
    Derived,
    /// Table has to be read from disk
    OnDisk,
    /// Table is present in main memory in materialized form
    InMemory,
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

#[derive(Debug)]
pub struct TableInfo {
    plan: Option<ExecutionPlan>,
    table: Option<Trie>,
    data_source: Option<DataSource>,

    current_status: TableStatus,
    preferred_status: TableStatus,

    priority: u64,
    space: u64,
}

#[derive(Debug)]
pub struct TableManager {
    strategy: TableManagerStrategy,
    tables: Vec<TableInfo>,
    entries: HashMap<Identifier, Vec<(TableIdentifier, TableId)>>,

    priority_heap: BinaryHeap<TablePriority>,

    space_consumed: u64,

    current_id: TableId,
}

impl TableManager {
    /// Create new [`TableManager`]
    pub fn new(strategy: TableManagerStrategy) -> Self {
        Self {
            strategy,
            tables: Vec::new(),
            entries: HashMap::new(),
            priority_heap: BinaryHeap::new(),
            space_consumed: 0,
            current_id: 0,
        }
    }

    /// Iterator of all table of a certain predicates
    /// Get further information with get_info(id)
    pub fn get_tables_by_predicate(
        &self,
        predicate: Identifier,
    ) -> impl Iterator<Item = &(TableIdentifier, TableId)> {
        self.entries.get(&predicate).unwrap().iter()
    }

    ///
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
            }
        } else {
            unimplemented!();
        }
    } */

    /// Add a table that is stored on disk to the manager
    pub fn add_edb(
        &mut self,
        data_source: DataSource,
        predicate: Identifier,
        priority: u64,
    ) -> TableId {
        self.tables.push(TableInfo {
            plan: None,
            data_source: Some(data_source),
            table: None,
            current_status: TableStatus::OnDisk,
            preferred_status: TableStatus::InMemory,
            priority,
            space: 0, //TODO: How to do this
        });

        let identifier = TableIdentifier {
            predicate_id: predicate,
            step_range: (0..0),
            variable_order: Vec::new(), //TODO: Set this properly
        };

        let new_id = self.current_id;
        self.current_id += 1;

        let table_list = self
            .entries
            .entry(identifier.predicate_id)
            .or_insert_with(|| Vec::new());
        table_list.push((identifier, new_id));

        self.priority_heap.push(TablePriority {
            table_id: new_id,
            priority,
        });

        new_id
    }

    /// Add a computed table to the manager
    pub fn add_idb(&mut self, plan: ExecutionPlan, identifier: TableIdentifier) -> TableId {
        let preferred_status = plan.target_status;
        let priority = plan.target_priority;

        self.tables.push(TableInfo {
            plan: Some(plan),
            table: None,
            data_source: None,
            current_status: TableStatus::Derived,
            preferred_status,
            priority,
            space: 0, //TODO: How to do this
        });

        let new_id = self.current_id;
        self.current_id += 1;

        let table_list = self
            .entries
            .entry(identifier.predicate_id)
            .or_insert_with(|| Vec::new());
        table_list.push((identifier, new_id));

        // self.update_status(new_id);

        new_id
    }

    /// Searches for table based on a [`TableIdentifier`]
    pub fn get_table(&self, identifier: TableIdentifier) -> Option<TableId> {
        for (table, id) in self.entries.get(&identifier.predicate_id)? {
            if table.predicate_id == identifier.predicate_id
                && table.step_range == identifier.step_range
            // TODO: normalize ranges
            // TODO: comparing vectors...
            // && table.variable_order == identifier.variable_order
            {
                return Some(*id);
            }
        }

        return None;
    }

    /// Checks if the given table is empty
    pub fn table_is_empty(&self, table_id: TableId) -> bool {
        let table_info = self.tables[table_id];

        match table_info.current_status {
            TableStatus::InMemory => table_info.table.unwrap().row_num() > 0,
            TableStatus::Derived => {
                scan_is_empty(&mut self.get_iterator(table_info.plan.as_ref().unwrap()))
            }
            TableStatus::OnDisk => false, // TODO: Whats the best way to determine this?
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

    ///TODO: ..
    pub fn get_table_alternatives() -> Vec<u64> {
        Vec::new()
    }

    ///TODO: ...
    pub fn estimate_costs() -> u64 {
        0
    }

    /// Estimate the potential space the materialization of a [`ExecutionPlan`] would take
    pub fn estimate_space(&self, plan: &ExecutionPlan) -> u64 {
        match &plan.operation {
            // TODO:
            ExecutionOperation::Fetch(id) => 0,
            // TODO: This is tricky
            ExecutionOperation::Join(subtables, schema) => 0,
            ExecutionOperation::Union(subtables) => {
                return subtables.iter().map(|s| self.estimate_space(&s)).sum();
            }
            ExecutionOperation::Minus(subtables) => {
                return self.estimate_space(&subtables[0]);
            }
            ExecutionOperation::Project(table_id, schema_sorting) => {
                // Should be easy to calculate
                0
            }
        }
    }

    /// TODO: Think about whether this should assume that all subtables are in memory
    /// TODO: Also for now this does not implement the materialize subtables feature
    /// Compute trie iterator based on execution plan
    fn get_iterator(&self, plan: &ExecutionPlan) -> TrieScanEnum {
        match &plan.operation {
            ExecutionOperation::Fetch(id) => {
                let table_info = self.get_info(*id);

                if table_info.current_status == TableStatus::InMemory {
                    let interval_trie_scan =
                        IntervalTrieScan::new(&table_info.table.as_ref().unwrap());

                    return TrieScanEnum::IntervalTrieScan(interval_trie_scan);
                } else if table_info.current_status == TableStatus::Derived {
                    return self.get_iterator(&table_info.plan.as_ref().unwrap());
                } else {
                    unimplemented!();
                }
            }
            ExecutionOperation::Join(subtables, schema) => {
                let join_scan = TrieJoin::new(
                    subtables.iter().map(|s| self.get_iterator(&s)).collect(),
                    schema.clone(),
                );

                return TrieScanEnum::TrieJoin(join_scan);
            }
            ExecutionOperation::Union(subtables) => {
                let union_scan =
                    TrieUnion::new(subtables.iter().map(|s| self.get_iterator(&s)).collect());

                return TrieScanEnum::TrieUnion(union_scan);
            }
            ExecutionOperation::Minus(subtables) => {
                debug_assert!(subtables.len() == 2);

                let difference_scan = TrieDifference::new(
                    self.get_iterator(&subtables[0]),
                    self.get_iterator(&subtables[1]),
                );
                return TrieScanEnum::TrieDifference(difference_scan);
            }
            ExecutionOperation::Project(table_id, schema_sorting) => {
                // TODO: We assume here that table is in memory
                let trie = self.get_info(*table_id).table.as_ref().unwrap();

                let project_scan = TrieProject::new(trie, schema_sorting.clone());
                return TrieScanEnum::TrieProject(project_scan);
            }
        }
    }
}

#[cfg(test)]
mod test {}
