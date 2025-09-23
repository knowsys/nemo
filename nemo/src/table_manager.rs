//! Managing of tables

use crate::{error::Error, rule_model::components::tag::Tag};

use nemo_physical::{
    datatypes::StorageValueT,
    datavalues::any_datavalue::AnyDataValue,
    management::{
        bytesized::ByteSized,
        database::{
            DatabaseInstance, Dict,
            id::{ExecutionId, PermanentTableId},
            sources::TableSource,
        },
        execution_plan::{ColumnOrder, ExecutionNodeRef, ExecutionPlan},
    },
    tabular::{operations::OperationTable, trie::Trie},
    util::mapping::permutation::Permutation,
};

use std::{
    cell::{Ref, RefMut},
    cmp::Ordering,
    collections::HashMap,
    hash::Hash,
    ops::Range,
};

/// Indicates that the table contains the union of successive tables.
/// For example assume that for predicate p there were tables derived in steps 2, 4, 7, 10, 11.
/// The range [4, 10] would be represented with SubtableRange { start: 1, len: 3 }.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub struct SubtableRange {
    start: usize,
    len: usize,
}

impl SubtableRange {
    /// Return the start and (the included) end point of this range.
    pub fn start_end(&self) -> (usize, usize) {
        (self.start, self.end())
    }

    /// Return the end point of this range
    pub fn end(&self) -> usize {
        self.start + self.len - 1
    }
}

impl Ord for SubtableRange {
    fn cmp(&self, other: &Self) -> Ordering {
        let start_cmp = self.start.cmp(&other.start);
        if let Ordering::Equal = start_cmp {
            self.len.cmp(&other.len)
        } else {
            start_cmp
        }
    }
}

impl PartialOrd for SubtableRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
struct SubtableHandler {
    single: Vec<(usize, PermanentTableId)>,
    combined: Vec<(SubtableRange, PermanentTableId)>,
}

impl SubtableHandler {
    /// A table may either be created as a result of a rule application at some step
    /// or may represent a union of many previously computed tables.
    /// Say, we have a table whose contents have been computed at steps 2, 4, 7, 10, 11.
    /// On the outside, we might now refer to all tables between steps 3 and 11 (exclusive).
    /// Representing this as [3, 11) is ambigious as [4, 11) refers to the same three tables.
    /// Hence, we translate both representations to TableCover { start: 1, len: 3}.
    ///
    /// This function performs the translation illustrated above.
    fn normalize_range(&self, range: &Range<usize>) -> SubtableRange {
        let mut normalized_start: usize = 0;
        let mut normalized_len: usize = 0;

        let mut start_set = false;
        for (index, step) in self.single_steps().enumerate() {
            if !start_set && *step >= range.start {
                normalized_start = index;
                start_set = true
            }

            if start_set && *step >= range.end {
                break;
            }

            if start_set {
                normalized_len += 1;
            }
        }

        SubtableRange {
            start: normalized_start,
            len: normalized_len,
        }
    }

    fn single_steps(&self) -> impl Iterator<Item = &usize> {
        self.single.iter().map(|(s, _)| s)
    }

    pub fn last_step(&self) -> Option<usize> {
        self.single_steps().last().copied()
    }

    pub fn subtable(&self, step: usize) -> Option<PermanentTableId> {
        let postion = *self.single_steps().find(|&&s| s == step)?;
        Some(self.single[postion].1)
    }

    pub fn count_rows_in_memory(&self, database: &DatabaseInstance) -> usize {
        self.single
            .iter()
            .map(|(_, subtable_id)| database.count_rows_in_memory(*subtable_id))
            .sum()
    }

    pub fn add_single_table(&mut self, step: usize, id: PermanentTableId) {
        debug_assert!(self.single.is_empty() || (self.single.last().unwrap().0 < step));
        self.single.push((step, id));
    }

    pub fn add_combined_table(&mut self, range: &Range<usize>, id: PermanentTableId) {
        let cover = self.normalize_range(range);
        if cover.len <= 1 {
            return;
        }

        self.combined.push((cover, id));

        // Sorting is done here because it is assumed by the function self.cover_range
        self.combined.sort_by(|x, y| x.0.cmp(&y.0));
    }

    pub fn cover_range(&self, range: &Range<usize>) -> Vec<PermanentTableId> {
        let mut result = Vec::<PermanentTableId>::new();

        if self.single.is_empty() {
            return result;
        }

        let normalized_range = self.normalize_range(range);

        if normalized_range.len == 0 {
            return result;
        }

        let (target_start, target_end) = normalized_range.start_end();

        let mut current_start = target_start;
        let mut interval_index: usize = 0;

        while current_start <= target_end {
            let mut best: Option<(SubtableRange, PermanentTableId)> = None;

            for &(current_interval, id) in self.combined.iter().skip(interval_index) {
                interval_index += 1;
                let (start, end) = current_interval.start_end();

                if start > current_start {
                    break;
                }

                if start < target_start || end > target_end {
                    continue;
                }

                if let Some((best_interval, _)) = best {
                    if best_interval.end() < end {
                        best = Some((current_interval, id));
                    }
                } else {
                    best = Some((current_interval, id));
                }
            }

            if let Some((best, id)) = best {
                result.push(id);
                current_start = best.end() + 1;
            } else {
                result.push(self.single[current_start].1);
                current_start += 1;
            }
        }

        result
    }
}

#[derive(Debug)]
struct PredicateInfo {
    arity: usize,
}

/// Tag of a subtable in a chase sequence.
#[derive(Debug, Clone)]
pub struct SubtableIdentifier {
    predicate: Tag,
    step: usize,
}

impl SubtableIdentifier {
    /// Create a new [SubtableIdentifier].
    pub fn new(predicate: Tag, step: usize) -> Self {
        Self { predicate, step }
    }
}

/// A execution plan that will result in the creation of new chase subtables.
#[derive(Debug, Default)]
pub struct SubtableExecutionPlan {
    /// The execution plan.
    execution_plan: ExecutionPlan,
    /// Each tree in the plan that will result in a new permanent table
    /// will have an associated [SubtableIdentifier].
    map_subtrees: HashMap<ExecutionId, SubtableIdentifier>,
}

impl SubtableExecutionPlan {
    /// Add a temporary table to the plan.
    pub fn add_temporary_table(&mut self, node: ExecutionNodeRef, tree_name: &str) -> ExecutionId {
        self.execution_plan.write_temporary(node, tree_name)
    }

    /// Add a permanent table ot the plan-
    pub fn add_permanent_table(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
        subtable_id: SubtableIdentifier,
    ) -> ExecutionId {
        let node_id = self
            .execution_plan
            .write_permanent(node, tree_name, table_name);
        self.map_subtrees.insert(node_id, subtable_id);

        node_id
    }

    /// Return a mutable reference to the underlying [ExecutionPlan].
    pub fn plan_mut(&mut self) -> &mut ExecutionPlan {
        &mut self.execution_plan
    }
}

/// Stores information about memory usage of predicates
#[derive(Debug)]
pub struct MemoryUsage {
    name: String,
    memory: u64,
    sub_blocks: Vec<MemoryUsage>,
}

impl MemoryUsage {
    /// Create a new [MemoryUsage].
    pub fn new(name: &str, memory: u64) -> Self {
        Self {
            name: name.to_string(),
            memory,
            sub_blocks: Vec::new(),
        }
    }

    /// Create a new [MemoryUsage] block.
    pub fn new_block(name: &str) -> Self {
        Self::new(name, 0)
    }

    /// Add a sub-block.
    pub fn add_sub_block(&mut self, sub_block: MemoryUsage) {
        self.memory += sub_block.memory;
        self.sub_blocks.push(sub_block)
    }

    fn format_node(&self) -> String {
        format!("{} ({})", self.name, self.memory)
    }

    fn ascii_tree_recursive(usage: &Self) -> ascii_tree::Tree {
        if usage.sub_blocks.is_empty() {
            ascii_tree::Tree::Leaf(vec![usage.format_node()])
        } else {
            let mut sorted_sub_blocks: Vec<&MemoryUsage> = usage.sub_blocks.iter().collect();
            sorted_sub_blocks.sort_by(|a, b| b.memory.partial_cmp(&a.memory).unwrap());

            ascii_tree::Tree::Node(
                usage.format_node(),
                sorted_sub_blocks
                    .into_iter()
                    .map(Self::ascii_tree_recursive)
                    .collect(),
            )
        }
    }

    /// Return an [ascii_tree::Tree] representation.
    pub fn ascii_tree(&self) -> ascii_tree::Tree {
        Self::ascii_tree_recursive(self)
    }
}

impl std::fmt::Display for MemoryUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ascii_tree::write_tree(f, &self.ascii_tree())
    }
}

/// Manager object for handling tables that are the result
/// of a seminaive existential rules evaluation process.
#[derive(Debug)]
pub(crate) struct TableManager {
    /// [DatabaseInstance] managing all existing tables.
    database: DatabaseInstance,

    /// Map containg all the ids of all the sub tables associated with a predicate.
    predicate_subtables: HashMap<Tag, SubtableHandler>,

    /// Mapping predicate identifiers to a [PredicateInfo] which contains relevant information.
    predicate_to_info: HashMap<Tag, PredicateInfo>,
}

impl Default for TableManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TableManager {
    /// Create new [TableManager].
    pub(crate) fn new() -> Self {
        Self {
            database: DatabaseInstance::default(),
            predicate_subtables: HashMap::new(),
            predicate_to_info: HashMap::new(),
        }
    }

    /// Return a mutbale reference to the [DatabaseInstance].
    pub(crate) fn database_mut(&mut self) -> &mut DatabaseInstance {
        &mut self.database
    }

    /// Return a reference to the [DatabaseInstance].
    pub(crate) fn database(&self) -> &DatabaseInstance {
        &self.database
    }

    /// Return the [PermanentTableId] that is associated with a given subtable.
    /// Returns None if the predicate does not exist.
    fn table_id(&self, subtable: &SubtableIdentifier) -> Option<PermanentTableId> {
        self.predicate_subtables
            .get(&subtable.predicate)?
            .subtable(subtable.step)
    }

    /// Return the step number of the last subtable that was added under a predicate.
    /// Returns None if the predicate has no subtables.
    pub(crate) fn last_step(&self, predicate: &Tag) -> Option<usize> {
        self.predicate_subtables.get(predicate)?.last_step()
    }

    /// Count all in-memory rows in the table manager that belong to a predicate.
    pub(crate) fn count_rows_in_memory_for_predicate(&self, predicate: &Tag) -> Option<usize> {
        self.predicate_subtables
            .get(predicate)
            .map(|s| s.count_rows_in_memory(&self.database))
    }

    /// Get a list of column iterators for the full table (i.e. the expanded trie)
    pub(crate) async fn table_row_iterator(
        &mut self,
        id: PermanentTableId,
    ) -> Result<impl Iterator<Item = Vec<AnyDataValue>> + '_, Error> {
        Ok(self.database.table_row_iterator(id).await?)
    }

    /// Get a an iterator over the rows of a trie
    pub(crate) fn trie_row_iterator<'a>(
        &'a self,
        trie: &'a Trie,
    ) -> Result<impl Iterator<Item = Vec<AnyDataValue>> + 'a, Error> {
        Ok(self.database.trie_row_iterator(trie)?)
    }

    pub(crate) async  fn table_raw_row_iterator(
        &mut self,
        id: PermanentTableId,
    ) -> Result<impl Iterator<Item = Vec<StorageValueT>> + '_, Error> {
        Ok(self.database.table_raw_row_iterator(id).await?)
    }

    /// Combine all subtables of a predicate into one table
    /// and return the [PermanentTableId] of that new table.
    pub(crate) async fn combine_predicate(
        &mut self,
        predicate: &Tag,
    ) -> Result<Option<PermanentTableId>, Error> {
        match self.last_step(predicate) {
            Some(last_step) => self.combine_tables(predicate, 0..(last_step + 1)).await,
            None => Ok(None),
        }
    }

    /// Generates an appropriate table name for subtable.
    pub(crate) fn generate_table_name(
        &self,
        predicate: &Tag,
        order: &ColumnOrder,
        step: usize,
    ) -> String {
        format!("{predicate} ({step}) {order:?}")
    }

    /// Generates an appropriate table name for a table that represents multiple subtables.
    fn generate_table_name_combined(
        &self,
        predicate: &Tag,
        order: &ColumnOrder,
        steps: &Range<usize>,
    ) -> String {
        format!("{predicate} ({}-{}) {order:?}", steps.start, steps.end)
    }

    /// Generates an appropriate table name for a table that is a reordered version of another.
    fn generate_table_name_reference(
        &self,
        predicate: &Tag,
        step: usize,
        referenced_table_id: PermanentTableId,
        permutation: &Permutation,
    ) -> String {
        let referenced_table_name = self.database.table_name(referenced_table_id);

        format!("{predicate} ({step}) -> {referenced_table_name} {permutation}")
    }

    /// Intitializes helper structures that are needed for handling the table associated with the predicate.
    /// Must be done before calling functions that add tables to that predicate.
    pub(crate) fn register_predicate(&mut self, predicate: Tag, arity: usize) {
        let predicate_info = PredicateInfo { arity };

        if self
            .predicate_to_info
            .insert(predicate.clone(), predicate_info)
            .is_some()
        {
            panic!("predicates must uniquely identify one relation");
        }
        self.predicate_subtables
            .insert(predicate, SubtableHandler::default());
    }

    /// Check whether a predicate has been registered.
    #[allow(dead_code)]
    fn predicate_exists(&self, predicate: &Tag) -> bool {
        self.predicate_subtables.contains_key(predicate)
    }

    fn add_subtable(&mut self, subtable: SubtableIdentifier, table_id: PermanentTableId) {
        let subtable_handler = self
            .predicate_subtables
            .get_mut(&subtable.predicate)
            .expect("Predicate should be registered before calling this function");

        subtable_handler.add_single_table(subtable.step, table_id);
    }

    /// Add a table that represents the input facts for some predicate for the chase procedure.
    /// Predicate must be registered before calling this function.
    pub(crate) fn add_edb(&mut self, predicate: Tag, sources: Vec<TableSource>) {
        let arity = if let Some(source) = sources.first() {
            source.arity()
        } else {
            return;
        };

        let edb_order = ColumnOrder::default();
        const EDB_STEP: usize = 0;

        let name = self.generate_table_name(&predicate, &edb_order, EDB_STEP);

        let table_id = self.database.register_table(&name, arity);
        self.database.add_sources(table_id, edb_order, sources);

        self.add_subtable(SubtableIdentifier::new(predicate, EDB_STEP), table_id)
    }

    /// Add a [Trie] as a subtable of a predicate.
    /// Predicate must be registered before calling this function.
    #[allow(dead_code)]
    pub fn add_table(&mut self, predicate: Tag, step: usize, order: ColumnOrder, trie: Trie) {
        let name = self.generate_table_name(&predicate, &order, step);

        let table_id = self.database.register_add_trie(&name, order, trie);
        self.add_subtable(SubtableIdentifier::new(predicate, step), table_id);
    }

    /// Add a reference to another table under a new name.
    /// Predicate must be registered before calling this function and referenced table must exist.
    #[allow(dead_code)]
    fn add_reference(
        &mut self,
        subtable: SubtableIdentifier,
        referenced_subtable: SubtableIdentifier,
        permutation: Permutation,
    ) {
        let referenced_id = self
            .table_id(&referenced_subtable)
            .expect("Referenced table does not exist.");

        let arity = self.arity(&subtable.predicate);

        let name = self.generate_table_name_reference(
            &subtable.predicate,
            subtable.step,
            referenced_id,
            &permutation,
        );

        let table_id = self.database.register_table(&name, arity);
        self.database
            .add_reference(table_id, referenced_id, permutation);

        self.add_subtable(subtable, table_id);
    }

    /// Return the arity of a given predicate.
    ///
    /// # Panics
    /// Panics if the predicate has not been registered yet.
    pub(crate) fn arity(&self, predicate: &Tag) -> usize {
        self.predicate_to_info
            .get(predicate)
            .expect("Predicate should be registered before calling this function")
            .arity
    }

    /// For a predicate,
    /// return all tables that are within a given range of steps.
    ///
    /// Returns a list of pairs consisting of step and id.
    pub fn tables_in_range_steps(
        &self,
        predicate: &Tag,
        range: Range<usize>,
    ) -> Vec<(usize, PermanentTableId)> {
        self.predicate_subtables
            .get(predicate)
            .map(|handler| {
                handler
                    .single
                    .iter()
                    .filter_map(|(step, id)| {
                        if range.contains(step) {
                            Some((*step, *id))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    /// For a predicate,
    /// return all tables within a given range of steps
    /// derived a given rule.
    ///
    /// Returns a list of pairs consisting of step and id.
    pub fn tables_in_range_rule_steps(
        &self,
        predicate: &Tag,
        range: Range<usize>,
        rules: &[usize],
        rule: usize,
    ) -> Vec<(usize, PermanentTableId)> {
        self.predicate_subtables
            .get(predicate)
            .map(|handler| {
                handler
                    .single
                    .iter()
                    .filter_map(|(step, id)| {
                        if rules[*step] == rule && range.contains(step) {
                            Some((*step, *id))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    /// Return the ids of all subtables of a predicate within a certain range of steps.
    pub fn tables_in_range(&self, predicate: &Tag, range: &Range<usize>) -> Vec<PermanentTableId> {
        self.predicate_subtables
            .get(predicate)
            .map(|handler| handler.cover_range(range))
            .unwrap_or_default()
    }

    /// Combine subtables in a certain range into one larger table.
    pub async fn combine_tables(
        &mut self,
        predicate: &Tag,
        range: Range<usize>,
    ) -> Result<Option<PermanentTableId>, Error> {
        let combined_order: ColumnOrder = ColumnOrder::default();

        let name = self.generate_table_name_combined(predicate, &combined_order, &range);
        let Some(subtable_handler) = self.predicate_subtables.get_mut(predicate) else {
            return Ok(None);
        };

        let tables = subtable_handler.cover_range(&range);

        if tables.len() == 1 {
            return Ok(Some(tables[0]));
        }

        let mut union_plan = ExecutionPlan::default();
        let fetch_nodes = tables
            .iter()
            .map(|id| union_plan.fetch_table(OperationTable::default(), *id))
            .collect();
        let union_node = union_plan.union(OperationTable::default(), fetch_nodes);
        let plan_id = union_plan.write_permanent(union_node, "Combining Tables", &name);

        let execution_result = self.database.execute_plan(union_plan).await?;
        let table_id = execution_result
            .get(&plan_id)
            .expect("Combining multiple non-empty tables should result in a non-empty table.");

        subtable_handler.add_combined_table(&range, *table_id);

        Ok(Some(*table_id))
    }

    /// Execute a plan and add the results as subtables to the manager.
    pub async fn execute_plan(
        &mut self,
        subtable_plan: SubtableExecutionPlan,
    ) -> Result<Vec<Tag>, Error> {
        let result = self
            .database
            .execute_plan(subtable_plan.execution_plan)
            .await?;

        let mut updated_predicates = Vec::new();
        for (plan_id, table_id) in result {
            let subtable = subtable_plan.map_subtrees.get(&plan_id).unwrap();
            updated_predicates.push(subtable.predicate.clone());

            self.add_subtable(subtable.clone(), table_id);
        }

        Ok(updated_predicates)
    }

    /// Returns a reference to the constants dictionary
    #[allow(dead_code)]
    pub fn dictionary(&self) -> Ref<'_, Dict> {
        self.database.dictionary()
    }

    /// Returns a mutable reference to the dictionary used for associating abstract constants with strings.
    pub fn dictionary_mut(&mut self) -> RefMut<'_, Dict> {
        self.database.dictionary_mut()
    }

    /// Return the current [MemoryUsage].
    pub fn memory_usage(&self) -> MemoryUsage {
        let mut result = MemoryUsage::new_block("Total");

        let memory = self.database.dictionary().size_bytes();
        result.add_sub_block(MemoryUsage::new("Dictionary", memory));

        let mut steps = MemoryUsage::new_block("Chase steps");

        for (identifier, subtable_handler) in &self.predicate_subtables {
            let mut predicate_usage = MemoryUsage::new_block(&identifier.to_string());

            for (step, id) in &subtable_handler.single {
                let memory = self.database.table_size_bytes(*id);
                predicate_usage.add_sub_block(MemoryUsage::new(&format!("Step {step}"), memory));
            }
            for (steps, id) in &subtable_handler.combined {
                let memory = self.database.table_size_bytes(*id);
                predicate_usage.add_sub_block(MemoryUsage::new(
                    &format!("Steps {}-{}", steps.start, steps.start + steps.len),
                    memory,
                ));
            }

            steps.add_sub_block(predicate_usage);
        }
        result.add_sub_block(steps);

        result
    }

    /// Return the chase step of the sub table that contains the given row within the given predicate.
    /// Returns None if the row does not exist.
    pub async fn find_table_row(&mut self, predicate: &Tag, row: &[AnyDataValue]) -> Option<usize> {
        let handler = self.predicate_subtables.get(predicate)?;

        for (step, id) in &handler.single {
            if self.database.table_contains_row(*id, row).await {
                return Some(*step);
            }
        }

        None
    }

    /// Return an id for a given row and predicate, if it exists.
    ///
    /// Returns `None` if there is no such row for this predicate.
    pub async fn table_row_id(&mut self, predicate: &Tag, row: &[AnyDataValue]) -> Option<usize> {
        let handler = self.predicate_subtables.get(predicate)?;

        let mut skipped: usize = 0;
        for (_, id) in &handler.single {
            if let Some(row_index) = self.database.table_row_position(*id, row).await {
                return Some(skipped + row_index);
            }

            skipped += self.database.count_rows_in_memory(*id);
        }

        None
    }

    /// Execute a given [SubtableExecutionPlan]
    /// but evaluate it only until the first row of the result table
    /// or return None if it is empty.
    /// The result table is considered to be the (unique) table marked as permanent output.
    ///
    /// Assumes that the given plan has only one output node.
    /// No tables will be saved in the database.
    pub async fn execute_plan_first_match(
        &mut self,
        subtable_plan: SubtableExecutionPlan,
    ) -> Option<Vec<AnyDataValue>> {
        self.database
            .execute_first_match(subtable_plan.execution_plan)
            .await
    }

    /// Execute a given [SubtableExecutionPlan]
    /// and return a list of [Trie]s for each permanent table
    /// instead of saving it to the database.
    pub async fn execute_plan_trie(
        &mut self,
        subtable_plan: SubtableExecutionPlan,
    ) -> Result<Vec<Trie>, Error> {
        Ok(self
            .database
            .execute_plan_trie(subtable_plan.execution_plan)
            .await?)
    }

    pub fn known_predicates(&self) -> impl Iterator<Item = &Tag> {
        self.predicate_subtables.keys()
    }
}

#[cfg(test)]
mod test {
    use nemo_physical::management::database::id::{PermanentTableId, TableId};

    use super::SubtableHandler;
    use std::{collections::HashSet, ops::Range};

    fn handler_from_table_steps(
        steps: &[usize],
        ranges: &[Range<usize>],
        expected_ranges: &[Range<usize>],
    ) -> (SubtableHandler, HashSet<PermanentTableId>) {
        let expected_set: HashSet<Range<usize>> = expected_ranges.iter().cloned().collect();

        let mut handler = SubtableHandler::default();
        let mut expected_ids = HashSet::<PermanentTableId>::new();
        let mut id = PermanentTableId::default();

        let intervals = steps
            .iter()
            .map(|&s| s..(s + 1))
            .chain(ranges.iter().cloned());

        for interval in intervals {
            let id = id.increment();

            if interval.len() == 1 {
                handler.add_single_table(interval.start, id);
            } else {
                handler.add_combined_table(&interval, id);
            }

            if expected_set.contains(&interval) {
                expected_ids.insert(id);
            }
        }

        (handler, expected_ids)
    }

    fn compare_covering(
        steps: &[usize],
        ranges: &[Range<usize>],
        expected_ranges: &[Range<usize>],
        target: &Range<usize>,
    ) {
        let (handler, expected_ids) = handler_from_table_steps(steps, ranges, expected_ranges);
        let answer: HashSet<PermanentTableId> =
            handler.cover_range(target).iter().cloned().collect();

        assert_eq!(answer, expected_ids);
    }

    #[test]
    fn test_cover() {
        let steps = vec![1, 2, 4, 10];
        let ranges = vec![1..6, 2..11];
        let target = 1..11;
        let expected_ranges = vec![1..6, 10..11];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 1..13;
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 0..11;
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let steps = vec![0, 1];
        let ranges = vec![];
        let expected_ranges = vec![0..1, 1..2];
        let target = 0..2;
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let steps = vec![1, 4, 6, 7, 12];
        let ranges = vec![4..8, 6..13, 1..5];
        let target = 0..5;
        let expected_ranges = vec![1..5; 1];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 3..5;
        let expected_ranges = vec![4..5; 1];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 3..13;
        let expected_ranges = vec![4..8, 12..13];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 0..30;
        let expected_ranges = vec![1..5, 6..13];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 6..8;
        let expected_ranges = vec![6..7, 7..8];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let steps = vec![1, 3, 5, 7, 9, 11, 13, 15];
        let ranges = vec![0..8, 0..16, 9..16];
        let target = 0..16;
        #[allow(clippy::single_range_in_vec_init)]
        let expected_ranges = vec![0..16];
        compare_covering(&steps, &ranges, &expected_ranges, &target);
    }
}
