//! Managing of tables

use super::model::Identifier;
use crate::{
    error::Error,
    physical::{
        datatypes::StorageTypeName,
        management::{
            database::{ColumnOrder, TableId, TableSource},
            execution_plan::ExecutionNodeRef,
            DatabaseInstance, ExecutionPlan,
        },
        tabular::{
            table_types::trie::{DebugTrie, Trie},
            traits::table_schema::TableSchema,
        },
        util::mapping::permutation::Permutation,
    },
};
use std::{cmp::Ordering, collections::HashMap, hash::Hash, ops::Range};

/// Indicates that the table contains the union of successive tables.
/// For example assume that for predicate p there were tables derived in steps 2, 4, 7, 10, 11.
/// The range [4, 10] would be represented with `SubtableRange { start: 1, len: 3 }`.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub struct SubtableRange {
    start: usize,
    len: usize,
}

impl SubtableRange {
    /// Return the start and (the included) end point of this range.
    pub fn start_end(&self) -> (usize, usize) {
        (self.start, self.start + self.len - 1)
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
    single: Vec<(usize, TableId)>,
    combined: Vec<(SubtableRange, TableId)>,
}

impl SubtableHandler {
    /// A table may either be created as a result of a rule application at some step
    /// or may represent a union of many previously computed tables.
    /// Say, we have a table whose contents have been computed at steps 2, 4, 7, 10, 11.
    /// On the outside, we might now refer to all tables between steps 3 and 11 (exclusive).
    /// Representing this as [3, 11) is ambigious as [4, 11) refers to the same three tables.
    /// Hence, we translate both representations to `TableCover { start: 1, len: 3}`.
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

    pub fn subtable(&self, step: usize) -> Option<TableId> {
        let postion = *self.single_steps().find(|&&s| s == step)?;
        Some(self.single[postion].1)
    }

    pub fn add_single_table(&mut self, step: usize, id: TableId) {
        debug_assert!(self.single.is_empty() || (self.single.last().unwrap().0 < step));
        self.single.push((step, id));
    }

    pub fn add_combined_table(&mut self, range: &Range<usize>, id: TableId) {
        let cover = self.normalize_range(range);
        if cover.len <= 1 {
            return;
        }

        self.combined.push((cover, id));

        // Sorting is done here because it is assumed by the function `self.cover_range`
        self.combined.sort_by(|x, y| x.0.cmp(&y.0));
    }

    pub fn cover_range(&self, range: &Range<usize>) -> Vec<TableId> {
        let mut result = Vec::<TableId>::new();
        if self.single.is_empty() {
            return result;
        }

        let normalized_range = self.normalize_range(range);
        if normalized_range.len == 0 {
            return result;
        }

        let (target_start, target_end) = normalized_range.start_end();

        // Algorithm is inspired by:
        // https://www.geeksforgeeks.org/minimum-number-of-intervals-to-cover-the-target-interval

        let mut current_start = target_start;
        let mut current_end = target_start;
        let mut current_id = self.single[current_start].1;

        // Iterate over all the intervals
        for (range, id) in &self.combined {
            let (range_start, range_end) = range.start_end();

            if range_start > target_end {
                break;
            }

            if range_start < target_start {
                continue;
            }

            if range_start > current_start {
                result.push(current_id);

                for step in current_end + 1..range_start {
                    result.push(self.single[step].1);
                }

                current_start = current_end + 1;
                current_end = current_start;
                current_id = self.single[current_start].1;
            }

            if range_end <= target_end && range_end > current_end {
                current_end = range_end;
                current_id = *id;
            }
        }

        if current_end <= target_end {
            result.push(current_id);
        }

        for step in current_end + 1..=target_end {
            result.push(self.single[step].1);
        }
        result
    }
}

#[derive(Debug)]
struct PredicateInfo {
    schema: TableSchema,
}

/// Identifier of a subtable in a chase sequence.
#[derive(Debug, Clone)]
pub struct SubtableIdentifier {
    predicate: Identifier,
    step: usize,
}

impl SubtableIdentifier {
    /// Create a new [`SubtableIdentifier`].
    pub fn new(predicate: Identifier, step: usize) -> Self {
        Self { predicate, step }
    }
}

/// A execution plan that will result in the creation of new chase subtables.
#[derive(Debug, Default)]
pub struct SubtableExecutionPlan {
    /// The execution plan.
    execution_plan: ExecutionPlan,
    /// Each tree in the plan that will result in a new permanent table
    /// will have an associated [`SubtableIdentifier`].
    map_subtrees: HashMap<usize, SubtableIdentifier>,
}

impl SubtableExecutionPlan {
    /// Add a temporary table to the plan.
    pub fn add_temporary_table(&mut self, node: ExecutionNodeRef, tree_name: &str) -> usize {
        self.execution_plan.write_temporary(node, tree_name)
    }

    /// Add a permanent table ot the plan-
    pub fn add_permanent_table(
        &mut self,
        node: ExecutionNodeRef,
        tree_name: &str,
        table_name: &str,
        subtable_id: SubtableIdentifier,
    ) -> usize {
        let node_id = self
            .execution_plan
            .write_permanent(node, tree_name, table_name);
        self.map_subtrees.insert(node_id, subtable_id);

        node_id
    }

    /// Return a reference to the underlying [`ExecutionPlan`].
    pub fn plan(&self) -> &ExecutionPlan {
        &self.execution_plan
    }

    /// Return a mutable reference to the underlying [`ExecutionPlan`].
    pub fn plan_mut(&mut self) -> &mut ExecutionPlan {
        &mut self.execution_plan
    }
}

/// Manager object for handling tables that are the result
/// of a seminaive existential rules evaluation process.
#[derive(Debug)]
pub struct TableManager {
    /// [`DatabaseInstance`] managing all existing tables.
    database: DatabaseInstance,

    /// Map containg all the ids of all the sub tables associated with a predicate.
    predicate_subtables: HashMap<Identifier, SubtableHandler>,

    /// Mapping predicate identifiers to a [`PredicateInfo`] which contains relevant information.
    predicate_to_info: HashMap<Identifier, PredicateInfo>,
}

impl Default for TableManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TableManager {
    /// Create new [`TableManager`].
    pub fn new() -> Self {
        Self {
            database: DatabaseInstance::new(),
            predicate_subtables: HashMap::new(),
            predicate_to_info: HashMap::new(),
        }
    }

    /// Return the [`TableId`] that is associated with a given subtable.
    /// Returns `None` if the predicate does not exist.
    fn table_id(&self, subtable: &SubtableIdentifier) -> Option<TableId> {
        self.predicate_subtables
            .get(&subtable.predicate)?
            .subtable(subtable.step)
    }

    /// Return the step number of the last subtable that was added under a predicate.
    /// Returns `None` if the predicate has no subtables.
    pub fn last_step(&self, predicate: Identifier) -> Option<usize> {
        self.predicate_subtables.get(&predicate)?.last_step()
    }

    /// Return a [`DebugTrie`] that is associated with the given id.
    /// Uses the default [`ColumnOrder`]
    /// Panics if there is no trie associated with the given id.
    pub fn table_from_id(&self, id: TableId) -> DebugTrie {
        self.database.get_debug_trie(id, &ColumnOrder::default())
    }

    /// Combine all subtables of a predicate into one table
    /// and return the [`TableId`] of that new table.
    pub fn combine_predicate(&mut self, predicate: Identifier) -> Result<Option<TableId>, Error> {
        match self.last_step(predicate.clone()) {
            Some(last_step) => self.combine_tables(predicate, 0..(last_step + 1)),
            None => Ok(None),
        }
    }

    /// Generates an appropriate table name for subtable.
    pub fn generate_table_name(
        &self,
        predicate: Identifier,
        order: &ColumnOrder,
        step: usize,
    ) -> String {
        let predicate_name = predicate.name();
        let order_string = format!("{order:?}");

        format!("{predicate_name} ({step}) {order_string}")
    }

    /// Generates an appropriate table name for a table that represents multiple subtables.
    fn generate_table_name_combined(
        &self,
        predicate: Identifier,
        order: &ColumnOrder,
        steps: &Range<usize>,
    ) -> String {
        let predicate_name = predicate.name();
        let order_string = format!("{order:?}");

        format!(
            "{predicate_name} ({}-{}) {order_string}",
            steps.start, steps.end
        )
    }

    /// Generates an appropriate table name for a table that is a reordered version of another.
    fn generate_table_name_reference(
        &self,
        predicate: Identifier,
        step: usize,
        referenced_table_id: TableId,
        permutation: &Permutation,
    ) -> String {
        let predicate_name = predicate.name();
        let referenced_table_name = self.database.table_name(referenced_table_id);

        format!("{predicate_name} ({step}) -> {referenced_table_name} {permutation}")
    }

    /// Workaround because of the missing type system.
    /// We assume for now that every table is a U64 table.
    fn generate_table_schema(arity: usize) -> TableSchema {
        let mut schema = TableSchema::new();
        (0..arity).for_each(|_| schema.add_entry(StorageTypeName::U64, false, false));

        schema
    }

    /// Intitializes helper structures that are needed for handling the table associated with the predicate.
    /// Must be done before calling functions that add tables to that predicate.
    pub fn register_predicate(&mut self, predicate: Identifier, arity: usize) {
        // TODO: Change this once type system is integrated
        let predicate_info = PredicateInfo {
            schema: Self::generate_table_schema(arity),
        };

        self.predicate_to_info
            .insert(predicate.clone(), predicate_info);
        self.predicate_subtables
            .insert(predicate, SubtableHandler::default());
    }

    fn add_subtable(&mut self, subtable: SubtableIdentifier, table_id: TableId) {
        let subtable_handler = self
            .predicate_subtables
            .get_mut(&subtable.predicate)
            .expect("Predicate should be registered before calling this function");

        subtable_handler.add_single_table(subtable.step, table_id);
    }

    /// Add a table that represents the input facts for some predicate for the chase procedure.
    /// Predicate must be registered before calling this function.
    pub fn add_edb(&mut self, predicate: Identifier, sources: Vec<TableSource>) {
        let edb_order = ColumnOrder::default();
        const EDB_STEP: usize = 0;

        let schema = self
            .predicate_to_info
            .get(&predicate)
            .expect("Predicate should be registered before calling this function")
            .schema
            .clone();
        let name = self.generate_table_name(predicate.clone(), &edb_order, EDB_STEP);

        let table_id = self.database.register_table(&name, schema);
        self.database.add_sources(table_id, edb_order, sources);

        self.add_subtable(SubtableIdentifier::new(predicate, EDB_STEP), table_id)
    }

    /// Add a [`Trie`] as a subtable of a predicate.
    /// Predicate must be registered before calling this function.
    pub fn add_table(
        &mut self,
        predicate: Identifier,
        step: usize,
        order: ColumnOrder,
        trie: Trie,
    ) {
        let schema = self
            .predicate_to_info
            .get(&predicate)
            .expect("Predicate should be registered before calling this function")
            .schema
            .clone();
        let name = self.generate_table_name(predicate.clone(), &order, step);

        let table_id = self.database.register_add_trie(&name, schema, order, trie);
        self.add_subtable(SubtableIdentifier::new(predicate, step), table_id);
    }

    /// Add a reference to another table under a new name.
    /// Predicate must be registered before calling this function and referenced table must exist.
    pub fn add_reference(
        &mut self,
        subtable: SubtableIdentifier,
        referenced_subtable: SubtableIdentifier,
        permutation: Permutation,
    ) {
        let referenced_id = self
            .table_id(&referenced_subtable)
            .expect("Referenced table does not exist.");

        let schema = self
            .predicate_to_info
            .get(&subtable.predicate)
            .expect("Predicate should be registered before calling this function")
            .schema
            .clone();
        let name = self.generate_table_name_reference(
            subtable.predicate.clone(),
            subtable.step,
            referenced_id,
            &permutation,
        );

        let table_id = self.database.register_table(&name, schema);
        self.database
            .add_reference(table_id, referenced_id, permutation);

        self.add_subtable(subtable, table_id);
    }

    /// Return the ids of all subtables of a predicate within a certain range of steps.
    pub fn tables_in_range(&self, predicate: Identifier, range: &Range<usize>) -> Vec<TableId> {
        self.predicate_subtables
            .get(&predicate)
            .map(|handler| handler.cover_range(range))
            .unwrap_or_default()
    }

    /// Combine subtables in a certain range into one larger table.
    pub fn combine_tables(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
    ) -> Result<Option<TableId>, Error> {
        let combined_order: ColumnOrder = ColumnOrder::default();

        let tables = self.tables_in_range(predicate.clone(), &range);

        if tables.is_empty() {
            return Ok(None);
        }

        let name = self.generate_table_name_combined(predicate.clone(), &combined_order, &range);

        let mut union_plan = ExecutionPlan::default();
        let fetch_nodes = tables
            .iter()
            .map(|id| union_plan.fetch_existing(*id))
            .collect();
        let union_node = union_plan.union(fetch_nodes);
        let plan_id = union_plan.write_permanent(union_node, "Combinding Tables", &name);

        let execution_result = self.database.execute_plan(union_plan)?;
        let table_id = execution_result
            .get(&plan_id)
            .expect("Combining multiple non-empty tables should result in a non-empty table.");

        self.predicate_subtables
            .get_mut(&predicate)
            .expect("Function should have been left earlier if this is the case.")
            .add_combined_table(&range, *table_id);

        Ok(Some(*table_id))
    }

    /// Execute a plan and add the results as subtables to the manager.
    pub fn execute_plan(
        &mut self,
        subtable_plan: SubtableExecutionPlan,
    ) -> Result<Vec<Identifier>, Error> {
        let result = self.database.execute_plan(subtable_plan.execution_plan)?;

        let mut updated_predicates = Vec::new();
        for (plan_id, table_id) in result {
            let subtable = subtable_plan.map_subtrees.get(&plan_id).unwrap();
            updated_predicates.push(subtable.predicate.clone());

            self.add_subtable(subtable.clone(), table_id);
        }

        Ok(updated_predicates)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, ops::Range};

    use crate::physical::management::database::TableId;

    use super::SubtableHandler;

    fn handler_from_table_steps(
        steps: &[usize],
        ranges: &[Range<usize>],
        expected_ranges: &[Range<usize>],
    ) -> (SubtableHandler, HashSet<TableId>) {
        let expected_set: HashSet<Range<usize>> = expected_ranges.iter().cloned().collect();

        let mut handler = SubtableHandler::default();
        let mut expected_ids = HashSet::<TableId>::new();
        let mut id = TableId::default();

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
        let answer: HashSet<TableId> = handler.cover_range(target).iter().cloned().collect();

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
        let expected_ranges = vec![1..5];
        compare_covering(&steps, &ranges, &expected_ranges, &target);

        let target = 3..5;
        let expected_ranges = vec![4..5];
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
    }
}
