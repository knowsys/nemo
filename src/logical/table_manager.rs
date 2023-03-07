//! Managing of tables

use super::model::Identifier;
use crate::{
    error::Error,
    physical::{
        datatypes::DataTypeName,
        dictionary::Dictionary,
        management::{
            column_order::ColumnOrder,
            database::{TableId, TableSource},
            execution_plan::ExecutionTree,
            DatabaseInstance, ExecutionPlan,
        },
        tabular::{table_types::trie::Trie, traits::table_schema::TableSchema},
        util::Reordering,
    },
};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    ops::Range,
};

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
        (self.start, self.start + self.len)
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

#[derive(Debug)]
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
        if self.single.is_empty() || self.single[0].0 > range.start {
            return result;
        }

        let (target_start, target_end) = self.normalize_range(range).start_end();

        // Algorithm is inspired by:
        // https://www.geeksforgeeks.org/minimum-number-of-intervals-to-cover-the-target-interval

        let mut current_start = target_start;
        let mut current_end = target_start + 1;
        let mut current_id = self.single[current_start].1;

        // Iterate over all the intervals
        for (range, id) in &self.combined {
            let (range_start, range_end) = range.start_end();

            if range_start > target_end {
                break;
            }

            if range_start > current_start {
                result.push(current_id);

                current_start = current_end;
                current_end = current_start + 1;
                current_id = self.single[current_start].1;
            }

            if range_end <= target_end && range_end > current_end {
                current_end = range_end;
                current_id = *id;
            }
        }

        result.push(current_id);

        result
    }
}

impl Default for SubtableHandler {
    fn default() -> Self {
        Self {
            single: Vec::default(),
            combined: Vec::default(),
        }
    }
}

#[derive(Debug)]
struct PredicateInfo {
    schema: TableSchema,
}

/// Identifier of a subtable in a chase sequence.
#[derive(Debug, Copy, Clone)]
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
#[derive(Debug)]
pub struct SubtableExecutionPlan {
    /// The execution plan.
    execution_plan: ExecutionPlan,
    /// Each tree in the plan that will result in a new permanent table
    /// will have an associated [`SubtableIdentifier`].
    map_subtrees: HashMap<usize, SubtableIdentifier>,
}

impl SubtableExecutionPlan {
    /// Add a temporary table to the plan.
    pub fn add_temporary_table(&mut self, tree: ExecutionTree) -> usize {
        self.execution_plan.push(tree)
    }

    /// ADd a permanent table ot the plan-
    pub fn add_permanent_table(
        &mut self,
        tree: ExecutionTree,
        subtable_id: SubtableIdentifier,
    ) -> usize {
        let plan_id = self.execution_plan.push(tree);
        self.map_subtrees.insert(plan_id, subtable_id);

        plan_id
    }
}

impl Default for SubtableExecutionPlan {
    fn default() -> Self {
        Self {
            execution_plan: Default::default(),
            map_subtrees: Default::default(),
        }
    }
}

/// Manager object for handling tables that are the result
/// of a seminaive existential rules evaluation process.
#[derive(Debug)]
pub struct TableManager<Dict: Dictionary> {
    /// [`DatabaseInstance`] managing all existing tables.
    database: DatabaseInstance<Dict>,

    /// Map containg all the ids of all the sub tables associated with a predicate.
    predicate_subtables: HashMap<Identifier, SubtableHandler>,

    /// Mapping predicate identifiers to their arity.
    predicate_to_info: HashMap<Identifier, PredicateInfo>,
}

impl<Dict: Dictionary> TableManager<Dict> {
    /// Create new [`TableManager`].
    pub fn new(dict_constants: Dict) -> Self {
        Self {
            database: DatabaseInstance::new(dict_constants),
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

    /// Return a reference to a [`Trie`] that is associated with the given id.
    /// Uses the default [`ColumnOrder`]
    pub fn table_from_id(&self, id: TableId) -> &Trie {
        self.database.get_trie_inmemory(id, &ColumnOrder::default())
    }

    /// Combine all subtables of a predicate into one table
    /// and return the [`TableId`] of that new table.
    pub fn combine_predicate(&mut self, predicate: Identifier) -> Result<TableId, Error> {
        let last_step = self
            .last_step(predicate)
            .expect("Function assumed that predicate has at least one subtable.");
        let combined_id = self
            .combine_tables(predicate, 0..last_step)?
            .expect("Function assumed that predicate has at least one subtable.");

        Ok(combined_id)
    }

    /// Generates an appropriate table name for subtable.
    pub fn generate_table_name(
        &self,
        predicate: Identifier,
        order: &ColumnOrder,
        step: usize,
    ) -> String {
        let predicate_name = self
            .database
            .get_dict_constants()
            .entry(predicate.0)
            .unwrap();
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
        let predicate_name = self
            .database
            .get_dict_constants()
            .entry(predicate.0)
            .unwrap();
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
        reorder: &Reordering,
    ) -> String {
        let predicate_name = self
            .database
            .get_dict_constants()
            .entry(predicate.0)
            .unwrap();
        let referenced_table_name = self.database.table_name(referenced_table_id);

        format!("{predicate_name} ({step}) -> {referenced_table_name} {reorder:?}")
    }

    fn generate_table_schema(arity: usize) -> TableSchema {
        let mut schema = TableSchema::new();
        (0..arity).for_each(|_| schema.add_entry(DataTypeName::U64, false, false));

        schema
    }

    /// Sets information
    /// Must be done before
    pub fn register_predicate(&mut self, predicate: Identifier, arity: usize) {
        // TODO: Change this once type system is integrated
        let predicate_info = PredicateInfo {
            schema: Self::generate_table_schema(arity),
        };

        self.predicate_to_info.insert(predicate, predicate_info);
    }

    fn add_subtable(&mut self, subtable: SubtableIdentifier, table_id: TableId) {
        let subtable_handler = match self.predicate_subtables.entry(subtable.predicate) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => vacant.insert(SubtableHandler::default()),
        };

        subtable_handler.add_single_table(subtable.step, table_id);
    }

    /// Add a table that represents the input facts for some predicate for the chase procedure.
    /// This function also registers the new predicate.
    pub fn add_edb(&mut self, predicate: Identifier, sources: Vec<TableSource>) {
        let edb_order = ColumnOrder::default();
        const EDB_STEP: usize = 0;

        let schema = self
            .predicate_to_info
            .get(&predicate)
            .expect("Predicate should be registered before callong this function")
            .schema
            .clone();
        let name = self.generate_table_name(predicate, &edb_order, EDB_STEP);

        let table_id = self.database.register_table(&name, schema);
        self.database.add_sources(table_id, edb_order, sources);

        self.add_subtable(SubtableIdentifier::new(predicate, EDB_STEP), table_id)
    }

    /// Add a [`Trie`].
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
            .expect("Predicate should be registered before callong this function")
            .schema
            .clone();
        let name = self.generate_table_name(predicate, &order, step);

        let table_id = self.database.register_add_trie(&name, schema, order, trie);
        self.add_subtable(SubtableIdentifier::new(predicate, step), table_id);
    }

    /// Add a reference to another table under a new name.
    /// This function will register the new table.
    pub fn add_reference(
        &mut self,
        subtable: SubtableIdentifier,
        referenced_subtable: SubtableIdentifier,
        reorder: Reordering,
    ) {
        debug_assert!(reorder.is_permutation());

        let referenced_id = self
            .table_id(&referenced_subtable)
            .expect("Referenced table does not exist.");

        let schema = self
            .predicate_to_info
            .get(&subtable.predicate)
            .expect("Predicate should be registered before callong this function")
            .schema
            .clone();
        let name = self.generate_table_name_reference(
            subtable.predicate,
            subtable.step,
            referenced_id,
            &reorder,
        );

        let table_id = self.database.register_table(&name, schema);
        self.database
            .add_reference(table_id, referenced_id, reorder);

        self.add_subtable(subtable, table_id);
    }

    /// Return the ids of all subtables of a predicate within a certain range of steps.
    pub fn tables_in_range(&self, predicate: Identifier, range: &Range<usize>) -> Vec<TableId> {
        let subtable_handler = self
            .predicate_subtables
            .get(&predicate)
            .expect("Predicate should be registered before callong this function");
        subtable_handler.cover_range(range)
    }

    /// Combine subtables in a certain range into one larger table.
    pub fn combine_tables(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
    ) -> Result<Option<TableId>, Error> {
        let combined_order: ColumnOrder = ColumnOrder::default();

        let tables = self.tables_in_range(predicate, &range);

        if tables.len() <= 1 {
            return Ok(tables.first().cloned());
        }

        let name = self.generate_table_name_combined(predicate, &combined_order, &range);

        let mut plan = ExecutionPlan::new();
        let mut union_tree = ExecutionTree::new_permanent("Defragmentaion", &name);
        let fetch_nodes = tables
            .iter()
            .map(|id| union_tree.fetch_existing(*id))
            .collect();
        let union_node = union_tree.union(fetch_nodes);
        union_tree.set_root(union_node);

        let plan_id = plan.push(union_tree);
        let execution_result = self.database.execute_plan(plan)?;
        let table_id = execution_result.get(&plan_id).unwrap();

        self.predicate_subtables
            .get_mut(&predicate)
            .expect("Predicate should be registered before callong this function")
            .add_combined_table(&range, *table_id);

        Ok(Some(*table_id))
    }

    /// Execute a plan and add the results as subtables to the manager.
    pub fn execute_plan(
        &mut self,
        mut subtable_plan: SubtableExecutionPlan,
    ) -> Result<Vec<Identifier>, Error> {
        subtable_plan.execution_plan.simplify();
        let result = self.database.execute_plan(subtable_plan.execution_plan)?;

        let mut updated_predicates = Vec::new();
        for (plan_id, table_id) in result {
            let subtable = subtable_plan.map_subtrees.get(&plan_id).unwrap();
            updated_predicates.push(subtable.predicate);

            self.add_subtable(*subtable, table_id);
        }

        Ok(updated_predicates)
    }

    /// Return the dictionary used in the database instance.
    /// TODO: Remove this once proper Dictionary support is implemented in the physical layer.
    pub fn get_dict(&self) -> &Dict {
        self.database.get_dict_constants()
    }
}
