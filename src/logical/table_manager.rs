//! Managing of tables

use csv::ReaderBuilder;

use super::model::{DataSource, Identifier};
use crate::{
    io::csv::read,
    physical::{
        datatypes::DataTypeName,
        dictionary::PrefixedStringDictionary,
        management::{
            database::TableKeyType,
            execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult, ExecutionTree},
            DatabaseInstance, ExecutionPlan,
        },
        tabular::{
            operations::triescan_project::ColumnPermutation,
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
        util::cover_interval,
    },
};
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap},
    fs::File,
    hash::Hash,
    ops::Range,
};

/// Type which represents a permutation of columns in a table
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ColumnOrder(pub Vec<usize>);

impl ColumnOrder {
    /// Create new [`ColumnOrder`].
    pub fn new(permutation: Vec<usize>) -> Self {
        debug_assert!(permutation.iter().all(|&p| p < permutation.len()));

        Self(permutation)
    }

    /// Return the default [`ColumnOrder`]
    pub fn default(arity: usize) -> Self {
        Self((0..arity).collect())
    }

    /// Derive a column order from
    pub fn from_transformation<T: PartialEq>(source: &[T], target: &[T]) -> Self {
        Self(
            target
                .iter()
                .map(|t| {
                    source
                        .iter()
                        .position(|s| *s == *t)
                        .expect("We expect that target only uses elements from source.")
                })
                .collect(),
        )
    }

    /// Returns the amount of columns left.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return an iterator over the contents of this order.
    pub fn iter(&self) -> impl Iterator<Item = &usize> {
        self.0.iter()
    }

    /// Return true if this is the default order
    pub fn is_default(&self) -> bool {
        for index in 1..self.len() {
            if let Some(difference) = self.0[index].checked_sub(self.0[index - 1]) {
                if difference != 1 {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    /// Concatenate two [`ColumnOrder`].
    /// For example [1, 0, 2].concat([2, 0, 1]) = [2, 1, 0]
    pub fn concat(&self, other: &Self) -> Self {
        debug_assert!(other.len() >= self.len());

        let result = other.0.iter().map(|&o| self.0[o]).collect::<Vec<usize>>();
        Self(result)
    }

    /// Returns the permutation necessary to transform the current order into the target one.
    pub fn reorder_to(&self, target: &Self) -> Self {
        debug_assert!(self.len() == target.len());

        let result = self.0.iter().map(|&s| target.0.iter().position(|&o| o == s).expect("If both objects are well-formed and of equal-length then they contain the same values.")).collect::<Vec<usize>>();
        Self(result)
    }

    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, self = [2, 1, 0] and other = [1, 0, 2].
    /// Starting from position 0 one needs to skip one layer to reach the 2 in other (+1).
    /// Then we need to go back two layers to reach the one (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if self == other.
    pub fn distance(&self, other: &Self) -> usize {
        debug_assert!(other.len() >= self.len());

        let mut current_score: usize = 0;
        let mut current_position: usize = 0;

        for &current_value in &self.0 {
            let position_other = other.0.iter().position(|&o| o == current_value).expect(
                "If both objects are well-formed then other must contain every value of self.",
            );

            let difference: isize = (position_other as isize) - (current_position as isize);
            let penalty: usize = if difference <= 0 {
                difference.abs() as usize
            } else {
                // Taking one forward step should not be punished
                (difference - 1) as usize
            };

            current_score += penalty;
            current_position = position_other;
        }

        current_score
    }
}

impl From<ColumnOrder> for ColumnPermutation {
    fn from(order: ColumnOrder) -> ColumnPermutation {
        order.0
    }
}

/// Indicates that the table contains the union of successive tables.
/// For example assume that for predicate p there were tables derived in steps 2, 4, 7, 10, 11.
/// The range [4, 11) would be represented with TableCover { start: 1, len: 3 }.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub struct TableCover {
    start: usize,
    len: usize,
}

impl Ord for TableCover {
    fn cmp(&self, other: &Self) -> Ordering {
        let start_cmp = self.start.cmp(&other.start);
        if let Ordering::Equal = start_cmp {
            self.len.cmp(&other.len)
        } else {
            start_cmp
        }
    }
}

impl PartialOrd for TableCover {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Indicates which step or steps a given table covers.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub enum TableRange {
    /// Table covers a single step.
    Single(usize),
    /// Table is the union of multiple tables.
    Multiple(TableCover),
}

/// Name of a table.
/// Consists of its predicate name and a range of steps.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub struct TableName {
    /// Number associated with a predicate which corresponds to a table on user level.
    pub predicate: Identifier,
    /// Ranges of execution steps this table covers.
    pub range: TableRange,
}

impl TableName {
    /// Create new [`TableName`].
    pub fn new(predicate: Identifier, range: TableRange) -> Self {
        Self { predicate, range }
    }
}

/// Properties which identify a table.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct TableKey {
    /// Name of the table.
    pub name: TableName,
    /// Order of the columns.
    pub order: ColumnOrder,
}
impl TableKeyType for TableKey {}

impl TableKey {
    /// Create new [`TableKey`].
    pub fn new(predicate: Identifier, range: TableRange, order: ColumnOrder) -> Self {
        Self {
            name: TableName::new(predicate, range),
            order,
        }
    }

    /// Create new [`TableKey`] given a [`TableName`].
    pub fn from_name(name: TableName, order: ColumnOrder) -> Self {
        Self { name, order }
    }
}

#[derive(Debug)]
enum TableStatus {
    /// Table is materialized in memory in given following orders.
    InMemory(Vec<ColumnOrder>),
    /// Table has to be loaded from disk.
    OnDisk(DataSource),
    /// Table has the same contents as another table except for reordering.
    /// Meaning that "ThisTable = Reorder(ReferencedTable, Reordering)"
    Reference(TableName, ColumnOrder),
}

/// Manager object for handling tables that are the result
/// of a seminaive existential rules evaluation process.
#[derive(Debug)]
pub struct TableManager {
    /// [`DatabaseInstance`] managing all existing tables.
    database: DatabaseInstance<TableKey>,

    /// Contains the status of each table.
    status: HashMap<TableName, TableStatus>,

    /// The arity associated with each predicate
    predicate_arity: HashMap<Identifier, usize>,

    /// Hashmap containing for each predicate on which
    /// execution steps the associated tables have been derived.
    /// Vector is assumed to be sorted.
    predicate_to_steps: HashMap<Identifier, Vec<usize>>,
    /// Hashmap containing for each predicate which ranges are covered by the predicate's tables.
    /// Vector is assumed to be sorted.
    predicate_to_covers: HashMap<Identifier, Vec<TableCover>>,
}

impl TableManager {
    /// Create new [`TableManager`].
    pub fn new() -> Self {
        Self {
            database: DatabaseInstance::new(),
            status: HashMap::new(),
            predicate_arity: HashMap::new(),
            predicate_to_steps: HashMap::new(),
            predicate_to_covers: HashMap::new(),
        }
    }

    /// A table may either be created as a result of a rule application at some step
    /// or may represent a union of many previously computed tables.
    /// Say, we have for a predicate p calculated tables at steps steps 2, 4, 7, 10, 11.
    /// On the outside, we might now refer to all tables between steps 3 and 11 (exclusive).
    /// Representing this as [3, 11) is ambigious as [4, 11) refers to the same three tables.
    /// Hence, we translate both representations to TableCover { start: 1, len: 3}.
    ///
    /// This function performs the translation illustrated above.
    fn normalize_range(&self, predicate: Identifier, range: &Range<usize>) -> TableCover {
        let mut start: usize = 0;
        let mut len: usize = 0;

        let steps = self
            .predicate_to_steps
            .get(&predicate)
            .expect("Function assumes that predicate exist.");
        let mut start_set = false;
        for (index, step) in steps.iter().enumerate() {
            if !start_set && *step >= range.start {
                start = index;
                start_set = true
            }

            if start_set && *step >= range.end {
                break;
            }

            if start_set {
                len += 1;
            }
        }

        TableCover { start, len }
    }

    /// Return all a list of all column orders associated with a table name.
    pub fn get_table_orders(&self, name: &TableName) -> Vec<&ColumnOrder> {
        let status_opt = self.status.get(name);

        if let Some(status) = status_opt {
            if let TableStatus::InMemory(orders) = status {
                orders.iter().map(|o| o).collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Returns whether there is at least one (non-empty) table associated with the given predicate.
    pub fn contains_predicate(&self, predicate: Identifier) -> bool {
        self.predicate_to_steps.contains_key(&predicate)
    }

    /// Given a predicate and a range, return the corresponding [`TableName`]
    pub fn get_table_name(&self, predicate: Identifier, range: Range<usize>) -> TableName {
        if let Some(steps) = self.predicate_to_steps.get(&predicate) {
            debug_assert!(!steps.is_empty());
            debug_assert!(steps.is_sorted());

            if range.end > *steps.last().unwrap() {
                debug_assert!(range.len() == 1);
                TableName::new(predicate, TableRange::Single(range.start))
            } else {
                TableName::new(
                    predicate,
                    TableRange::Multiple(self.normalize_range(predicate, &range)),
                )
            }
        } else {
            debug_assert!(range.len() == 1);
            TableName::new(predicate, TableRange::Single(range.start))
        }
    }

    /// Update all the auxillary structures for a new table name.
    fn register_name(&mut self, name: TableName, arity: usize) {
        let prev_arity = self.predicate_arity.entry(name.predicate).or_insert(arity);
        debug_assert!(*prev_arity == arity);

        let cover = match name.range {
            TableRange::Single(step) => {
                let steps = self
                    .predicate_to_steps
                    .entry(name.predicate)
                    .or_insert(Vec::new());

                steps.push(step);

                TableCover {
                    start: steps.len() - 1,
                    len: 1,
                }
            }
            TableRange::Multiple(cover) => cover,
        };

        let covers = self
            .predicate_to_covers
            .entry(name.predicate)
            .or_insert(Vec::new());
        covers.push(cover);
        covers.sort();
    }

    /// Update all auxillary structures for a new in-memory table.
    fn register_table(&mut self, key: TableKey) {
        let arity = key.order.len();

        let register = match self.status.entry(key.name) {
            Entry::Occupied(mut entry) => {
                if let TableStatus::InMemory(orders) = entry.get_mut() {
                    orders.push(key.order);
                } else {
                    panic!("You can only add to tables that are available in memory.");
                }

                false
            }
            Entry::Vacant(vacant) => {
                vacant.insert(TableStatus::InMemory(vec![key.order]));

                true
            }
        };

        if register {
            self.register_name(key.name, arity);
        }
    }

    /// Add input table that is currently stored on disk.
    pub fn add_source(
        &mut self,
        predicate: Identifier,
        arity: usize,
        source: DataSource,
    ) -> TableName {
        const EDB_RANGE: Range<usize> = 0..1;
        let table_name = self.get_table_name(predicate, EDB_RANGE);

        self.status
            .insert(table_name.clone(), TableStatus::OnDisk(source));

        self.register_name(table_name.clone(), arity);

        table_name
    }

    /// Add a [`Trie`].
    pub fn add_table(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
        order: ColumnOrder,
        schema: TableSchema,
        table: Trie,
    ) -> TableKey {
        let table_key = TableKey {
            name: self.get_table_name(predicate, range),
            order,
        };

        self.register_table(table_key.clone());
        self.database.add(table_key.clone(), table, schema);

        table_key
    }

    /// Add a reference to another table under a new name.
    /// If it is another reference then it will point to the referenced table of that.
    pub fn add_reference(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
        ref_name: TableName,
        ref_reorder: ColumnOrder,
    ) -> TableName {
        let new_name = self.get_table_name(predicate, range);

        debug_assert!(ref_reorder.len() == *self.predicate_arity.get(&new_name.predicate).unwrap());

        let arity = ref_reorder.len();

        let (overall_name, overall_reorder) =
            if let TableStatus::Reference(another_table, another_order) = self
                .status
                .get(&ref_name)
                .expect("Funcion assumes that referenced table exists.")
            {
                let combined_reorder = another_order.concat(&ref_reorder);

                (another_table.clone(), combined_reorder)
            } else {
                (ref_name, ref_reorder)
            };

        self.register_name(new_name.clone(), arity);
        self.status.insert(
            new_name,
            TableStatus::Reference(overall_name, overall_reorder),
        );

        new_name
    }

    /// Compute a table that contains the contents of the tables in the given range.
    pub fn add_union_table(
        &mut self,
        input_predicate: Identifier,
        input_ranges: Range<usize>,
        output_predicate: Identifier,
        output_range: Range<usize>,
        output_order_opt: Option<ColumnOrder>,
    ) -> Option<TableKey> {
        let input_covering = self.get_table_covering(input_predicate, input_ranges);
        let input_arity = *self.predicate_arity.get(&input_predicate)?;

        if input_covering.is_empty() {
            return None;
        }

        let input_orders =
            self.get_table_orders(&TableName::new(input_predicate, input_covering[0]));

        let input_order = if input_orders.is_empty() {
            // This occurs when input table is just a reference
            ColumnOrder::default(input_arity)
        } else {
            input_orders[0].clone()
        };

        let output_order = if let Some(order) = output_order_opt {
            order
        } else {
            input_order.clone()
        };

        let output_name = self.get_table_name(output_predicate, output_range);
        let output_key = TableKey::from_name(output_name, output_order.clone());
        let mut output_tree =
            ExecutionTree::<TableKey>::new(ExecutionResult::Save(output_key.clone()));

        let fetch_nodes: Vec<ExecutionNodeRef<TableKey>> = input_covering
            .into_iter()
            .map(|r| {
                let key = TableKey::new(input_predicate, r, input_order.clone());
                output_tree.fetch_table(key)
            })
            .collect();
        let union_node = output_tree.union(fetch_nodes);
        output_tree.set_root(union_node);

        let mut execution_plan = ExecutionPlan::<TableKey>::new();
        execution_plan.push(output_tree);

        self.execute_plan(execution_plan);
        Some(output_key)
    }

    /// Load table from a given on-disk source
    /// TODO: This function should change when the type system gets introduced on the logical layer
    fn load_table(source: &DataSource, arity: usize, dict: &mut PrefixedStringDictionary) -> Trie {
        let (trie, _name) = match source {
            DataSource::CsvFile(file) => {
                // Using fallback solution to treat eveything as string for now (storing as u64 internally)
                let datatypes: Vec<Option<DataTypeName>> = (0..arity).map(|_| None).collect();

                let mut reader = ReaderBuilder::new()
                    .delimiter(b',')
                    .has_headers(false)
                    .from_reader(File::open(file.as_path()).unwrap());

                let col_table = read(&datatypes, &mut reader, dict).unwrap();

                let trie = Trie::from_cols(col_table);
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

        trie
    }

    // Basically, makes sure that the given [`TableKey`] will exist in the [`DatabaseInstance`]
    // or alternatively, return a key that does and will contain the same entries as the requested table.
    fn materialize_table(&mut self, key: TableKey) -> TableKey {
        let (actual_name, reordering) = if let TableStatus::Reference(referenced_name, reordering) =
            &self
                .status
                .get(&key.name)
                .expect("Function assumes that table is known.")
        {
            (referenced_name.clone(), reordering.clone())
        } else {
            (
                key.name.clone(),
                ColumnOrder::default(
                    *self
                        .predicate_arity
                        .get(&key.name.predicate)
                        .expect("Function assumes that predicate is not new."),
                ),
            )
        };

        let required_order = reordering.concat(&key.order);
        let result_key = TableKey {
            name: actual_name,
            order: required_order.clone(),
        };

        if let Entry::Occupied(mut entry) = self.status.entry(actual_name) {
            let status = entry.get_mut();

            let reordered_table_opt = match status {
                TableStatus::InMemory(orders) => {
                    let mut closest_order = &orders[0];
                    let mut distance = usize::MAX;

                    for order in orders.iter() {
                        let current_distance = order.distance(&required_order);

                        if current_distance < distance {
                            closest_order = order;
                            distance = current_distance;
                        }
                    }

                    if distance > 0 {
                        Some(TableKey {
                            name: actual_name.clone(),
                            order: closest_order.clone(),
                        })
                    } else {
                        // The required order is already present
                        None
                    }
                }
                TableStatus::OnDisk(source) => {
                    let arity = required_order.len();

                    // Trie has to be loaded from disk
                    let trie =
                        Self::load_table(&source, arity, self.database.get_dict_constants_mut());

                    // We deduce the schema from the trie itself here
                    // TODO: Should change when type system is introduced in the logical layer
                    let mut schema = TableSchema::new();
                    for &type_name in trie.get_types() {
                        schema.add_entry(type_name, false, false);
                    }

                    let loaded_table_key = TableKey {
                        name: actual_name.clone(),
                        order: ColumnOrder::default(arity),
                    };

                    self.database.add(loaded_table_key.clone(), trie, schema);
                    *status = TableStatus::InMemory(vec![loaded_table_key.order.clone()]);

                    if !required_order.is_default() {
                        Some(loaded_table_key)
                    } else {
                        // If the default order is required then we are in luck
                        None
                    }
                }
                TableStatus::Reference(_, _) => unreachable!(),
            };

            if let Some(reordered_table) = reordered_table_opt {
                // Construct new [`ÈxecutionPlan`] for the reordering
                let projection_reordering = reordered_table.order.reorder_to(&required_order);

                let mut project_tree =
                    ExecutionTree::<TableKey>::new(ExecutionResult::Save(result_key.clone()));
                let reordered_node = project_tree.fetch_table(reordered_table);
                let project_node =
                    project_tree.project(reordered_node, projection_reordering.into());
                project_tree.set_root(project_node);

                let mut reorder_plan = ExecutionPlan::<TableKey>::new();
                reorder_plan.push(project_tree);

                self.execute_plan(reorder_plan);
            }
        } else {
            panic!("Function assumes that ")
        }

        result_key
    }

    /// Execute a given [`ExecutionPlan`].
    pub fn execute_plan(&mut self, mut plan: ExecutionPlan<TableKey>) -> bool {
        // TODO: Simplify
        // TODO: Check for and remove duplicate tables

        // First, we need to make sure that every requested table is available
        // or produce it if necessary
        for tree in &mut plan.trees {
            for fetch_node in tree.all_fetched_tables() {
                let node_unpacked = &mut *fetch_node.0.borrow_mut();
                if let ExecutionNode::FetchTable(key) = node_unpacked {
                    *key = self.materialize_table(key.clone());
                } else {
                    unreachable!()
                }
            }
        }

        match self.database.execute_plan(&plan) {
            Ok(new_tables) => {
                let is_empty = new_tables.is_empty();

                for key in new_tables {
                    self.register_table(key);
                }

                !is_empty
            }
            Err(err) => {
                log::warn!("{err:?}");
                false
            }
        }
    }

    /// Given a range of step return a list of [`TableRange`] that would cover it
    /// using tables that are available for the given predicate.
    pub fn get_table_covering(
        &self,
        predicate: Identifier,
        steps: Range<usize>,
    ) -> Vec<TableRange> {
        let ranges = if let Some(ranges) = self.predicate_to_covers.get(&predicate) {
            ranges
        } else {
            return Vec::new();
        };

        debug_assert!(ranges.is_sorted());

        let table_steps = self
            .predicate_to_steps
            .get(&predicate)
            .expect("If the above map contains the predicate then this one should too.");

        let ranges_transformed: Vec<Range<usize>> =
            ranges.iter().map(|r| r.start..(r.start + r.len)).collect();
        let covering = cover_interval(&ranges_transformed, &steps);

        // We assume here that every length = 1 covering here corresponds to a non-union table
        covering
            .iter()
            .map(|&c| {
                if c.len() == 1 {
                    TableRange::Single(table_steps[c.start])
                } else {
                    TableRange::Multiple(TableCover {
                        start: c.start,
                        len: c.end - c.start,
                    })
                }
            })
            .collect()
    }

    /// Get all the [`TableRange`] that would cover every element of a given predicate's table.
    pub fn cover_whole_table(&self, predicate: Identifier) -> Vec<TableRange> {
        if let Some(steps) = self.predicate_to_steps.get(&predicate) {
            let last_step = steps.last().expect("There is never empty.");

            self.get_table_covering(predicate, 0..(last_step + 1))
        } else {
            Vec::new()
        }
    }

    /// Combine all tables associated with a given predicate into a materialized table in default variable order
    pub fn combine_predicate(&mut self, predicate: Identifier, step: usize) -> Option<TableKey> {
        let arity = *self.predicate_arity.get(&predicate)?;
        let key = self.add_union_table(
            predicate,
            0..step,
            predicate,
            0..step,
            Some(ColumnOrder::default(arity)),
        )?;

        Some(self.materialize_table(key))
    }

    /// Return trie with the given key.
    pub fn get_trie<'a>(&'a self, key: &TableKey) -> &'a Trie {
        self.database.get_by_key(key)
    }

    /// Return the dictionary used in the database instance.
    /// TODO: Remove this once proper Dictionary support is implemented on the physical layer.
    pub fn get_dict(&self) -> &PrefixedStringDictionary {
        self.database.get_dict_constants()
    }
}
