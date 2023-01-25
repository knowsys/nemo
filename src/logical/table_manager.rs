//! Managing of tables

use super::model::{DataSource, Identifier};
use crate::{
    error::Error,
    io::csv::read,
    meta::logging::{log_add_reference, log_load_table},
    physical::{
        datatypes::DataTypeName,
        dictionary::PrefixedStringDictionary,
        management::{
            database::{TableId, TableKeyType},
            execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult, ExecutionTree},
            DatabaseInstance, ExecutionPlan,
        },
        tabular::{
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
        util::{cover_interval, Reordering},
    },
};
use csv::ReaderBuilder;
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt,
    fs::File,
    hash::Hash,
    ops::{Index, Range},
};

/// Type which represents the order of a column.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ColumnOrder(Vec<usize>);

impl ColumnOrder {
    /// Construct new [`ColumnOrder`].
    pub fn new(order: Vec<usize>) -> Self {
        debug_assert!(!order.is_empty());
        debug_assert!(order.iter().all(|&r| r < order.len()));

        Self(order)
    }

    /// Construct the default [`ColumnOrder`].
    pub fn default(arity: usize) -> Self {
        Self::new((0..arity).collect())
    }

    /// Return the arity of the reordered table.
    pub fn arity(&self) -> usize {
        self.0.len()
    }

    /// Return an iterator over the contents of this order.
    pub fn iter(&self) -> impl Iterator<Item = &usize> {
        self.0.iter()
    }

    /// Returns whether this is the default column order
    pub fn is_default(&self) -> bool {
        self.iter()
            .enumerate()
            .all(|(index, element)| index == *element)
    }

    /// Returns a view into ordering vector.
    pub fn as_slice(&self) -> &[usize] {
        &self.0
    }

    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, self = [2, 1, 0] and other = [1, 0, 2].
    /// Starting from position 0 one needs to skip one layer to reach the 2 in other (+1).
    /// Then we need to go back two layers to reach the one (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if self == other.
    fn distance(&self, to: &ColumnOrder) -> usize {
        debug_assert!(to.arity() >= self.arity());

        let mut current_score: usize = 0;
        let mut current_position: usize = 0;

        for &current_value in self.iter() {
            let position_other = to.iter().position(|&o| o == current_value).expect(
                "If both objects are well-formed then other must contain every value of self.",
            );

            let difference: isize = (position_other as isize) - (current_position as isize);
            let penalty: usize = if difference <= 0 {
                difference.unsigned_abs()
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

impl Index<usize> for ColumnOrder {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl fmt::Debug for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Reordering> for ColumnOrder {
    fn from(reordering: Reordering) -> Self {
        ColumnOrder::new(reordering.iter().copied().collect())
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
    /// Meaning that "ThisTable = Project(ReferencedTable, Reordering)"
    Reference(TableName, Reordering),
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
    pub fn new(dict_constants: PrefixedStringDictionary) -> Self {
        Self {
            database: DatabaseInstance::new(dict_constants),
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

    /// Given [`TableRange`], returns the range of steps that is covered.
    /// I.e. this undoes the function `normalize_range`.
    pub fn translate_range(&self, predicate: Identifier, range: TableRange) -> Range<usize> {
        match range {
            TableRange::Single(step) => step..(step + 1),
            TableRange::Multiple(cover) => {
                let steps = self
                    .predicate_to_steps
                    .get(&predicate)
                    .expect("If the range is given as a cover then it must be known");
                let start = steps[cover.start];
                let end = steps[cover.start + cover.len];

                start..(end + 1)
            }
        }
    }

    /// Return all a list of all column orders associated with a table name.
    pub fn get_table_orders(&self, name: &TableName) -> Vec<&ColumnOrder> {
        if let Some(TableStatus::InMemory(orders)) = self.status.get(name) {
            orders.iter().collect()
        } else {
            Vec::new()
        }
    }

    /// Return the last step a new table for a predicate has been derived or None if predicate is unknown.
    pub fn predicate_last_step(&self, predicate: Identifier) -> Option<usize> {
        let steps = self.predicate_to_steps.get(&predicate)?;

        steps.last().copied()
    }

    /// Returns whether there is at least one (non-empty) table associated with the given predicate.
    pub fn contains_predicate(&self, predicate: Identifier) -> bool {
        self.predicate_to_steps.contains_key(&predicate)
    }

    /// Returns whether there is at least one (non-empty) table with the given key.
    pub fn contains_key(&self, key: &TableKey) -> bool {
        if let Some(status) = self.status.get(&key.name) {
            match status {
                TableStatus::InMemory(orders) => {
                    for order in orders {
                        if *order == key.order {
                            return true;
                        }
                    }

                    false
                }
                TableStatus::OnDisk(_) => true,
                TableStatus::Reference(_, _) => true,
            }
        } else {
            false
        }
    }

    /// Given a predicate and a range, return the corresponding [`TableName`]
    pub fn get_table_name(&self, predicate: Identifier, range: Range<usize>) -> TableName {
        if let Some(steps) = self.predicate_to_steps.get(&predicate) {
            debug_assert!(!steps.is_empty());
            debug_assert!(steps.is_sorted());

            if range.start > *steps.last().unwrap() {
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
        let arity = key.order.arity();

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

        self.status.insert(table_name, TableStatus::OnDisk(source));

        self.register_name(table_name, arity);

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
    /// We only allow reorderings that are permutations.
    pub fn add_reference(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
        ref_name: TableName,
        ref_reorder: Reordering,
    ) -> TableName {
        debug_assert!(ref_reorder.is_permutation());

        log_add_reference(predicate, ref_name.predicate, &ref_reorder);

        debug_assert!(
            ref_reorder.len_source() == *self.predicate_arity.get(&ref_name.predicate).unwrap()
        );

        let new_name = self.get_table_name(predicate, range);
        let arity = ref_reorder.len_target();

        let (overall_name, overall_reorder) =
            if let TableStatus::Reference(another_table, another_order) = self
                .status
                .get(&ref_name)
                .expect("Funcion assumes that referenced table exists.")
            {
                let combined_reorder = another_order.chain(&ref_reorder);

                (*another_table, combined_reorder)
            } else {
                (ref_name, ref_reorder)
            };

        self.register_name(new_name, arity);
        self.status.insert(
            new_name,
            TableStatus::Reference(overall_name, overall_reorder),
        );

        new_name
    }

    /// Compute a table that contains the contents of the tables in the given range.
    /// Returns an [`Ok(None)`] on non usable tables.
    pub fn add_union_table(
        &mut self,
        input_predicate: Identifier,
        input_ranges: Range<usize>,
        output_predicate: Identifier,
        output_range: Range<usize>,
        output_order_opt: Option<ColumnOrder>,
    ) -> Result<Option<TableKey>, Error> {
        let input_covering = self.get_table_covering(input_predicate, input_ranges);
        let input_arity = match self.predicate_arity.get(&input_predicate) {
            Some(val) => *val,
            None => {
                return Ok(None);
            }
        };

        if input_covering.is_empty() {
            return Ok(None);
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
        let output_key = TableKey::from_name(output_name, output_order);
        let mut output_tree = ExecutionTree::<TableKey>::new(
            "Merge Tables".to_string(),
            ExecutionResult::Save(output_key.clone()),
        );

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

        self.execute_plan(execution_plan)?;
        Ok(Some(output_key))
    }

    /// Load table from a given on-disk source
    /// TODO: This function should change when the type system gets introduced on the logical layer
    fn load_table(
        source: &DataSource,
        arity: usize,
        dict: &mut PrefixedStringDictionary,
    ) -> Result<Trie, Error> {
        log_load_table(source);

        let (trie, _name) = match source {
            DataSource::CsvFile(file) => {
                // Using fallback solution to treat eveything as string for now (storing as u64 internally)
                let datatypes: Vec<Option<DataTypeName>> = (0..arity).map(|_| None).collect();

                let mut reader = ReaderBuilder::new()
                    .delimiter(b',')
                    .escape(Some(b'\\'))
                    .has_headers(false)
                    .double_quote(true)
                    .from_reader(File::open(file.as_path())?);

                let col_table = read(&datatypes, &mut reader, dict)?;

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

        Ok(trie)
    }

    // Basically, makes sure that the given [`TableKey`] will exist in the [`DatabaseInstance`]
    // or alternatively, return a key that does and will contain the same entries as the requested table.
    fn materialize_table(&mut self, key: TableKey) -> Result<TableKey, Error> {
        let (actual_name, reordering) = if let TableStatus::Reference(referenced_name, reordering) =
            &self
                .status
                .get(&key.name)
                .expect("Function assumes that table is known.")
        {
            (*referenced_name, reordering.clone())
        } else {
            (
                key.name,
                Reordering::default(
                    *self
                        .predicate_arity
                        .get(&key.name.predicate)
                        .expect("Function assumes that predicate is not new."),
                ),
            )
        };

        let required_order = ColumnOrder::new(reordering.inverse().apply_to(key.order.as_slice()));
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
                            name: actual_name,
                            order: closest_order.clone(),
                        })
                    } else {
                        // The required order is already present
                        None
                    }
                }
                TableStatus::OnDisk(source) => {
                    let arity = required_order.arity();

                    // Trie has to be loaded from disk
                    let trie =
                        Self::load_table(source, arity, self.database.get_dict_constants_mut())?;

                    // We deduce the schema from the trie itself here assuming they are all dictionary entries
                    // TODO: Should change when type system is introduced in the logical layer
                    let mut schema = TableSchema::new();
                    for &type_name in trie.get_types() {
                        schema.add_entry(type_name, true, false);
                    }

                    let loaded_table_key = TableKey {
                        name: actual_name,
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
                // Construct new [`ExecutionPlan`] for the reordering
                let projection_reordering = Reordering::from_transformation(
                    reordered_table.order.as_slice(),
                    required_order.as_slice(),
                );

                let mut project_tree = ExecutionTree::<TableKey>::new(
                    "Required Reorder".to_string(),
                    ExecutionResult::Save(result_key.clone()),
                );
                let reordered_node = project_tree.fetch_table(reordered_table);
                let project_node = project_tree.project(reordered_node, projection_reordering);
                project_tree.set_root(project_node);

                let mut reorder_plan = ExecutionPlan::<TableKey>::new();
                reorder_plan.push(project_tree);

                self.execute_plan(reorder_plan)?;
            }
        } else {
            panic!("Function assumes that materialized table is known.");
        }

        Ok(result_key)
    }

    fn execute_plan(
        &mut self,
        mut plan: ExecutionPlan<TableKey>,
    ) -> Result<HashSet<Identifier>, Error> {
        // First, we need to make sure that every requested table is available or produce it if necessary
        // We exclude from this tables that will be produced during the execution of the plan
        let mut produced_keys = HashSet::<TableKey>::new();
        for tree in &mut plan.trees {
            for fetch_node in tree.all_fetched_tables() {
                let node_unpacked = &mut *fetch_node.0.borrow_mut();
                if let ExecutionNode::FetchTable(key) = node_unpacked {
                    if !produced_keys.contains(key) {
                        *key = self.materialize_table(key.clone())?;
                    }
                } else {
                    unreachable!()
                }
            }

            if let ExecutionResult::Save(new_key) = tree.result() {
                produced_keys.insert(new_key.clone());
            }
        }

        match self.database.execute_plan(&plan) {
            Ok(new_tables) => {
                let mut result = HashSet::new();

                for key in new_tables {
                    result.insert(key.name.predicate);
                    self.register_table(key);
                }

                Ok(result)
            }
            Err(err) => {
                log::warn!("{err:?}");
                Ok(HashSet::new())
            }
        }
    }

    /// Checks wether the tree is a union of continous part of a single table.
    /// If so, returns the [`TableKey`] of the new table containing the result of this computation.
    fn plan_recognize_simple_union(&self, tree: &ExecutionTree<TableKey>) -> Option<TableKey> {
        if let ExecutionNode::Union(union) = &*tree.root()?.0.upgrade()?.as_ref().borrow() {
            let mut overall_cover = Vec::<usize>::new();
            let mut predicate_opt: Option<Identifier> = None;
            let mut order_opt: Option<ColumnOrder> = None;

            for subnode in union {
                if let ExecutionNode::FetchTable(key) = &*subnode.0.upgrade()?.as_ref().borrow() {
                    if let Some(predicate) = predicate_opt {
                        let order = order_opt
                            .as_ref()
                            .expect("If predicate_opt is set then order_opt is also.");

                        if predicate != key.name.predicate || key.order != *order {
                            return None;
                        }
                    } else {
                        predicate_opt = Some(key.name.predicate);
                        order_opt = Some(key.order.clone());
                    }

                    let cover = match key.name.range {
                        TableRange::Single(step) => {
                            self.normalize_range(key.name.predicate, &(step..(step + 1)))
                        }
                        TableRange::Multiple(cover) => cover,
                    };
                    let cover_range = cover.start..(cover.start + cover.len);

                    overall_cover.append(&mut cover_range.collect());
                } else {
                    return None;
                }
            }

            overall_cover.sort();
            overall_cover.dedup();

            if overall_cover.len() <= 1 {
                return None;
            }

            for cover_index in 1..overall_cover.len() {
                if overall_cover[cover_index] - overall_cover[cover_index - 1] != 1 {
                    return None;
                }
            }

            if let Some(predicate) = predicate_opt {
                return Some(TableKey::new(
                    predicate,
                    TableRange::Multiple(TableCover {
                        start: overall_cover[0],
                        len: overall_cover.len(),
                    }),
                    order_opt.expect("If predicate_opt is set then order_opt is also."),
                ));
            }
        }

        None
    }

    /// Checks whether tree consists of only a `FetchTable` node.
    /// or if it is a `Project` node that has a `FetchTable` as a subnode.
    /// If so, returns the reordering which would turn the referenced table into this one.
    fn plan_recognize_renaming(
        &self,
        tree: &ExecutionTree<TableKey>,
    ) -> Option<(TableName, Reordering)> {
        match &*tree.root()?.0.upgrade()?.as_ref().borrow() {
            ExecutionNode::FetchTable(key) => {
                let arity = *self.predicate_arity.get(&key.name.predicate)?;
                Some((key.name, Reordering::default(arity)))
            }
            ExecutionNode::Project(subnode, reordering) => {
                if !reordering.is_permutation() {
                    return None;
                }

                if let ExecutionNode::FetchTable(key) = &*subnode.0.upgrade()?.as_ref().borrow() {
                    Some((key.name, reordering.clone()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Execute a given [`ExecutionPlan`].
    /// Before executing applies the following optimizations:
    ///     * Remove trees that would compute a duplicate table (meaning a table with the same name)
    ///     * Temporary tables which result in a continious union of subtables are saved permanently
    ///     * Computation which only reorder another permanent tree are removed and a reference is added instead
    ///
    /// Returns a list of those predicates that received new values.
    pub fn execute_plan_optimized(
        &mut self,
        mut plan: ExecutionPlan<TableKey>,
    ) -> Result<HashSet<Identifier>, Error> {
        let mut result = HashSet::<Identifier>::new();

        // Remove trees that save tables under existing names
        plan.trees.retain(|t| {
            if let ExecutionResult::Save(key) = t.result() {
                return !self.contains_key(key);
            }

            true
        });

        // Simplifiy plan
        plan.simplify();

        // Save temporary table which compute a continious union under a new name
        let mut union_map = HashMap::<TableId, ExecutionNode<TableKey>>::new();
        for tree in &mut plan.trees {
            if let ExecutionResult::Temp(id) = tree.result() {
                if let Some(key) = self.plan_recognize_simple_union(tree) {
                    union_map.insert(*id, ExecutionNode::FetchTable(key.clone()));
                }
            }

            tree.replace_temp_ids(&union_map);
        }

        // Remove trees that only (project &) fetch another permanent table and replace it with a reference
        plan.trees.retain(|t| {
            if let ExecutionResult::Save(new_key) = t.result() {
                if let Some((ref_name, reordering)) = self.plan_recognize_renaming(t) {
                    self.add_reference(
                        new_key.name.predicate,
                        self.translate_range(new_key.name.predicate, new_key.name.range),
                        ref_name,
                        reordering,
                    );

                    result.insert(new_key.name.predicate);

                    return false;
                }
            }

            true
        });

        // Execute the omptimized plan
        result.extend(self.execute_plan(plan)?.iter());
        Ok(result)
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
        let target_range = self.normalize_range(predicate, &steps);

        let ranges_transformed: Vec<Range<usize>> =
            ranges.iter().map(|r| r.start..(r.start + r.len)).collect();
        let target_transformed = target_range.start..(target_range.start + target_range.len);

        let covering = cover_interval(&ranges_transformed, &target_transformed);

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
    /// Returns an [`Ok(None)`] on non usable tables.
    pub fn combine_predicate(
        &mut self,
        predicate: Identifier,
        step: usize,
    ) -> Result<Option<TableKey>, Error> {
        let mut table_key = || {
            let arity = match self.predicate_arity.get(&predicate) {
                Some(val) => *val,
                None => return Ok(None),
            };
            self.add_union_table(
                predicate,
                0..step,
                predicate,
                0..step,
                Some(ColumnOrder::default(arity)),
            )
        };

        if let Some(key) = table_key()? {
            self.materialize_table(key).map(Some)
        } else {
            Ok(None)
        }
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
