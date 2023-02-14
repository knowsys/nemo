//! Managing of tables

use super::model::{DataSource, Identifier};
use crate::{
    error::Error,
    io::csv::read,
    meta::TimedCode,
    physical::{
        datatypes::DataTypeName,
        dictionary::Dictionary,
        management::{
            database::{TableId, TableName},
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
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt,
    fs::File,
    hash::Hash,
    ops::{Index, Range},
};

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
pub struct ChaseTableName {
    /// Number associated with a predicate which corresponds to a table on user level.
    pub predicate: Identifier,
    /// Ranges of execution steps this table covers.
    pub range: TableRange,
}

impl ChaseTableName {
    /// Create new [`ChaseTableName`].
    pub fn new(predicate: Identifier, range: TableRange) -> Self {
        Self { predicate, range }
    }
}

/// Representation of a table that is created during the chase.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ChaseTable {
    /// Name of the table.
    pub name: ChaseTableName,
    /// Order of the columns.
    pub order: ColumnOrder,
    /// Name of corresponding table in database instance
    pub db_name: TableName,
}

impl ChaseTable {
    /// Create new [`ChaseTable`].
    pub fn new(
        predicate: Identifier,
        range: TableRange,
        order: ColumnOrder,
        db_name: TableName,
    ) -> Self {
        Self {
            name: ChaseTableName::new(predicate, range),
            order,
            db_name,
        }
    }

    /// Create new [`ChaseTable`], using a generated internal table name.
    pub fn from_pred(predicate: Identifier, range: TableRange, order: ColumnOrder) -> Self {
        Self {
            name: ChaseTableName::new(predicate, range),
            order,
            db_name: TableName(ChaseTable::generate_table_name(predicate, range, order)),
        }
    }

    /// Create new [`ChaseTable`] given a [`TableName`],  using a generated internal table name.
    pub fn from_name(name: ChaseTableName, order: ColumnOrder) -> Self {
        Self {
            name,
            order,
            db_name: TableName(ChaseTable::generate_table_name(
                name.predicate,
                name.range,
                order,
            )),
        }
    }

    /// Make an internal table name from the given inputs.
    /// TODO: this is not a pretty name yet.
    fn generate_table_name(predicate: Identifier, range: TableRange, order: ColumnOrder) -> String {
        let table_name = format!("T{:?}-{:?}-{:?}", predicate, range, order);
        table_name
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
    Reference(ChaseTableName, Reordering),
}

/// Manager object for handling tables that are the result
/// of a seminaive existential rules evaluation process.
#[derive(Debug)]
pub struct TableManager<Dict: Dictionary> {
    /// [`DatabaseInstance`] managing all existing tables.
    database: DatabaseInstance<Dict>,

    /// Contains the status of each table.
    status: HashMap<ChaseTableName, TableStatus>,

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

impl<Dict: Dictionary> TableManager<Dict> {
    /// Create new [`TableManager`].
    pub fn new(dict_constants: Dict) -> Self {
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
    pub fn get_table_orders(&self, name: &ChaseTableName) -> Vec<&ColumnOrder> {
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
    pub fn contains_key(&self, key: &ChaseTable) -> bool {
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
                TableStatus::OnDisk(_) => false, // TODO: Check the order here as well?
                TableStatus::Reference(_, _) => false, // TODO: Check the order here as well?
            }
        } else {
            false
        }
    }

    /// Given a predicate and a range, return the corresponding [`TableName`]
    pub fn get_table_name(&self, predicate: Identifier, range: Range<usize>) -> ChaseTableName {
        if let Some(steps) = self.predicate_to_steps.get(&predicate) {
            debug_assert!(!steps.is_empty());
            debug_assert!(steps.is_sorted());

            if range.start > *steps.last().unwrap() {
                debug_assert!(range.len() == 1);
                ChaseTableName::new(predicate, TableRange::Single(range.start))
            } else {
                ChaseTableName::new(
                    predicate,
                    TableRange::Multiple(self.normalize_range(predicate, &range)),
                )
            }
        } else {
            debug_assert!(range.len() == 1);
            ChaseTableName::new(predicate, TableRange::Single(range.start))
        }
    }

    /// Update all the auxiliary structures for a new table name.
    fn register_name(&mut self, name: ChaseTableName, arity: usize) {
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
    fn register_table(&mut self, key: ChaseTable) {
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
    ) -> ChaseTableName {
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
        table_name: TableName,
    ) -> ChaseTable {
        let table_key = ChaseTable {
            name: self.get_table_name(predicate, range),
            order,
            db_name: table_name,
        };

        // self.register_table(table_key.clone());
        // self.database.add(table_name, table, schema);

        table_key
    }

    /// Add a reference to another table under a new name.
    /// If it is another reference then it will point to the referenced table of that.
    /// We only allow reorderings that are permutations.
    pub fn add_reference(
        &mut self,
        predicate: Identifier,
        range: Range<usize>,
        ref_name: ChaseTableName,
        ref_reorder: Reordering,
    ) -> ChaseTableName {
        debug_assert!(ref_reorder.is_permutation());
        log::info!(
            "Add reference {} -> {} ({:?})",
            predicate.0,
            ref_name.predicate.0,
            ref_reorder
        );

        debug_assert!(
            ref_reorder.len_source() == *self.predicate_arity.get(&ref_name.predicate).unwrap()
        );

        let new_name = self.get_table_name(predicate, range);
        let arity = ref_reorder.len_target();

        let (overall_name, overall_reorder) =
            if let TableStatus::Reference(another_table, another_order) = self
                .status
                .get(&ref_name)
                .expect("Function assumes that referenced table exists.")
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
    ) -> Result<Option<ChaseTable>, Error> {
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
            self.get_table_orders(&ChaseTableName::new(input_predicate, input_covering[0]));

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
        let output_table = ChaseTable::from_name(output_name, output_order);
        let mut output_tree = ExecutionTree::new(
            "Merge Tables".to_string(),
            ExecutionResult::Save(output_table.db_name),
        );

        let fetch_nodes: Vec<ExecutionNodeRef> = input_covering
            .into_iter()
            .map(|r| {
                let key = ChaseTable::from_pred(input_predicate, r, input_order.clone());
                output_tree.fetch_table(key.db_name)
            })
            .collect();
        let union_node = output_tree.union(fetch_nodes);
        output_tree.set_root(union_node);

        let mut execution_plan = ExecutionPlan::new();
        execution_plan.push(output_tree);

        self.execute_plan(execution_plan)?;
        Ok(Some(output_table))
    }

    /// Load table from a given on-disk source
    /// TODO: This function should change when the type system gets introduced on the logical layer
    fn load_table(source: &DataSource, arity: usize, dict: &mut Dict) -> Result<Trie, Error> {
        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .start();

        log::info!("Loading {:?}", source);

        let (trie, _name) = match source {
            DataSource::CsvFile(file) => {
                // Using fallback solution to treat eveything as string for now (storing as u64 internally)
                let datatypes: Vec<Option<DataTypeName>> = (0..arity).map(|_| None).collect();

                let gz_decoder = flate2::read::GzDecoder::new(File::open(file.as_path())?);

                let col_table = if gz_decoder.header().is_some() {
                    read(&datatypes, &mut crate::io::csv::reader(gz_decoder), dict)?
                } else {
                    read(
                        &datatypes,
                        &mut crate::io::csv::reader(File::open(file.as_path())?),
                        dict,
                    )?
                };
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
        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .stop();

        Ok(trie)
    }

    /// Prepare a table for a known chase predicate to be loaded and available in the required order.
    ///
    /// To this end, the table name is resolved by considering its [`TableStatus`], tracing back references
    /// and thus finding the actual name of a chase table with that contents. For in-memory tables, a
    /// re-ordering may or may not be needed to get the requested table; for on-disk tables, data might
    /// first need to be loaded.
    ///
    /// The function returns a chase table that can be used to access the requested data.
    ///
    fn materialize_table(&mut self, table: ChaseTable) -> Result<ChaseTable, Error> {
        // Check if the chase table is a reference to another table:
        let (actual_name, reordering) = if let TableStatus::Reference(referenced_name, reordering) =
            &self
                .status
                .get(&table.name)
                .expect("Could not find status for chase table.")
        {
            (*referenced_name, reordering.clone())
        } else {
            (
                table.name,
                Reordering::default(
                    *self
                        .predicate_arity
                        .get(&table.name.predicate)
                        .expect("Could not find predicate to determine its arity."),
                ),
            )
        };

        let required_order =
            ColumnOrder::new(reordering.inverse().apply_to(table.order.as_slice()));
        let result_table = ChaseTable::from_name(actual_name, required_order.clone());

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
                        Some(ChaseTable::from_name(actual_name, closest_order.clone()))
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
                        schema.add_entry(type_name, false, false);
                    }

                    let loaded_table =
                        ChaseTable::from_name(actual_name, ColumnOrder::default(arity));

                    // self.database.add(loaded_table.db_name, trie, schema);
                    *status = TableStatus::InMemory(vec![loaded_table.order.clone()]);

                    if !required_order.is_default() {
                        Some(loaded_table)
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

                let mut project_tree = ExecutionTree::new(
                    "Required Reorder".to_string(),
                    ExecutionResult::Save(result_table.db_name),
                );
                let reordered_node = project_tree.fetch_table(reordered_table.db_name);
                let project_node = project_tree.project(reordered_node, projection_reordering);
                project_tree.set_root(project_node);

                let mut reorder_plan = ExecutionPlan::new();
                reorder_plan.push(project_tree);

                self.execute_plan(reorder_plan)?;
            }
        } else {
            panic!("Function assumes that materialized table is known.");
        }

        Ok(result_table)
    }

    fn execute_plan(&mut self, mut plan: ExecutionPlan) -> Result<HashSet<Identifier>, Error> {
        // First, we need to make sure that every requested table is available or produce it if necessary
        // We exclude from this tables that will be produced during the execution of the plan
        let mut produced_keys = HashSet::<ChaseTable>::new();
        for tree in &mut plan.trees {
            for fetch_node in tree.all_fetched_tables() {
                let node_unpacked = &mut *fetch_node.0.borrow_mut();
                if let ExecutionNode::FetchTable(key) = node_unpacked {
                    if !produced_keys.contains(key) {
                        *key = self.materialize_table(key)?;
                    }
                } else {
                    unreachable!()
                }
            }

            if let ExecutionResult::Save(new_key) = tree.result() {
                produced_keys.insert(new_key.clone());
            }
        }

        let new_tables = self.database.execute_plan(&plan)?;

        let mut result = HashSet::new();

        for key in new_tables {
            result.insert(key.name.predicate);
            self.register_table(key);
        }

        Ok(result)
    }

    /// Checks whether the tree is a union of continuous part of a single table.
    /// If so, returns the [`TableKey`] of the new table containing the result of this computation.
    fn plan_recognize_simple_union(&self, tree: &ExecutionTree) -> Option<ChaseTable> {
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
                return Some(ChaseTable::from_pred(
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
        tree: &ExecutionTree,
    ) -> Option<(ChaseTableName, Reordering)> {
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
    ///     * Temporary tables which result in a continuous union of subtables are saved permanently
    ///     * Computation which only reorder another permanent tree are removed and a reference is added instead
    ///
    /// Returns a list of those predicates that received new values.
    pub fn execute_plan_optimized(
        &mut self,
        mut plan: ExecutionPlan,
    ) -> Result<HashSet<Identifier>, Error> {
        let mut result = HashSet::<Identifier>::new();

        // Debug check whether we procude duplicate entries
        for tree in &plan.trees {
            if let ExecutionResult::Save(key) = tree.result() {
                debug_assert!(!self.contains_key(key))
            }
        }

        // Simplify plan
        plan.simplify();

        // Save temporary table which compute a continuous union under a new name
        let mut union_map = HashMap::<TableId, ExecutionNode>::new();
        for tree in &mut plan.trees {
            if let ExecutionResult::Temp(id) = tree.result() {
                if let Some(key) = self.plan_recognize_simple_union(tree) {
                    union_map.insert(*id, ExecutionNode::FetchTable(key.db_name));
                }
            }

            tree.replace_temp_ids(&union_map);
        }

        // Remove trees that only (project &) fetch another permanent table and are themselves unused
        // Remember them so they can still be added as references later
        struct AddReference {
            from_predicate: Identifier,
            from_range: Range<usize>,
            to: ChaseTableName,
            reordering: Reordering,
        }

        let all_fetched_keys = plan.all_fetched_keys();
        let mut references = Vec::<AddReference>::new();
        let mut retain_index: usize = 0;
        plan.trees.retain(|t| {
            let result = if let ExecutionResult::Save(new_key) = t.result() {
                if let Some((ref_name, reordering)) = self.plan_recognize_renaming(t) {
                    if all_fetched_keys.contains(new_key) {
                        return true;
                    }

                    references.push(AddReference {
                        from_predicate: new_key.name.predicate,
                        from_range: self
                            .translate_range(new_key.name.predicate, new_key.name.range),
                        to: ref_name,
                        reordering,
                    });

                    false
                } else {
                    true
                }
            } else {
                true
            };

            // Note: This works because retain guarantees the iteration order
            retain_index += 1;

            result
        });

        // Execute the optimized plan
        result.extend(self.execute_plan(plan)?.iter());

        // Add the references from before
        for reference in references {
            self.add_reference(
                reference.from_predicate,
                reference.from_range,
                reference.to,
                reference.reordering,
            );
        }

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
    ) -> Result<Option<ChaseTable>, Error> {
        if !self.predicate_arity.contains_key(&predicate) {
            return Ok(None);
        }

        let arity = *self
            .predicate_arity
            .get(&predicate)
            .expect("Arity should have a value; Checked for existence before.");
        self.add_union_table(
            predicate,
            0..step,
            predicate,
            0..step,
            Some(ColumnOrder::default(arity)),
        )?
        .map(|key| self.materialize_table(key))
        .transpose()
    }

    /// Return trie with the given key.
    pub fn get_trie<'a>(&'a self, key: &ChaseTable) -> &'a Trie {
        self.database.get_by_key(&key.db_name)
    }

    /// Return the dictionary used in the database instance.
    /// TODO: Remove this once proper Dictionary support is implemented on the physical layer.
    pub fn get_dict(&self) -> &Dict {
        self.database.get_dict_constants()
    }
}
