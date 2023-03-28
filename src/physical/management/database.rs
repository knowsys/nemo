use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::fs::File;
use std::path::PathBuf;

use bytesize::ByteSize;

use crate::io::csv::{read, DSVReader};
use crate::physical::columnar::proxy_builder;
use crate::physical::datatypes::{DataValueT, StorageTypeName, StorageValueT};
use crate::physical::tabular::operations::project_reorder::project_and_reorder;
use crate::physical::tabular::operations::triescan_project::ProjectReordering;
use crate::physical::tabular::table_types::trie::DebugTrie;
use crate::physical::tabular::traits::table::Table;
use crate::physical::util::mapping::permutation::Permutation;
use crate::physical::util::mapping::traits::NatMapping;
use crate::{
    error::Error,
    meta::TimedCode,
    physical::tabular::{
        operations::{
            materialize::materialize, triescan_append::TrieScanAppend, TrieScanJoin, TrieScanMinus,
            TrieScanNulls, TrieScanProject, TrieScanSelectEqual, TrieScanSelectValue,
            TrieScanUnion,
        },
        table_types::trie::{Trie, TrieScanGeneric},
        traits::{table_schema::TableSchema, triescan::TrieScanEnum},
    },
};

use super::execution_plan::{ExecutionOperation, ExecutionTree};
use super::{
    execution_plan::{ExecutionNodeRef, ExecutionResult},
    type_analysis::{TypeTree, TypeTreeNode},
    ByteSized, ExecutionPlan,
};

#[cfg(feature = "no-prefixed-string-dictionary")]
/// Dictionary Implementation used in the current configuration
pub type Dict = crate::physical::dictionary::StringDictionary;
#[cfg(not(feature = "no-prefixed-string-dictionary"))]
/// Dictionary Implementation used in the current configuration
pub type Dict = crate::physical::dictionary::PrefixedStringDictionary;

/// Type that represents a reordering of the columns of a table.
/// It is given in form of a permutation which encodes the transformation
/// that is needed to get from the original column order to this one.
pub type ColumnOrder = Permutation;

/// Type which represents table id.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TableId(u64);

impl Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TableId {
    /// Increment the id by one.
    /// Return the old (non-incremented) id.
    pub fn increment(&mut self) -> Self {
        let old = *self;
        self.0 += 1;
        old
    }

    /// Return the integer value that represents the id.
    pub fn get(&self) -> u64 {
        self.0
    }
}

/// Indicates the file format of a table stored on disc.
#[derive(Debug)]
pub enum TableSource {
    /// Table is contained in a DSV (delimiter-separated values) file
    DSV {
        /// the path to the DSV file
        file: PathBuf,
        /// the delimiter separating values
        delimiter: u8,
    },
    /// Table is stored as facts in an rls file
    /// TODO: To not invoke the parser twice I just put the parsed "row-table" here.
    /// Does not seem quite right
    RLS(Vec<Vec<DataValueT>>),
}

impl Display for TableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableSource::DSV {
                file,
                delimiter: b',',
            } => write!(f, "CSV file: {}", file.display()),
            TableSource::DSV {
                file,
                delimiter: b'\t',
            } => write!(f, "TSV file: {}", file.display()),
            TableSource::DSV { file, delimiter } => write!(
                f,
                "DSV file with delimiter {delimiter:?}: {}",
                file.display()
            ),
            TableSource::RLS(_) => write!(f, "Rule file"),
        }
    }
}

/// Data which stores a trie, possibly not in memory.
#[derive(Debug)]
pub enum TableStorage {
    /// Table is stored as a [`Trie`] in memory.
    InMemory(Trie),
    /// Table is stored on disk.
    OnDisk(TableSchema, Vec<TableSource>),
}

impl TableStorage {
    /// Load table from a given on-disk source
    /// TODO: This function should change when the type system gets introduced on the logical layer
    fn load_from_disk(
        source: &TableSource,
        schema: &TableSchema,
        dict: &mut Dict,
    ) -> Result<Trie, Error> {
        {
            TimedCode::instance()
                .sub("Reasoning/Execution/Load Table")
                .start();

            log::info!("Loading source {source}");

            let trie = match source {
                TableSource::DSV { file, delimiter } => {
                    // Using fallback solution to treat everything as string for now (storing as u64 internally)
                    let datatypes: Vec<Option<StorageTypeName>> =
                        (0..schema.arity()).map(|_| None).collect();

                    let gz_decoder = flate2::read::GzDecoder::new(File::open(file.as_path())?);

                    let col_table = if gz_decoder.header().is_some() {
                        read(
                            &datatypes,
                            &mut crate::io::csv::reader(gz_decoder, *delimiter),
                            dict,
                        )?
                    } else {
                        read(
                            &datatypes,
                            &mut crate::io::csv::reader(File::open(file.as_path())?, *delimiter),
                            dict,
                        )?
                    };

                    Trie::from_cols(col_table)
                }
                TableSource::RLS(table_rows) => {
                    let rows: Vec<Vec<StorageValueT>> = table_rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .cloned()
                                .map(|val| val.to_storage_value(dict))
                                .collect()
                        })
                        .collect();
                    Trie::from_rows(&rows)
                }
            };

            TimedCode::instance()
                .sub("Reasoning/Execution/Load Table")
                .stop();

            Ok(trie)
        }
    }

    /// Function that makes sure that underlying table is available in memory.
    pub fn into_memory<'a>(&'a mut self, dict: &mut Dict) -> Result<&'a Trie, Error> {
        match self {
            TableStorage::InMemory(_) => {}
            TableStorage::OnDisk(schema, sources) => {
                let new_trie = if sources.len() == 1 {
                    Self::load_from_disk(&sources[0], schema, dict)?
                } else {
                    // If the trie results form multiple sources
                    // we load each source indivdually and then compute the union over all tries

                    let mut loaded_tries = Vec::<Trie>::with_capacity(sources.len());
                    for source in sources {
                        loaded_tries.push(Self::load_from_disk(source, schema, dict)?);
                    }

                    let loaded_tries_iters: Vec<TrieScanEnum> = loaded_tries
                        .iter()
                        .map(|t| TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(t)))
                        .collect();
                    let mut union_iter =
                        TrieScanEnum::TrieScanUnion(TrieScanUnion::new(loaded_tries_iters));

                    materialize(&mut union_iter).unwrap()
                };

                *self = TableStorage::InMemory(new_trie);
            }
        }

        Ok(self
            .get_trie()
            .expect("Trie has been loaded into memory above."))
    }

    /// Return a reference to the stored trie.
    /// Returns `None` if trie is not in memory.
    pub fn get_trie(&self) -> Option<&Trie> {
        if let TableStorage::InMemory(trie) = self {
            Some(trie)
        } else {
            None
        }
    }
}

impl ByteSized for TableStorage {
    fn size_bytes(&self) -> ByteSize {
        match self {
            TableStorage::InMemory(trie) => trie.size_bytes(),
            TableStorage::OnDisk(_, _) => ByteSize(0),
        }
    }
}

#[derive(Debug)]
enum TableStatus {
    /// Table is present in different orders
    Present(HashMap<ColumnOrder, TableStorage>),
    /// Table has the same contents as another table except for reordering.
    /// The permutation is the transformation needed to get from the referenced table to this one.
    Reference(TableId, Permutation),
}

/// Manages tables under different orders.
/// Also has the capability of representing a table as a reordered version of another.
#[derive(Debug, Default)]
pub struct OrderedReferenceManager {
    map: HashMap<TableId, TableStatus>,
}

/// Stores the result of a function that resolves table references (mutable version)
struct TableResolvedMut<'a> {
    /// Hashmap containing all the available orders of a table.
    map: &'a mut HashMap<ColumnOrder, TableStorage>,
    /// If the requested table is a reordered reference then this is the reordered requested column order
    /// If no reordering was required then this is the original requested column order.
    order: ColumnOrder,
}

/// Stores the result of a function that resolves table references
struct TableResolved<'a> {
    /// Hashmap containing all the available orders of a table.
    map: &'a HashMap<ColumnOrder, TableStorage>,
    /// If the requested table is a reordered reference then this is the reordered requested column order
    /// If no reordering was required then this is the original requested column order.
    order: ColumnOrder,
}

impl OrderedReferenceManager {
    fn resolve_reference_mut(
        &mut self,
        id: TableId,
        order: &ColumnOrder,
    ) -> Option<TableResolvedMut> {
        match &self.map.get(&id)? {
            TableStatus::Present(_) => {}
            TableStatus::Reference(ref_id, permutation) => {
                return self.resolve_reference_mut(
                    *ref_id,
                    &order.chain_permutation(&permutation.invert()),
                )
            }
        }

        match self.map.get_mut(&id)? {
            TableStatus::Present(map) => {
                return Some(TableResolvedMut {
                    map,
                    order: order.clone(),
                })
            }
            TableStatus::Reference(_, _) => {}
        }

        unreachable!("Each case should have been handled by one of the above matches.")
    }

    fn resolve_reference(&self, id: TableId, order: &ColumnOrder) -> Option<TableResolved> {
        match &self.map.get(&id)? {
            TableStatus::Present(map) => Some(TableResolved {
                map,
                order: order.clone(),
            }),
            TableStatus::Reference(ref_id, permutation) => {
                self.resolve_reference(*ref_id, &order.chain_permutation(&permutation.invert()))
            }
        }
    }

    /// Add a new table (that is not a reference).
    pub fn add_present(&mut self, id: TableId, order: ColumnOrder, storage: TableStorage) {
        if let Some(resolved) = self.resolve_reference_mut(id, &order) {
            resolved.map.insert(order, storage);
        } else {
            let mut new_order_map = HashMap::<ColumnOrder, TableStorage>::new();
            new_order_map.insert(order, storage);

            let status = TableStatus::Present(new_order_map);
            self.map.insert(id, status);
        }
    }

    /// Add a new reference to another table.
    /// Panics if the id of the referenced table does not exist.
    pub fn add_reference(&mut self, id: TableId, reference_id: TableId, permutation: Permutation) {
        let (final_id, final_permutation) = if let TableStatus::Reference(ref_id, ref_permutation) =
            &self
                .map
                .get(&reference_id)
                .expect("Referenced id should exist.")
        {
            (*ref_id, ref_permutation.chain_permutation(&permutation))
        } else {
            (id, permutation)
        };

        let status = TableStatus::Reference(final_id, final_permutation);
        self.map.insert(id, status);
    }

    /// Return a reference to the [`TableStorage`] associated with the given [`TableId`] and [`ColumnOrder`].
    /// Returns `None` if there is no table with that id or order.
    pub fn table_storage<'a>(
        &'a self,
        id: TableId,
        order: &ColumnOrder,
    ) -> Option<&'a TableStorage> {
        let resolved = self.resolve_reference(id, order)?;
        resolved.map.get(&resolved.order)
    }

    /// Return a mutable reference to the [`TableStorage`] associated with the given [`TableId`] and [`ColumnOrder`].
    /// Returns `None` if there is no table with that id or order.
    pub fn table_storage_mut<'a>(
        &'a mut self,
        id: TableId,
        order: &ColumnOrder,
    ) -> Option<&'a mut TableStorage> {
        let resolved = self.resolve_reference_mut(id, order)?;
        resolved.map.get_mut(&resolved.order)
    }

    /// Return an iterator of all the available orders of a table.
    /// Returns `None` if there is no table with the given id.
    pub fn available_orders(&self, id: TableId) -> Option<impl Iterator<Item = &ColumnOrder>> {
        let resolved = self.resolve_reference(id, &ColumnOrder::default())?;
        Some(resolved.map.keys())
    }

    /// Delete the given table.
    /// Return `None` if there is no table with the given id.
    /// TODO: This does not check/fix references of tables.
    pub fn delete_table(&mut self, id: &TableId) -> Option<()> {
        self.map.remove(id)?;
        Some(())
    }
}

impl ByteSized for OrderedReferenceManager {
    fn size_bytes(&self) -> ByteSize {
        self.map.iter().fold(ByteSize(0), |acc, (_, status)| {
            acc + match &status {
                TableStatus::Present(ordered_tries) => ordered_tries
                    .iter()
                    .map(|(_, s)| s.size_bytes())
                    .fold(ByteSize(0), |acc, x| acc + x),
                TableStatus::Reference(_, _) => ByteSize(0),
            }
        })
    }
}

/// Struct that contains useful information about a trie
/// as well as the actual owner of the trie.
#[derive(Debug)]
struct TableInfo {
    /// The name of the table.
    pub name: String,
    /// The schema of the table
    pub schema: TableSchema,
}

impl TableInfo {
    /// Create new [`TableInfo`].
    pub fn new(name: String, schema: TableSchema) -> Self {
        Self { name, schema }
    }
}

/// Represents a collection of tables
#[derive(Debug)]
pub struct DatabaseInstance {
    /// Structure which owns all the tries; accessed through Id.
    storage_handler: OrderedReferenceManager,
    /// Map with additional infromation about the tables
    table_infos: HashMap<TableId, TableInfo>,

    /// Dictionary which stores the strings associates with abstract constants
    dict_constants: RefCell<Dict>,

    /// Lowest unused null value.
    current_null: u64,

    /// The lowest unused TableId.
    /// Will be incremented for each new table and will never be reused.
    current_id: TableId,
}

/// Result of executing an [`ExecutionTree`].
enum ComputationResult {
    /// Resulting trie is only stored temporarily within this object.
    Temporary(Trie),
    /// Trie is stored permanently under a [`TableId`] and [`ColumnOrder`].
    Permanent(TableId, ColumnOrder),
    /// The computation resulted in an empty trie.
    Empty,
}

impl Default for DatabaseInstance {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseInstance {
    /// Create new [`DatabaseInstance`]
    pub fn new() -> Self {
        let current_null = 1 << 63; // TODO: Think about a robust null representation method

        Self {
            storage_handler: OrderedReferenceManager::default(),
            table_infos: HashMap::new(),
            dict_constants: RefCell::new(Dict::default()),
            current_null,
            current_id: TableId::default(),
        }
    }

    /// Return the current number of tables.
    pub fn num_tables(&self) -> usize {
        self.table_infos.len()
    }

    /// Return the name of a table given its id.
    /// Panics if the id does not exist.
    pub fn table_name(&self, id: TableId) -> &str {
        &self.table_infos.get(&id).unwrap().name
    }

    /// Return the schema of a table identified by the given [`TableId`].
    /// Panics if the id does not exist.
    pub fn table_schema(&self, id: TableId) -> &TableSchema {
        &self.table_infos.get(&id).unwrap().schema
    }

    /// Return the arity of a table identified by the given [`TableId`].
    /// Panics if the id does not exist.
    pub fn table_arity(&self, id: TableId) -> usize {
        self.table_schema(id).arity()
    }

    /// Returns a reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants(&self) -> Dict {
        self.dict_constants.clone().take()
    }

    /// Register a new table under a given name and schema.
    /// Returns the [`TableId`] with which the new table can be addressed.
    pub fn register_table(&mut self, name: &str, schema: TableSchema) -> TableId {
        debug_assert!(schema.arity() > 0);

        self.table_infos
            .insert(self.current_id, TableInfo::new(String::from(name), schema));

        self.current_id.increment()
    }

    /// Add a new trie.
    pub fn add_trie(&mut self, id: TableId, order: ColumnOrder, trie: Trie) {
        self.storage_handler
            .add_present(id, order, TableStorage::InMemory(trie));
    }

    /// Register table and add a new trie.
    pub fn register_add_trie(
        &mut self,
        name: &str,
        schema: TableSchema,
        order: ColumnOrder,
        trie: Trie,
    ) -> TableId {
        let id = self.register_table(name, schema);
        self.add_trie(id, order, trie);

        id
    }

    /// Add the sources of a table currently stored on disk.
    pub fn add_sources(&mut self, id: TableId, order: ColumnOrder, sources: Vec<TableSource>) {
        let schema = self.table_schema(id).clone();
        self.storage_handler
            .add_present(id, order, TableStorage::OnDisk(schema, sources));
    }

    /// Add a new table that is a reordered version of an existing table.
    /// Panics if referenced id does not exist.
    pub fn add_reference(&mut self, id: TableId, reference_id: TableId, permutation: Permutation) {
        self.storage_handler
            .add_reference(id, reference_id, permutation);
    }

    /// Deletes a table with all its orders.
    /// TODO: For now, this does not care about keeping references intact.
    /// Panics if the table does not exist.
    pub fn delete(&mut self, id: TableId) {
        self.storage_handler
            .delete_table(&id)
            .expect("Table to be deleted should exist.");
        self.table_infos
            .remove(&id)
            .expect("Table to be deleted should exist.");
    }

    /// Provides a measure of how "difficult" it is to transform a column with this order into another.
    /// Say, `from = {0->2, 1->1, 2->0}` and `to = {0->1, 1->0, 2->2}`.
    /// Starting from position 0 in "from" one needs to skip one layer to reach the 2 in "to" (+1).
    /// Then we need to go back two layers to reach the 1 (+2)
    /// Finally, we go one layer down to reach 0 (+-0).
    /// This gives us an overall score of 3.
    /// Returned value is 0 if and only if from == to.
    fn distance(from: &ColumnOrder, to: &ColumnOrder) -> usize {
        let max_len = from.last_mapped().max(to.last_mapped()).unwrap_or(0);

        let to_inverted = to.invert();

        let mut current_score: usize = 0;
        let mut current_position_from: isize = -1;

        for position_to in 0..=max_len {
            let current_value = to_inverted.get(position_to);

            let position_from = from.get(current_value);
            let difference: isize = (position_from as isize) - current_position_from;

            let penalty: usize = if difference <= 0 {
                difference.unsigned_abs()
            } else {
                // Taking one forward step should not be punished
                (difference - 1) as usize
            };

            current_score += penalty;
            current_position_from = position_from as isize;
        }

        current_score
    }

    /// Helper function that iterates through a collection of [`ColumnOrder`]
    /// to find the one that is "closest" to given one.
    /// Returns `None` if the given iterator is empty.
    fn search_closest_order<'a, OrderIter: Iterator<Item = &'a ColumnOrder>>(
        iter_orders: OrderIter,
        order: &ColumnOrder,
    ) -> Option<&'a ColumnOrder> {
        iter_orders.min_by(|x, y| Self::distance(x, order).cmp(&Self::distance(y, order)))
    }

    /// Return a [`ProjectReordering`] that will turn a table given in some [`ColumnOrder`] into the same table in another [`ColumnOrder`].
    pub fn reorder_to(
        source_order: &ColumnOrder,
        target_order: &ColumnOrder,
        arity: usize,
    ) -> ProjectReordering {
        let mut result_map = HashMap::<usize, usize>::new();

        for input in 0..arity {
            let source_output = source_order.get(input);
            let target_output = target_order.get(input);

            result_map.insert(source_output, target_output);
        }

        ProjectReordering::from_map(result_map, arity)
    }

    /// Will ensure that the requested table will exist as a [`Trie`] in memory.
    /// More precisely this will
    ///     * Reorder an existing table if the table is not available in the requested order
    ///     * Load a table from disk if it currently not in memory
    /// Panics if the requested table does not exist.
    fn make_available_in_memory(&mut self, id: TableId, order: &ColumnOrder) -> Result<(), Error> {
        let arity = self.table_arity(id);
        let closest_order = Self::search_closest_order(
            self.storage_handler
                .available_orders(id)
                .expect("Table with given id should exist."),
            order,
        )
        .expect("This function assumes that there is at least one table under the given id.");

        let reorder = Self::reorder_to(order, closest_order, arity);
        let trie_unordered = self
            .storage_handler
            .table_storage_mut(id, &closest_order.clone())
            .expect("Call to search_closest_ordered should give us an existing order")
            .into_memory(self.dict_constants.get_mut())?;

        if !reorder.is_identity() {
            TimedCode::instance()
                .sub("Reasoning/Execution/Required Reorder")
                .start();

            let trie_reordered = project_and_reorder(trie_unordered, &reorder);

            TimedCode::instance()
                .sub("Reasoning/Execution/Required Reorder")
                .stop();

            self.add_trie(id, order.clone(), trie_reordered);
        }

        Ok(())
    }

    // Helper function which checks whether the top level tree node is of type `AppendNulls`.
    // If this is the case returns the amount of null-columns that have been appended.
    // TODO: Nothing about this feels right; revise later
    fn appends_nulls(node: ExecutionNodeRef) -> u64 {
        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::AppendNulls(_subnode, num_nulls) => *num_nulls as u64,
            _ => 0,
        }
    }

    /// Produces a new [`Trie`] from an [`ExecutionTree`].
    /// # Panics
    /// Panics if the tables that are being loaded by the [`ExecutionTree`] are not available in memory.
    /// Also panics if the [`ExecutionTree`] wants to perform a project/reorder operation on a non-materialized trie.
    fn produce_new_trie(
        &self,
        execution_tree: &ExecutionTree,
        type_tree: &TypeTree,
        computation_results: &HashMap<usize, ComputationResult>,
    ) -> Result<Option<Trie>, Error> {
        let root_rc = execution_tree.root().get_rc();
        let root_operation = &root_rc.borrow().operation;

        if let ExecutionOperation::Project(subnode, reordering) = root_operation {
            let subnode_rc = subnode.get_rc();
            let subnode_operation = &subnode_rc.borrow().operation;

            match subnode_operation {
                ExecutionOperation::FetchExisting(id, order) => {
                    let trie_ref = self.get_trie(*id, order);
                    if trie_ref.row_num() == 0 {
                        return Ok(None);
                    }

                    Ok(Some(project_and_reorder(trie_ref, reordering)))
                }
                ExecutionOperation::FetchNew(index) => {
                    let comp_result = computation_results.get(index).unwrap();
                    let trie_ref = match comp_result {
                        ComputationResult::Temporary(trie) => trie,
                        ComputationResult::Permanent(id, order) => self.get_trie(*id, order),
                        ComputationResult::Empty => return Ok(None),
                    };

                    if trie_ref.row_num() == 0 {
                        return Ok(None);
                    }

                    Ok(Some(project_and_reorder(trie_ref, reordering)))
                }
                _ => panic!(
                    "The project/reorder operation can only be applied to materialized tries."
                ),
            }
        } else {
            let iter_opt =
                self.get_iterator_node(execution_tree.root(), type_tree, computation_results)?;

            Ok(iter_opt.and_then(|mut iter| materialize(&mut iter)))
        }
    }

    /// Executes a given [`ExecutionPlan`].
    /// Returns a map that assigns to each plan id of a permanenet table the [`TableId`] in the [`DatabaseInstance`]
    /// This may fail if certain operations are performed on tries with incompatible types
    /// or if the plan references tries that do not exist.
    pub fn execute_plan(&mut self, plan: ExecutionPlan) -> Result<HashMap<usize, TableId>, Error> {
        let execution_trees = plan.split_at_write_nodes();

        // The variables below associate the write node ids of the given plan with some additional information
        let mut permanent_ids = HashMap::<usize, TableId>::new();
        let mut type_trees = HashMap::<usize, TypeTree>::new();
        let mut computation_results = HashMap::<usize, ComputationResult>::new();
        let mut removed_temp_ids = HashSet::<usize>::new();

        for (tree_id, mut execution_tree) in execution_trees {
            let timed_string = format!("Reasoning/Execution/{}", execution_tree.name());
            TimedCode::instance().sub(&timed_string).start();

            if let Some(simplified_tree) = execution_tree.simplify(&removed_temp_ids) {
                execution_tree = simplified_tree;
            } else {
                removed_temp_ids.insert(tree_id);

                TimedCode::instance().sub(&timed_string).stop();
                continue;
            }

            execution_tree.satisfy_leapfrog_triejoin();

            let type_tree = TypeTree::from_execution_tree(self, &type_trees, &execution_tree)?;
            let schema = type_tree.schema.clone();

            for (id, order) in execution_tree.required_tables() {
                self.make_available_in_memory(id, &order)?;
            }

            let num_null_columns = Self::appends_nulls(execution_tree.root());

            let new_trie_opt =
                self.produce_new_trie(&execution_tree, &type_tree, &computation_results)?;
            type_trees.insert(tree_id, type_tree);

            if let Some(new_trie) = new_trie_opt {
                // If trie appended nulls then we need to update our `current_null` value
                self.current_null += new_trie.num_elements() as u64 * num_null_columns;

                // Add new trie to the appropriate place
                match execution_tree.result() {
                    ExecutionResult::Temporary => {
                        log::info!(
                            "Saved temporary table: {} entries ({})",
                            new_trie.row_num(),
                            new_trie.size_bytes()
                        );

                        computation_results.insert(tree_id, ComputationResult::Temporary(new_trie));
                    }
                    ExecutionResult::Permanent(order, name) => {
                        log::info!(
                            "Saved permanent table: {} entries ({})",
                            new_trie.row_num(),
                            new_trie.size_bytes()
                        );

                        let new_id = self.register_table(name, schema);
                        self.add_trie(new_id, order.clone(), new_trie);

                        permanent_ids.insert(tree_id, new_id);
                        computation_results
                            .insert(tree_id, ComputationResult::Permanent(new_id, order.clone()));
                    }
                }
            } else {
                log::info!("Trie does not contain any elements");

                computation_results.insert(tree_id, ComputationResult::Empty);
            }

            TimedCode::instance().sub(&timed_string).stop();
        }

        Ok(permanent_ids)
    }

    /// Return a reference to a trie with the given id and order.
    /// Panics if no table under the given id and order exists.
    /// Panics if trie is not available in memory.
    pub fn get_trie<'a>(&'a self, id: TableId, order: &ColumnOrder) -> &'a Trie {
        let storage = self
            .storage_handler
            .table_storage(id, order)
            .expect("Function assumes that there is a table with the given id and order.");

        storage
            .get_trie()
            .expect("Function assumes that trie is in memory.")
    }

    /// Returns a DebugTrie for writing to CSV.
    /// Panics if no table under the given id and order exists.
    /// Panics if trie is not available in memory.
    pub fn get_debug_trie(&self, id: TableId, order: &ColumnOrder) -> DebugTrie {
        let storage = self
            .storage_handler
            .table_storage(id, order)
            .expect("Function assumes that there is a table with the given id and order.");

        storage
            .get_trie()
            .expect("Function assumes that trie is in memory.")
            .debug(self.dict_constants.clone().take())
    }

    /// Return a reference to a trie identified by its id and order.
    /// If the trie is not available in memory, this function will laod it.
    /// Panics if no table under the given id and order exists.
    pub fn get_trie_or_load<'a>(
        &'a mut self,
        id: TableId,
        order: &ColumnOrder,
    ) -> Result<&'a Trie, Error> {
        self.storage_handler
            .table_storage_mut(id, order)
            .expect("Function assumes that there is a table with the given id and order.")
            .into_memory(self.dict_constants.get_mut())
    }

    /// Given a node in the execution tree returns the trie iterator
    /// that if materialized will turn into the resulting trie of the represented computation
    fn get_iterator_node<'a>(
        &'a self,
        execution_node: ExecutionNodeRef,
        type_node: &TypeTreeNode,
        computation_results: &'a HashMap<usize, ComputationResult>,
    ) -> Result<Option<TrieScanEnum<'a>>, Error> {
        if type_node.schema.is_empty() {
            // That there is no schema for this node implies that the table is empty
            return Ok(None);
        }

        let node_rc = execution_node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        return match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                let trie_ref = self.get_trie(*id, order);
                if trie_ref.row_num() == 0 {
                    return Ok(None);
                }

                let schema = type_node.schema.clone();
                let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema.get_storage_types());

                Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
            }
            ExecutionOperation::FetchNew(index) => {
                let comp_result = computation_results.get(index).unwrap();
                let trie_ref = match comp_result {
                    ComputationResult::Temporary(trie) => trie,
                    ComputationResult::Permanent(id, order) => self.get_trie(*id, order),
                    ComputationResult::Empty => return Ok(None),
                };

                if trie_ref.row_num() == 0 {
                    return Ok(None);
                }

                let schema = type_node.schema.clone();
                let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema.get_storage_types());

                Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
            }
            ExecutionOperation::Join(subtables, bindings) => {
                let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
                for (table_index, subtable) in subtables.iter().enumerate() {
                    let subiterator_opt = self.get_iterator_node(
                        subtable.clone(),
                        &type_node.subnodes[table_index],
                        computation_results,
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
            ExecutionOperation::Union(subtables) => {
                let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
                for (table_index, subtable) in subtables.iter().enumerate() {
                    let subiterator_opt = self.get_iterator_node(
                        subtable.clone(),
                        &type_node.subnodes[table_index],
                        computation_results,
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
            ExecutionOperation::Minus(subtable_left, subtable_right) => {
                if let Some(left_scan) = self.get_iterator_node(
                    subtable_left.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )? {
                    if let Some(right_scan) = self.get_iterator_node(
                        subtable_right.clone(),
                        &type_node.subnodes[1],
                        computation_results,
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
            ExecutionOperation::Project(subnode, reorder) => {
                let subnode_rc = subnode.get_rc();
                let subnode_operation = &subnode_rc.borrow().operation;

                let trie = match subnode_operation {
                    ExecutionOperation::FetchExisting(id, order) => self.get_trie(*id, order),
                    ExecutionOperation::FetchNew(index) => {
                        let comp_result = computation_results.get(index).unwrap();
                        let trie_ref = match comp_result {
                            ComputationResult::Temporary(trie) => trie,
                            ComputationResult::Permanent(id, order) => self.get_trie(*id, order),
                            ComputationResult::Empty => return Ok(None),
                        };

                        trie_ref
                    }
                    _ => {
                        panic!("Project node has to have a Fetch node as its child.");
                    }
                };

                let project_scan = TrieScanProject::new(trie, reorder.clone());

                Ok(Some(TrieScanEnum::TrieScanProject(project_scan)))
            }
            ExecutionOperation::SelectValue(subtable, assignments) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;

                if let Some(subiterator) = subiterator_opt {
                    let select_scan = TrieScanSelectValue::new(
                        &mut self.dict_constants.borrow_mut(),
                        subiterator,
                        assignments,
                    );
                    Ok(Some(TrieScanEnum::TrieScanSelectValue(select_scan)))
                } else {
                    Ok(None)
                }
            }
            ExecutionOperation::SelectEqual(subtable, classes) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;

                if let Some(subiterator) = subiterator_opt {
                    let select_scan = TrieScanSelectEqual::new(subiterator, classes);
                    Ok(Some(TrieScanEnum::TrieScanSelectEqual(select_scan)))
                } else {
                    Ok(None)
                }
            }
            ExecutionOperation::AppendColumns(subtable, instructions) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;
                let target_types = type_node.schema.clone();

                if let Some(subiterator) = subiterator_opt {
                    let append_scan = TrieScanAppend::new(
                        &mut self.dict_constants.borrow_mut(),
                        subiterator,
                        instructions,
                        target_types.get_storage_types(),
                    );
                    Ok(Some(TrieScanEnum::TrieScanAppend(append_scan)))
                } else {
                    Ok(None)
                }
            }
            ExecutionOperation::AppendNulls(subtable, num_nulls) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;

                if let Some(subiterator) = subiterator_opt {
                    let nulls_scan = TrieScanNulls::new(subiterator, *num_nulls, self.current_null);
                    Ok(Some(TrieScanEnum::TrieScanNulls(nulls_scan)))
                } else {
                    Ok(None)
                }
            }
        };
    }
}

impl ByteSized for DatabaseInstance {
    fn size_bytes(&self) -> ByteSize {
        self.storage_handler.size_bytes()
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        columnar::traits::column::Column,
        datatypes::{DataTypeName, StorageValueT},
        management::{
            database::{ColumnOrder, TableId},
            ByteSized, ExecutionPlan,
        },
        tabular::{
            operations::JoinBindings,
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
        util::make_column_with_intervals_t,
    };

    use super::DatabaseInstance;

    #[test]
    fn basic_add_delete() {
        let column_a = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_b = make_column_with_intervals_t(&[1, 2, 3, 4, 5, 6], &[0]);

        let trie_a = Trie::new(vec![column_a]);
        let trie_b = Trie::new(vec![column_b]);

        let mut instance = DatabaseInstance::new();
        let mut reference_id = TableId::default();

        let mut schema_a = TableSchema::new();
        schema_a.add_entry(DataTypeName::U64);

        let trie_a_id = instance.register_table("A", schema_a);
        instance.add_trie(trie_a_id, ColumnOrder::default(), trie_a);

        assert_eq!(trie_a_id, reference_id.increment());
        assert_eq!(instance.table_name(trie_a_id), "A");
        assert_eq!(
            instance
                .get_trie(trie_a_id, &ColumnOrder::default())
                .row_num(),
            3
        );

        let last_size = instance.size_bytes();

        let mut schema_b = TableSchema::new();
        schema_b.add_entry(DataTypeName::U64);

        let trie_b_id = instance.register_table("B", schema_b);
        instance.add_trie(trie_b_id, ColumnOrder::default(), trie_b);

        assert_eq!(trie_b_id, reference_id.increment());
        assert!(instance.size_bytes() > last_size);
        assert_eq!(
            instance
                .get_trie(trie_b_id, &ColumnOrder::default())
                .row_num(),
            6
        );

        let last_size = instance.size_bytes();

        instance.delete(trie_a_id);
        assert!(instance.size_bytes() < last_size);
    }

    fn test_casting_execution_plan() -> (ExecutionPlan, usize) {
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

        let mut table_id = TableId::default();
        let id_a = table_id.increment();
        let id_b = table_id.increment();
        let id_c = table_id.increment();
        let id_x = table_id.increment();
        let id_y = table_id.increment();

        let mut execution_tree = ExecutionPlan::default();

        let node_load_a = execution_tree.fetch_existing(id_a);
        let node_load_b_1 = execution_tree.fetch_existing(id_b);
        let node_load_b_2 = execution_tree.fetch_existing(id_b);
        let node_load_c = execution_tree.fetch_existing(id_c);
        let node_load_x = execution_tree.fetch_existing(id_x);
        let node_load_y = execution_tree.fetch_existing(id_y);

        let node_minus = execution_tree.minus(node_load_x, node_load_y);

        let node_left_union = execution_tree.union(vec![node_load_a, node_load_b_1]);
        let node_right_union = execution_tree.union(vec![node_load_b_2, node_load_c]);

        let node_join = execution_tree.join(
            vec![node_left_union, node_right_union],
            JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        );

        let node_root = execution_tree.union(vec![node_join, node_minus]);
        let result_id = execution_tree.write_permanent(node_root, "Test", "Test");

        (execution_tree, result_id)
    }

    #[test]
    fn test_casting() {
        let trie_a = Trie::from_rows(&[vec![StorageValueT::U32(1), StorageValueT::U32(2)]]);
        let trie_b = Trie::from_rows(&[
            vec![StorageValueT::U32(2), StorageValueT::U64(1 << 35)],
            vec![StorageValueT::U32(3), StorageValueT::U64(2)],
        ]);
        let trie_c = Trie::from_rows(&[vec![StorageValueT::U32(2), StorageValueT::U64(4)]]);
        let trie_x = Trie::from_rows(&[
            vec![
                StorageValueT::U64(1),
                StorageValueT::U32(2),
                StorageValueT::U64(4),
            ],
            vec![
                StorageValueT::U64(3),
                StorageValueT::U32(2),
                StorageValueT::U64(4),
            ],
            vec![
                StorageValueT::U64(1 << 36),
                StorageValueT::U32(6),
                StorageValueT::U64(12),
            ],
        ]);
        let trie_y = Trie::from_rows(&[
            vec![
                StorageValueT::U32(1),
                StorageValueT::U64(2),
                StorageValueT::U32(4),
            ],
            vec![
                StorageValueT::U32(2),
                StorageValueT::U64(1 << 37),
                StorageValueT::U32(7),
            ],
        ]);

        let schema_a = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let schema_b = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_c = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let schema_x = TableSchema::from_vec(vec![
            DataTypeName::U64,
            DataTypeName::U32,
            DataTypeName::U64,
        ]);
        let schema_y = TableSchema::from_vec(vec![
            DataTypeName::U32,
            DataTypeName::U64,
            DataTypeName::U32,
        ]);

        let mut instance = DatabaseInstance::new();
        instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
        instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);
        instance.register_add_trie("TableC", schema_c, ColumnOrder::default(), trie_c);
        instance.register_add_trie("TableX", schema_x, ColumnOrder::default(), trie_x);
        instance.register_add_trie("TableY", schema_y, ColumnOrder::default(), trie_y);

        let (plan, node_id) = test_casting_execution_plan();
        let result = instance.execute_plan(plan);
        assert!(result.is_ok());

        let result_id = *result.unwrap().get(&node_id).unwrap();
        let result_trie = instance.get_trie(result_id, &ColumnOrder::default());

        let result_col_first = result_trie.get_column(0).as_u64().unwrap();
        let result_col_second = result_trie.get_column(1).as_u32().unwrap();
        let result_col_third = result_trie.get_column(2).as_u64().unwrap();

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
