use std::collections::{hash_map::Entry, HashMap};
use std::fmt::{Debug, Display};
use std::fs::File;
use std::path::PathBuf;

use bytesize::ByteSize;

use crate::io::csv::read;
use crate::physical::datatypes::{DataTypeName, DataValueT};
use crate::physical::tabular::traits::table::Table;
use crate::physical::util::Reordering;
use crate::{
    error::Error,
    meta::TimedCode,
    physical::{
        dictionary::Dictionary,
        tabular::{
            operations::{
                materialize::materialize, triescan_append::TrieScanAppend, TrieScanJoin,
                TrieScanMinus, TrieScanNulls, TrieScanProject, TrieScanSelectEqual,
                TrieScanSelectValue, TrieScanUnion,
            },
            table_types::trie::{Trie, TrieScanGeneric},
            traits::{table_schema::TableSchema, triescan::TrieScanEnum},
        },
    },
};

use super::column_order::ColumnOrder;
use super::{
    execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionResult},
    type_analysis::{TypeTree, TypeTreeNode},
    ByteSized, ExecutionPlan,
};

/// Type which represents table id.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TableId(u64);

impl Default for TableId {
    fn default() -> Self {
        Self(0)
    }
}

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
        self.0 = self.0 + 1;
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
    /// Table is contained in csv file
    CSV(PathBuf),
    /// Table is stored as facts in an rls file
    /// TODO: To not invoke the parser twice I just put the parsed "row-table" here.
    /// Does not seem quite right
    RLS(Vec<Vec<DataValueT>>),
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
    fn load_from_disk<Dict: Dictionary>(
        source: &TableSource,
        schema: &TableSchema,
        dict: &mut Dict,
    ) -> Result<Trie, Error> {
        {
            TimedCode::instance()
                .sub("Reasoning/Execution/Load Table")
                .start();
            log_load_table(&source);

            let trie = match source {
                TableSource::CSV(file) => {
                    // Using fallback solution to treat eveything as string for now (storing as u64 internally)
                    let datatypes: Vec<Option<DataTypeName>> =
                        (0..schema.arity()).map(|_| None).collect();

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

                    Trie::from_cols(col_table)
                }
                TableSource::RLS(table_rows) => Trie::from_rows(table_rows),
            };

            TimedCode::instance()
                .sub("Reasoning/Execution/Load Table")
                .stop();

            Ok(trie)
        }
    }

    /// Function that makes sure that underlying table is available in memory.
    pub fn into_memory<'a, Dict: Dictionary>(
        &'a mut self,
        dict: &mut Dict,
    ) -> Result<&'a Trie, Error> {
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

        if let TableStorage::InMemory(trie) = self {
            Ok(trie)
        } else {
            unreachable!();
        }
    }

    /// Return a reference to the stored trie.
    /// Pancics if the trie is not in memory.
    pub fn trie_unchecked(&self) -> &Trie {
        if let TableStorage::InMemory(trie) = self {
            trie
        } else {
            panic!("Function assumes that trie is in memory");
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
    Reference(TableId, Reordering),
}

/// Manages tables under different orders.
/// Also has the capability of representing a table as a reordered version of another.
#[derive(Debug)]
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
        if let TableStatus::Reference(ref_id, reorder) = &self.map.get(&id)? {
            self.resolve_reference_mut(*ref_id, &order.apply_reorder(&reorder.inverse()))
        } else {
            if let TableStatus::Present(map) = self.map.get_mut(&id)? {
                Some(TableResolvedMut {
                    map,
                    order: order.clone(),
                })
            } else {
                unreachable!()
            }
        }
    }

    fn resolve_reference(&self, id: TableId, order: &ColumnOrder) -> Option<TableResolved> {
        match &self.map.get(&id)? {
            TableStatus::Present(map) => Some(TableResolved {
                map,
                order: order.clone(),
            }),
            TableStatus::Reference(ref_id, reorder) => {
                self.resolve_reference(*ref_id, &order.apply_reorder(&reorder.inverse()))
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
    /// Panics if the given ids do not exist.
    pub fn add_reference(&mut self, id: TableId, reference_id: TableId, reorder: Reordering) {
        let (final_id, final_reorder) = if let TableStatus::Reference(ref_id, ref_reorder) =
            &self.map.get(&reference_id).unwrap()
        {
            (*ref_id, ref_reorder.chain(&reorder))
        } else {
            (id, reorder)
        };

        let status = TableStatus::Reference(final_id, final_reorder);
        self.map.insert(id, status);
    }

    /// Return a reference to the [`TableStorage`] associated with the given [`TableId`] and [`ColumnOrder`].
    /// Panics if the requested table does not exist.
    pub fn table_storage<'a>(&'a self, id: TableId, order: &ColumnOrder) -> &'a TableStorage {
        let resolved = self.resolve_reference(id, order).unwrap();
        resolved.map.get(&resolved.order).unwrap()
    }

    /// Return a mutable reference to the [`TableStorage`] associated with the given [`TableId`] and [`ColumnOrder`].
    /// Pancis if the requested table does not exist.
    pub fn table_storage_mut<'a>(
        &'a mut self,
        id: TableId,
        order: &ColumnOrder,
    ) -> &'a mut TableStorage {
        let resolved = self.resolve_reference_mut(id, order).unwrap();
        resolved.map.get_mut(&resolved.order).unwrap()
    }

    /// Return an iterator of all the available orders of a table.
    /// Panics if the requested table does not exist.
    pub fn available_orders<'a>(&'a self, id: TableId) -> impl Iterator<Item = &'a ColumnOrder> {
        if let TableStatus::Present(order_map) = self.map.get(&id).unwrap() {
            order_map.keys()
        } else {
            panic!("Function assumes that the given table exists.");
        }
    }

    /// Delete the given table.
    /// TODO: This does not check/fix references of tables.
    pub fn delete_table(&mut self, id: &TableId) {
        if let Entry::Occupied(entry) = self.map.entry(*id) {
            entry.remove();
        } else {
            panic!("Table to be deleted should exist.");
        }
    }
}

impl Default for OrderedReferenceManager {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
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
pub struct DatabaseInstance<Dict: Dictionary> {
    /// Structure which owns all the tries; accessed through Id.
    storage_handler: OrderedReferenceManager,
    /// Map with additional infromation about the tables
    table_infos: HashMap<TableId, TableInfo>,

    /// Dictionary which stores the strings associates with abstract constants
    dict_constants: Dict,

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

impl<Dict: Dictionary> DatabaseInstance<Dict> {
    /// Create new [`DatabaseInstance`]
    pub fn new(dict_constants: Dict) -> Self {
        let current_null = 1 << 63; // TODO: Think about a robust null representation method

        Self {
            storage_handler: OrderedReferenceManager::default(),
            table_infos: HashMap::new(),
            dict_constants,
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

    /// Returns a mutable reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants_mut(&mut self) -> &mut Dict {
        &mut self.dict_constants
    }

    /// Returns a reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants(&self) -> &Dict {
        &self.dict_constants
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
    pub fn add_reference(&mut self, id: TableId, reference_id: TableId, reorder: Reordering) {
        self.storage_handler
            .add_reference(id, reference_id, reorder);
    }

    /// Deletes a table with all its orders.
    /// TODO: For now, this does not care about keeping references intact.
    /// Panics if the table does not exist.
    pub fn delete(&mut self, id: TableId) {
        self.storage_handler.delete_table(&id);

        if let Entry::Occupied(entry) = self.table_infos.entry(id) {
            entry.remove();
        } else {
            panic!("Table to be deleted should exist.");
        }
    }

    /// Helper function that iterates through a collection of [`ColumnOrder`]
    /// to find the one that is "closest" to given one.
    fn search_closest_order<'a, OrderIter: Iterator<Item = &'a ColumnOrder>>(
        iter_orders: OrderIter,
        order: &ColumnOrder,
    ) -> Option<&'a ColumnOrder> {
        iter_orders.min_by(|x, y| x.distance(order).cmp(&y.distance(order)))
    }

    /// Will ensure that the requested table will exist as a [`Trie`] in memory.
    /// More precisely this will
    ///     * Reorder an existing table if the table is not available in the requested order
    ///     * Load a table from disk if it currently not in memory
    /// Panics if the requested table does not exist.
    fn make_available_in_memory<'a>(
        &'a mut self,
        id: TableId,
        order: &ColumnOrder,
    ) -> Result<(), Error> {
        let arity = self.table_arity(id);
        let closest_order =
            Self::search_closest_order(self.storage_handler.available_orders(id), order).expect(
                "This function assumes that there is at least one table under the given id.",
            );

        let reorder = order.reorder_to(closest_order, arity);
        let trie_unordered = self
            .storage_handler
            .table_storage_mut(id, &closest_order.clone())
            .into_memory(&mut self.dict_constants)?;

        if !reorder.is_identity() {
            let mut scan_project =
                TrieScanEnum::TrieScanProject(TrieScanProject::new(trie_unordered, reorder));
            let trie_reordered = materialize(&mut scan_project).unwrap();

            self.add_trie(id, order.clone(), trie_reordered);
        }

        Ok(())
    }

    // Helper function which checks whether the top level tree node is of type `AppendNulls`.
    // If this is the case returns the amount of null-columns that have been appended.
    // TODO: Nothing about this feels right; revise later
    fn appends_nulls(node: ExecutionNodeRef) -> u64 {
        let node_rc = node
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
        let node_ref = &*node_rc.as_ref().borrow();

        match node_ref {
            ExecutionNode::AppendNulls(_subnode, num_nulls) => *num_nulls as u64,
            _ => 0,
        }
    }

    /// Executes a given [`ExecutionPlan`].
    /// Returns a map that assigns to each plan id of a permanenet table the [`TableId`] in the [`DatabaseInstance`]
    /// This may fail if certain operations are performed on tries with incompatible types
    /// or if the plan references tries that do not exist.
    pub fn execute_plan(
        &mut self,
        mut plan: ExecutionPlan,
    ) -> Result<HashMap<usize, TableId>, Error> {
        let mut permanent_ids = HashMap::<usize, TableId>::new();
        let mut type_trees = HashMap::<usize, TypeTree>::new();
        let mut computation_results = HashMap::<usize, ComputationResult>::new();

        for (&tree_index, execution_tree) in plan.iter() {
            // This is a hack to get the arity of the over all tree beforehand ...
            // TODO: Remove this
            let type_tree = TypeTree::from_execution_tree(self, &type_trees, execution_tree)?;
            let arity = type_tree.schema.arity();
            execution_tree.satisfy_leapfrog_triejoin(arity);

            let type_tree = TypeTree::from_execution_tree(self, &type_trees, execution_tree)?;
            let schema = type_tree.schema.clone();

            for (id, order) in execution_tree.required_tables() {
                self.make_available_in_memory(id, &order)?;
            }

            let timed_string = format!("Reasoning/Execution/{}", execution_tree.name());
            TimedCode::instance().sub(&timed_string).start();

            let mut num_null_columns = 0u64;

            // Calculate the new trie
            let new_trie_opt = if let Some(root) = execution_tree.root() {
                num_null_columns = Self::appends_nulls(root.clone());

                let iter_opt = self.get_iterator_node(root, &type_tree, &computation_results)?;

                if let Some(mut iter) = iter_opt {
                    materialize(&mut iter)
                } else {
                    None
                }
            } else {
                None
            };

            type_trees.insert(tree_index, type_tree);

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

                        computation_results
                            .insert(tree_index, ComputationResult::Temporary(new_trie));
                    }
                    ExecutionResult::Permanent(order, name) => {
                        log::info!(
                            "Saved permanent table: {} entries ({})",
                            new_trie.row_num(),
                            new_trie.size_bytes()
                        );

                        let new_id = self.register_table(name, schema);
                        self.add_trie(new_id, order.clone(), new_trie);

                        permanent_ids.insert(tree_index, new_id);
                        computation_results.insert(
                            tree_index,
                            ComputationResult::Permanent(new_id, order.clone()),
                        );
                    }
                }
            } else {
                log_empty_trie();

                computation_results.insert(tree_index, ComputationResult::Empty);
            }

            TimedCode::instance().sub(&timed_string).stop();
        }

        Ok(permanent_ids)
    }

    /// Return a reference to a trie that is assumed to be in memory.
    /// Panics if the trie is not in memory.
    pub fn get_trie_inmemory<'a>(&'a self, id: TableId, order: &ColumnOrder) -> &'a Trie {
        if let TableStorage::InMemory(trie) = self.storage_handler.table_storage(id, order) {
            trie
        } else {
            panic!("Function assumes that trie is in memory.");
        }
    }

    /// Return a reference to a trie identified by its id and order.
    /// If the trie is not available in memory, this function will laod it.
    pub fn get_trie<'a>(&'a mut self, id: TableId, order: &ColumnOrder) -> Result<&'a Trie, Error> {
        self.storage_handler
            .table_storage_mut(id, order)
            .into_memory(&mut self.dict_constants)
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
        let node_ref = &*node_rc.borrow();

        return match node_ref {
            ExecutionNode::FetchExisting(id, order) => {
                let trie_ref = self.get_trie_inmemory(*id, order);
                if trie_ref.num_elements() == 0 {
                    return Ok(None);
                }

                let schema = type_node.schema.get_column_types();
                let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema);

                Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
            }
            ExecutionNode::FetchNew(index) => {
                let comp_result = computation_results.get(index).unwrap();
                let trie_ref = match comp_result {
                    ComputationResult::Temporary(trie) => trie,
                    ComputationResult::Permanent(id, order) => self.get_trie_inmemory(*id, order),
                    ComputationResult::Empty => return Ok(None),
                };

                let schema = type_node.schema.get_column_types();
                let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema);

                Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
            }
            ExecutionNode::Join(subtables, bindings) => {
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
            ExecutionNode::Union(subtables) => {
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
            ExecutionNode::Minus(subtable_left, subtable_right) => {
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
            ExecutionNode::Project(subnode, sorting) => {
                let subnode_rc = subnode.get_rc();
                let subnode_ref = &*subnode_rc.borrow();

                let trie = match subnode_ref {
                    ExecutionNode::FetchExisting(id, order) => self.get_trie_inmemory(*id, order),
                    ExecutionNode::FetchNew(index) => {
                        let comp_result = computation_results.get(index).unwrap();
                        let trie_ref = match comp_result {
                            ComputationResult::Temporary(trie) => trie,
                            ComputationResult::Permanent(id, order) => {
                                self.get_trie_inmemory(*id, order)
                            }
                            ComputationResult::Empty => return Ok(None),
                        };

                        trie_ref
                    }
                    _ => {
                        panic!("Project node has to have a Fetch node as its child.");
                    }
                };

                let project_scan = TrieScanProject::new(trie, sorting.clone());

                Ok(Some(TrieScanEnum::TrieScanProject(project_scan)))
            }
            ExecutionNode::SelectValue(subtable, assignments) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;

                if let Some(subiterator) = subiterator_opt {
                    let select_scan = TrieScanSelectValue::new(subiterator, assignments);
                    Ok(Some(TrieScanEnum::TrieScanSelectValue(select_scan)))
                } else {
                    Ok(None)
                }
            }
            ExecutionNode::SelectEqual(subtable, classes) => {
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
            ExecutionNode::AppendColumns(subtable, instructions) => {
                let subiterator_opt = self.get_iterator_node(
                    subtable.clone(),
                    &type_node.subnodes[0],
                    computation_results,
                )?;
                let target_types = type_node.schema.get_column_types();

                if let Some(subiterator) = subiterator_opt {
                    let append_scan = TrieScanAppend::new(subiterator, instructions, target_types);
                    Ok(Some(TrieScanEnum::TrieScanAppend(append_scan)))
                } else {
                    Ok(None)
                }
            }
            ExecutionNode::AppendNulls(subtable, num_nulls) => {
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

impl<Dict: Dictionary> ByteSized for DatabaseInstance<Dict> {
    fn size_bytes(&self) -> ByteSize {
        self.storage_handler.size_bytes()
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        columnar::traits::column::Column,
        datatypes::{DataTypeName, DataValueT},
        dictionary::StringDictionary,
        management::{
            column_order::ColumnOrder, database::TableId, execution_plan::ExecutionTree, ByteSized,
            ExecutionPlan,
        },
        tabular::{
            table_types::trie::Trie,
            traits::{
                table::Table,
                table_schema::{TableSchema, TableSchemaEntry},
            },
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

        let mut instance = DatabaseInstance::<_>::new(StringDictionary::default());
        let mut reference_id = TableId::default();

        let mut schema_a = TableSchema::new();
        schema_a.add_entry(DataTypeName::U64, false, false);

        let trie_a_id = instance.register_table("A", schema_a);
        instance.add_trie(trie_a_id, ColumnOrder::default(), trie_a);

        assert_eq!(trie_a_id, reference_id.increment());
        assert_eq!(instance.table_name(trie_a_id), "A");
        assert_eq!(
            instance
                .get_trie_inmemory(trie_a_id, &ColumnOrder::default())
                .row_num(),
            3
        );

        let last_size = instance.size_bytes();

        let mut schema_b = TableSchema::new();
        schema_b.add_entry(DataTypeName::U64, false, false);

        let trie_b_id = instance.register_table("B", schema_b);
        instance.add_trie(trie_b_id, ColumnOrder::default(), trie_b);

        assert_eq!(trie_b_id, reference_id.increment());
        assert!(instance.size_bytes() > last_size);
        assert_eq!(
            instance
                .get_trie_inmemory(trie_b_id, &ColumnOrder::default())
                .row_num(),
            6
        );

        let last_size = instance.size_bytes();

        instance.delete(trie_a_id);
        assert!(instance.size_bytes() < last_size);
    }

    fn schema_entry(type_name: DataTypeName) -> TableSchemaEntry {
        TableSchemaEntry {
            type_name,
            dict: false,
            nullable: false,
        }
    }

    fn test_casting_execution_plan() -> ExecutionPlan {
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

        let mut execution_tree = ExecutionTree::new_permanent("Test", "TableResult");

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
            vec![vec![0, 1], vec![1, 2]],
        );

        let node_root = execution_tree.union(vec![node_join, node_minus]);
        execution_tree.set_root(node_root);

        let mut execution_plan = ExecutionPlan::new();
        execution_plan.push(execution_tree);

        execution_plan
    }

    #[test]
    fn test_casting() {
        let trie_a = Trie::from_rows(&[vec![DataValueT::U32(1), DataValueT::U32(2)]]);
        let trie_b = Trie::from_rows(&[
            vec![DataValueT::U32(2), DataValueT::U64(1 << 35)],
            vec![DataValueT::U32(3), DataValueT::U64(2)],
        ]);
        let trie_c = Trie::from_rows(&[vec![DataValueT::U32(2), DataValueT::U64(4)]]);
        let trie_x = Trie::from_rows(&[
            vec![DataValueT::U64(1), DataValueT::U32(2), DataValueT::U64(4)],
            vec![DataValueT::U64(3), DataValueT::U32(2), DataValueT::U64(4)],
            vec![
                DataValueT::U64(1 << 36),
                DataValueT::U32(6),
                DataValueT::U64(12),
            ],
        ]);
        let trie_y = Trie::from_rows(&[
            vec![DataValueT::U32(1), DataValueT::U64(2), DataValueT::U32(4)],
            vec![
                DataValueT::U32(2),
                DataValueT::U64(1 << 37),
                DataValueT::U32(7),
            ],
        ]);

        let schema_a = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_b = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_c = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_x = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_y = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);

        let mut instance = DatabaseInstance::<_>::new(StringDictionary::default());
        instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
        instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);
        instance.register_add_trie("TableC", schema_c, ColumnOrder::default(), trie_c);
        instance.register_add_trie("TableX", schema_x, ColumnOrder::default(), trie_x);
        instance.register_add_trie("TableY", schema_y, ColumnOrder::default(), trie_y);

        let plan = test_casting_execution_plan();
        let result = instance.execute_plan(plan);
        assert!(result.is_ok());

        let result_id = *result.unwrap().get(&0).unwrap();
        let result_trie = instance.get_trie_inmemory(result_id, &ColumnOrder::default());

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
