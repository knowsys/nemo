//! This module defines [DatabaseInstance],
//! which is used to manage a collection of tables.

pub mod id;
pub mod sources;

pub(crate) mod execution_series;

mod order;
mod storage;

use std::{
    cell::{Ref, RefCell},
    collections::HashMap,
    error::Error,
    fmt::Debug,
};

use bytesize::ByteSize;

use crate::{
    datasources::table_providers::TableProvider,
    datavalues::AnyDataValueIterator,
    management::{database::execution_series::ExecutionTreeRoot, ByteSized},
    meta::TimedCode,
    tabular::{
        operations::{prune::TrieScanPrune, OperationGenerator},
        trie::Trie,
        triescan::TrieScanEnum,
    },
    util::mapping::permutation::Permutation,
};

use self::{
    execution_series::{ExecutionTree, ExecutionTreeLeaf, ExecutionTreeOperation},
    id::{ExecutionId, PermanentTableId, TableId},
    order::{OrderedReferenceManager, StorageId},
    sources::{SimpleTable, TableSource},
};

use super::{
    execution_plan::{ColumnOrder, ExecutionResult},
    ExecutionPlan,
};

/// Dictionary Implementation used in the current configuration
pub type Dict = crate::dictionary::meta_dv_dict::MetaDvDictionary;

/// Struct that contains useful information about a trie
/// as well as the actual owner of the trie.
#[derive(Debug)]
struct TableInfo {
    /// The name of the table
    name: String,
    /// The number of columns stored in the table
    arity: usize,
}

impl TableInfo {
    /// Create new [TableInfo].
    fn new(name: String, arity: usize) -> Self {
        Self { name, arity }
    }
}

/// Represents a collection of tables
#[derive(Debug, Default)]
pub struct DatabaseInstance {
    /// Helper object to manager references and different ordering of the same table
    reference_manager: OrderedReferenceManager,
    /// Associates each table id with additional information
    table_infos: HashMap<PermanentTableId, TableInfo>,

    /// Dictionary that represents the general mapping between datavalues and integer ids
    /// used in all tables of this database
    dictionary: RefCell<Dict>,

    /// The lowest unused [PermanentTableId]
    ///
    /// This will be incremented for each new table.
    current_id: PermanentTableId,
}

// Return basic information about tables managed by the database
impl DatabaseInstance {
    /// Return the current number of tables.
    pub fn num_tables(&self) -> usize {
        self.table_infos.len()
    }

    /// Return the number of rows for a given table.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn table_rows(&self, table_id: PermanentTableId) -> usize {
        self.reference_manager.count_rows(table_id)
    }

    /// Return the name of a table given its [PermanentTableId].
    ///
    /// # Panics
    /// Panics if the id does not exist.
    pub fn table_name(&self, id: PermanentTableId) -> &str {
        &self
            .table_infos
            .get(&id)
            .expect("No table with id {id} exists.")
            .name
    }

    /// Return the arity of a table identified by the given [PermanentTableId].
    ///
    /// # Panics
    /// Panics if the id does not exist.
    pub fn table_arity(&self, id: PermanentTableId) -> usize {
        self.table_infos
            .get(&id)
            .expect("No table with id {id} exists.")
            .arity
    }

    /// Returns a reference to the dictionary used for associating abstract constants with strings.
    pub fn dictionary(&self) -> Ref<'_, Dict> {
        self.dictionary.borrow()
    }

    /// Return the amount of memory consumed by the table under the given [PermanentTableId].
    /// This also includes additional index structures but excludes tables that are currently stored on disk.
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn memory_consumption(&self, id: PermanentTableId) -> ByteSize {
        self.reference_manager.memory_consumption(id)
    }

    /// Return the number of rows contained in this table.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn count_rows(&self, id: PermanentTableId) -> usize {
        self.reference_manager.count_rows(id)
    }

    /// Get a list of column iterators for the full table (i.e. the expanded trie)
    pub fn get_table_column_iterators(
        &mut self,
        id: PermanentTableId,
    ) -> Result<Vec<AnyDataValueIterator>, Box<dyn Error>> {
        // Make sure trie is loaded
        let storage_id =
            self.reference_manager
                .trie_id(&self.dictionary, id, ColumnOrder::default())?;
        let trie = self.reference_manager.trie(storage_id);

        Ok(trie.full_column_iterators())
    }
}

// Add new tables to the database
impl DatabaseInstance {
    /// Register a new table under a given name and schema.
    /// Returns the [PermanentTableId] with which the new table can be addressed.
    pub fn register_table(&mut self, name: &str, arity: usize) -> PermanentTableId {
        self.table_infos
            .insert(self.current_id, TableInfo::new(String::from(name), arity));

        self.current_id.increment()
    }

    /// Add a new trie.
    fn add_trie(&mut self, id: PermanentTableId, order: ColumnOrder, trie: Trie) {
        self.reference_manager.add_trie(id, order, trie);
    }

    /// Register table and add a new trie.
    pub fn register_add_trie(
        &mut self,
        name: &str,
        order: ColumnOrder,
        trie: Trie,
    ) -> PermanentTableId {
        let arity = trie.arity();
        let id = self.register_table(name, arity);
        self.add_trie(id, order, trie);

        id
    }

    /// Add a table represented by a list of [TableSources].
    pub fn add_sources(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        sources: Vec<TableSource>,
    ) {
        self.reference_manager.add_sources(id, order, sources);
    }

    /// Add [TableProvider] which loads the table.
    pub fn add_source_external(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        provider: Box<dyn TableProvider>,
    ) {
        let arity = self.table_arity(id);

        self.reference_manager
            .add_source(id, order, TableSource::External(provider, arity));
    }

    /// Add a table given as [SimpleTable].
    pub fn add_source_table(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        table: SimpleTable,
    ) {
        self.reference_manager
            .add_source(id, order, TableSource::SimpleTable(table));
    }

    /// Add a new table that is a reordered version of an existing table.
    /// Panics if referenced id does not exist.
    pub fn add_reference(
        &mut self,
        id: PermanentTableId,
        reference_id: PermanentTableId,
        permutation: Permutation,
    ) {
        self.reference_manager
            .add_reference(id, reference_id, permutation);
    }
}

/// Result of the computation of an [ExecutionTree]
#[derive(Debug)]
struct ComputationResult {
    /// How to store the result
    storage: ExecutionResult,
    /// Id of the execution node
    execution_id: ExecutionId,
    /// The result of the computation
    trie: Option<Trie>,
}

/// Contains
#[derive(Debug)]
struct TemporaryStorage {
    /// Tables that were loaded from the [DatabaseInstance] before the computation
    loaded_tables: Vec<StorageId>,
    /// Tables that were computed during the execution of an [ExecutionPlan]
    computed_tables: Vec<ComputationResult>,
}

// Functions for computing results of execution plans
impl DatabaseInstance {
    /// For a given list of [PermanentTableId] and [ColumnOrder] pairs,
    /// make sure that the tables represented by those ids and orders
    /// exist as [Trie]s and return a list with the [StorageId]s
    /// to obtain them from `self.reference_manager`
    fn collect_requiured_tries(
        &mut self,
        tables: &[(PermanentTableId, ColumnOrder)],
    ) -> Result<Vec<StorageId>, Box<dyn Error>> {
        let mut result = Vec::new();

        for (id, order) in tables.iter().cloned() {
            result.push(
                self.reference_manager
                    .trie_id(&self.dictionary, id, order)?,
            );
        }

        Ok(result)
    }

    /// Return a [TrieScanEnum] representing the given [ExecutionTreeLeaf] node.
    ///
    /// Returns `None` if a previous computation represented by this node was empty.
    fn evaluate_tree_leaf<'a>(
        &'a self,
        storage: &'a TemporaryStorage,
        leaf: &ExecutionTreeLeaf,
    ) -> Option<TrieScanEnum<'a>> {
        let trie = match leaf {
            ExecutionTreeLeaf::LoadTable(load_id) => {
                Some(self.reference_manager.trie(storage.loaded_tables[*load_id]))
            }
            ExecutionTreeLeaf::FetchComputedTable(computed_id) => {
                storage.computed_tables[*computed_id].trie.as_ref()
            }
        };

        trie.map(|trie| TrieScanEnum::TrieScanGeneric(trie.iter()))
    }

    /// Return a [TrieScanEnum] representing the given [ExecutionTreeOperation] node.
    ///
    /// Returns `None` if a previous computation represented by this node was empty
    /// or it can be known that evaulating this operation would result in an empty table.
    fn evaluate_operation<'a>(
        &'a self,
        dictionary: &'a Dict,
        storage: &'a TemporaryStorage,
        operation: &ExecutionTreeOperation,
    ) -> Option<TrieScanEnum<'a>> {
        match operation {
            ExecutionTreeOperation::Leaf(leaf) => self.evaluate_tree_leaf(storage, leaf),
            ExecutionTreeOperation::Node {
                generator,
                subnodes,
            } => {
                let mut input_scans = subnodes
                    .into_iter()
                    .map(|subnode| self.evaluate_operation(dictionary, storage, subnode))
                    .filter_map(|result_option| result_option)
                    .collect::<Vec<_>>();

                if input_scans.is_empty() {
                    return None;
                }

                if generator.is_unary_identity() && input_scans.len() == 1 {
                    return Some(input_scans.remove(0));
                }

                Some(generator.generate(input_scans, dictionary))
            }
        }
    }

    /// Evaluate the tree of operations represented by the [ExecutionTree].
    fn execute_tree<'a>(
        &'a self,
        storage: &'a TemporaryStorage,
        tree: &ExecutionTree,
    ) -> Result<ComputationResult, Box<dyn Error>> {
        let dictionary = &self.dictionary.borrow();

        let trie = match &tree.root {
            ExecutionTreeRoot::Operation(operation) => {
                let trie_scan = self.evaluate_operation(dictionary, storage, operation);
                trie_scan
                    .map(|scan| Trie::from_trie_scan(TrieScanPrune::new(scan), tree.cut_layers))
            }
            ExecutionTreeRoot::ProjectReorder { generator, subnode } => self
                .evaluate_tree_leaf(storage, subnode)
                .map(|scan| generator.apply_operation(TrieScanPrune::new(scan))),
        };

        Ok(ComputationResult {
            storage: tree.result.clone(),
            execution_id: tree.execution_id,
            trie,
        })
    }

    // log::info!("Execution step: {}", execution_tree.name());

    //             execution_tree.satisfy_leapfrog_triejoin();

    //             if let Some(simplified_tree) = execution_tree.simplify(&removed_temp_ids) {
    //                 execution_tree = simplified_tree;
    //             } else {
    //                 removed_temp_ids.insert(tree_id);
    //                 log::info!("Result is empty. No computation was required.");

    //                 continue;
    //             }

    //             TimedCode::instance()
    //                 .sub("Reasoning/Execution/Load Table")
    //                 .start();

    //             for (id, order) in execution_tree.required_tables() {
    //                 self.make_available_in_memory(id, &order)?;
    //             }

    //             TimedCode::instance()
    //                 .sub("Reasoning/Execution/Load Table")
    //                 .stop();

    //             let timed_string = format!("Reasoning/Execution/{}", execution_tree.name());
    //             TimedCode::instance().sub(&timed_string).start();

    //             let type_tree = TypeTree::from_execution_tree(self, &type_trees, &execution_tree)?;
    //             let schema = type_tree.schema.clone();

    //             let num_null_columns = Self::appends_nulls(execution_tree.root());

    //             let new_trie_opt =
    //                 self.evaluate_execution_tree(&execution_tree, &type_tree, &computation_results)?;
    //             type_trees.insert(tree_id, type_tree);

    //             if let Some(new_trie) = new_trie_opt {
    //                 // If trie appended nulls then we need to update our current_null value
    //                 self.current_null += new_trie.row_num() as u64 * num_null_columns;

    //                 // Add new trie to the appropriate place
    //                 match execution_tree.result() {
    //                     ExecutionResult::Temporary => {
    //                         log::info!(
    //                             "Saved temporary table: {} entries ({})",
    //                             new_trie.row_num(),
    //                             new_trie.size_bytes()
    //                         );

    //                         if new_trie.row_num() > 0 || new_trie.get_types().is_empty() {
    //                             computation_results
    //                                 .insert(tree_id, ComputationResult::Temporary(Some(new_trie)));
    //                         } else {
    //                             computation_results.insert(tree_id, ComputationResult::Temporary(None));
    //                         }
    //                     }
    //                     ExecutionResult::Permanent(order, name) => {
    //                         let new_id = self.register_table(name, schema);

    //                         log::info!(
    //                             "Saved permanent table {new_id} - {name} with {} entries ({})",
    //                             new_trie.row_num(),
    //                             new_trie.size_bytes()
    //                         );

    //                         self.add_trie(new_id, order.clone(), new_trie);

    //                         permanent_ids.insert(tree_id, new_id);
    //                         computation_results
    //                             .insert(tree_id, ComputationResult::Permanent(new_id, order.clone()));
    //                     }
    //                 }
    //             } else {
    //                 log::info!("Trie does not contain any elements");

    //                 computation_results.insert(tree_id, ComputationResult::Empty);
    //             }

    //             TimedCode::instance().sub(&timed_string).stop();
    //         }

    //         Ok(permanent_ids)

    /// Evaluate the given [ExecutionPlan].
    pub fn execute_plan(
        &mut self,
        plan: ExecutionPlan,
    ) -> Result<HashMap<ExecutionId, PermanentTableId>, Box<dyn Error>> {
        let exeuction_series = plan.finalize();

        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .start();

        let mut temporary_storage = TemporaryStorage {
            loaded_tables: self.collect_requiured_tries(&exeuction_series.loaded_tries)?,
            computed_tables: Vec::<ComputationResult>::new(),
        };

        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .stop();

        for tree in &exeuction_series.trees {
            log::info!("Execution step: {}", tree.operation_name);
            let timed_string = format!("Reasoning/Execution/{}", tree.operation_name);

            TimedCode::instance().sub(&timed_string).start();
            let result = self.execute_tree(&temporary_storage, tree)?;
            TimedCode::instance().sub(&timed_string).stop();

            if result.trie.is_some() && result.trie.as_ref().unwrap().num_rows() > 0 {
                let new_trie = result.trie.as_ref().unwrap();

                match &tree.result {
                    ExecutionResult::Temporary => {
                        log::info!(
                            "Saved temporary table: {} entries ({})",
                            new_trie.num_rows(),
                            new_trie.size_bytes()
                        );
                    }
                    ExecutionResult::Permanent(_, name) => {
                        log::info!(
                            "Saved permanent table {name} with {} entries ({})",
                            new_trie.num_rows(),
                            new_trie.size_bytes()
                        );
                    }
                }
            } else {
                log::info!("Trie does not contain any elements");
            }

            temporary_storage.computed_tables.push(result);
        }

        let mut result = HashMap::new();
        for computation in temporary_storage.computed_tables {
            match computation.storage {
                ExecutionResult::Temporary => {} // Temporary table will be dropped at the end of the function
                ExecutionResult::Permanent(order, name) => {
                    if let Some(trie) = computation.trie {
                        let permanent_id = self.register_add_trie(&name, order, trie);
                        let execution_id = computation.execution_id;

                        result.insert(execution_id, permanent_id);
                    }
                }
            }
        }

        Ok(result)
    }
}

// /// Result of executing an [ExecutionTree].
// enum ComputationResult {
//     /// Resulting trie is only stored temporarily within this object.
//     Temporary(Option<Trie>),
//     /// Trie is stored permanently under a [TableId] and [ColumnOrder].
//     Permanent(TableId, ColumnOrder),
//     /// The computation resulted in an empty trie.
//     Empty,
// }

// impl Default for DatabaseInstance {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl DatabaseInstance {
//     /// Return the number of rows for a given table.
//     ///
//     /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
//     pub fn count_rows(&self, table_id: &TableId) -> usize {
//         self.storage_handler.count_rows(table_id)
//     }

//     /// Return the current number of tables.
//     pub fn num_tables(&self) -> usize {
//         self.table_infos.len()
//     }

//     /// Return the name of a table given its id.
//     /// Panics if the id does not exist.
//     pub fn table_name(&self, id: TableId) -> &str {
//         &self.table_infos.get(&id).unwrap().name
//     }

//     /// Return the schema of a table identified by the given [TableId].
//     /// Panics if the id does not exist.
//     pub fn table_schema(&self, id: TableId) -> &TableSchema {
//         &self.table_infos.get(&id).unwrap().schema
//     }

//     /// Return the arity of a table identified by the given [TableId].
//     /// Panics if the id does not exist.
//     pub fn table_arity(&self, id: TableId) -> usize {
//         self.table_schema(id).arity()
//     }

//     /// Returns a reference to the dictionary used for associating abstract constants with strings.
//     pub fn dict(&self) -> Ref<'_, Dict> {
//         self.dict.borrow()
//     }

//     /// Register a new table under a given name and schema.
//     /// Returns the [TableId] with which the new table can be addressed.
//     pub fn register_table(&mut self, name: &str, schema: TableSchema) -> TableId {
//         self.table_infos
//             .insert(self.current_id, TableInfo::new(String::from(name), schema));

//         self.current_id.increment()
//     }

//     /// Add a new trie.
//     fn add_trie(&mut self, id: TableId, order: ColumnOrder, trie: Trie) {
//         self.storage_handler
//             .add_present(id, order, TableStorage::InMemory(trie));
//     }

//     /// Register table and add a new trie.
//     pub fn register_add_trie(
//         &mut self,
//         name: &str,
//         schema: TableSchema,
//         order: ColumnOrder,
//         trie: Trie,
//     ) -> TableId {
//         let id = self.register_table(name, schema);
//         self.add_trie(id, order, trie);

//         id
//     }

//     /// Add the sources of a table currently stored on disk.
//     pub fn add_sources(&mut self, id: TableId, order: ColumnOrder, sources: Vec<TableSource>) {
//         let schema = self.table_schema(id).clone();
//         self.storage_handler
//             .add_present(id, order, TableStorage::OnDisk(schema, sources));
//     }

//     /// Add a new table that is a reordered version of an existing table.
//     /// Panics if referenced id does not exist.
//     pub fn add_reference(&mut self, id: TableId, reference_id: TableId, permutation: Permutation) {
//         self.storage_handler
//             .add_reference(id, reference_id, permutation);
//     }

//     /// Deletes a table with all its orders.
//     /// TODO: For now, this does not care about keeping references intact.
//     /// Panics if the table does not exist.
//     pub fn delete(&mut self, id: TableId) {
//         self.storage_handler
//             .delete_table(&id)
//             .expect("Table to be deleted should exist.");
//         self.table_infos
//             .remove(&id)
//             .expect("Table to be deleted should exist.");
//     }

//     /// Prunes and materializes a trie scan by
//     /// * either unwrapping an [TrieScanAggregateWrapper]
//     /// * or otherwise wrapping the [TrieScanEnum] using a [TrieScanPrune].
//     fn materialized_trie_scan(trie_scan: TrieScanEnum<'_>, cut_bottom: usize) -> Option<Trie> {
//         match trie_scan {
//             TrieScanEnum::TrieScanAggregateWrapper(mut aggregate_wrapper) => {
//                 materialize_up_to(&mut aggregate_wrapper.trie_scan, cut_bottom)
//             }
//             _ => materialize_up_to(&mut TrieScanPrune::new(trie_scan), cut_bottom),
//         }
//     }

//     /// Produces a new [Trie] by executing a [ExecutionTree].
//     ///
//     /// # Panics
//     /// Panics if the tables that are being loaded by the [ExecutionTree] are not available in memory.
//     /// Also panics if the [ExecutionTree] wants to perform a project/reorder operation on a non-materialized trie.
//     fn evaluate_execution_tree(
//         &self,
//         execution_tree: &ExecutionTree,
//         type_tree: &TypeTree,
//         computation_results: &HashMap<usize, ComputationResult>,
//     ) -> Result<Option<Trie>, Error> {
//         let root_rc = execution_tree.root().get_rc();
//         let root_operation = &root_rc.borrow().operation;

//         if let ExecutionOperation::Project(subnode, reordering) = root_operation {
//             let schema_less = reordering.iter().collect::<Vec<_>>().is_empty();

//             let subnode_rc = subnode.get_rc();
//             let subnode_operation = &subnode_rc.borrow().operation;

//             match subnode_operation {
//                 ExecutionOperation::FetchExisting(id, order) => {
//                     let trie_ref = self.get_trie_order(*id, order);
//                     if trie_ref.row_num() == 0 {
//                         return Ok(None);
//                     }

//                     if !schema_less {
//                         Ok(Some(project_and_reorder(trie_ref, reordering)))
//                     } else {
//                         Ok(Some(Trie::new(vec![])))
//                     }
//                 }
//                 ExecutionOperation::FetchNew(index) => {
//                     let comp_result = computation_results.get(index).unwrap();
//                     let trie_ref = match comp_result {
//                         ComputationResult::Temporary(trie_opt) => {
//                             if let Some(trie) = trie_opt {
//                                 trie
//                             } else {
//                                 return Ok(None);
//                             }
//                         }
//                         ComputationResult::Permanent(id, order) => self.get_trie_order(*id, order),
//                         ComputationResult::Empty => return Ok(None),
//                     };

//                     if !schema_less {
//                         Ok(Some(project_and_reorder(trie_ref, reordering)))
//                     } else {
//                         Ok(Some(Trie::new(vec![])))
//                     }
//                 }
//                 _ => panic!(
//                     "The project/reorder operation can only be applied to materialized tries."
//                 ),
//             }
//         } else {
//             let iter_opt =
//                 self.get_iterator_node(execution_tree.root(), type_tree, computation_results)?;
//             let cut_bottom = execution_tree.cut_bottom();

//             Ok(iter_opt.and_then(|iter| Self::materialized_trie_scan(iter, cut_bottom)))
//         }
//     }

//     /// Evaluates an [ExecutionTree] until until it finds the first row in the result and returns it.
//     /// Returns None if the tree evaluates to the empty table.
//     ///
//     /// # Panics
//     /// Panics if the tables that are being loaded by the [ExecutionTree] are not available in memory.
//     /// Also panics if the [ExecutionTree] wants to perform a project/reorder operation on a non-materialized trie.
//     fn evaluate_execution_tree_first_match(
//         &self,
//         execution_tree: &ExecutionTree,
//         type_tree: &TypeTree,
//         computation_results: &HashMap<usize, ComputationResult>,
//     ) -> Result<Option<TableRow>, Error> {
//         let root_rc = execution_tree.root().get_rc();
//         let root_operation = &root_rc.borrow().operation;

//         if let ExecutionOperation::Project(_, _) = root_operation {
//             panic!("Project is not supported in this evaluation mode.");
//         }

//         let iter_opt =
//             self.get_iterator_node(execution_tree.root(), type_tree, computation_results)?;

//         Ok(iter_opt.and_then(|iter| match iter {
//             TrieScanEnum::TrieScanAggregateWrapper(mut aggregate_wrapper) => {
//                 scan_first_match(&mut aggregate_wrapper.trie_scan)
//             }
//             _ => scan_first_match(&mut TrieScanPrune::new(iter)),
//         }))
//     }

//     /// Executes a given [ExecutionPlan].
//     /// Returns a map that assigns to each plan id of a permanent table the [TableId] in the [DatabaseInstance]
//     /// This may fail if certain operations are performed on tries with incompatible types
//     /// or if the plan references tries that do not exist.
//     pub fn execute_plan(&mut self, plan: ExecutionPlan) -> Result<HashMap<usize, TableId>, Error> {
//         let execution_trees = plan.split_at_write_nodes();

//         // The variables below associate the write node ids of the given plan with some additional information
//         let mut permanent_ids = HashMap::<usize, TableId>::new();
//         let mut type_trees = HashMap::<usize, TypeTree>::new();
//         let mut computation_results = HashMap::<usize, ComputationResult>::new();
//         let mut removed_temp_ids = HashSet::<usize>::new();

//         for (tree_id, mut execution_tree) in execution_trees {
//             log::info!("Execution step: {}", execution_tree.name());

//             execution_tree.satisfy_leapfrog_triejoin();

//             if let Some(simplified_tree) = execution_tree.simplify(&removed_temp_ids) {
//                 execution_tree = simplified_tree;
//             } else {
//                 removed_temp_ids.insert(tree_id);
//                 log::info!("Result is empty. No computation was required.");

//                 continue;
//             }

//             TimedCode::instance()
//                 .sub("Reasoning/Execution/Load Table")
//                 .start();

//             for (id, order) in execution_tree.required_tables() {
//                 self.make_available_in_memory(id, &order)?;
//             }

//             TimedCode::instance()
//                 .sub("Reasoning/Execution/Load Table")
//                 .stop();

//             let timed_string = format!("Reasoning/Execution/{}", execution_tree.name());
//             TimedCode::instance().sub(&timed_string).start();

//             let type_tree = TypeTree::from_execution_tree(self, &type_trees, &execution_tree)?;
//             let schema = type_tree.schema.clone();

//             let num_null_columns = Self::appends_nulls(execution_tree.root());

//             let new_trie_opt =
//                 self.evaluate_execution_tree(&execution_tree, &type_tree, &computation_results)?;
//             type_trees.insert(tree_id, type_tree);

//             if let Some(new_trie) = new_trie_opt {
//                 // If trie appended nulls then we need to update our current_null value
//                 self.current_null += new_trie.row_num() as u64 * num_null_columns;

//                 // Add new trie to the appropriate place
//                 match execution_tree.result() {
//                     ExecutionResult::Temporary => {
//                         log::info!(
//                             "Saved temporary table: {} entries ({})",
//                             new_trie.row_num(),
//                             new_trie.size_bytes()
//                         );

//                         if new_trie.row_num() > 0 || new_trie.get_types().is_empty() {
//                             computation_results
//                                 .insert(tree_id, ComputationResult::Temporary(Some(new_trie)));
//                         } else {
//                             computation_results.insert(tree_id, ComputationResult::Temporary(None));
//                         }
//                     }
//                     ExecutionResult::Permanent(order, name) => {
//                         let new_id = self.register_table(name, schema);

//                         log::info!(
//                             "Saved permanent table {new_id} - {name} with {} entries ({})",
//                             new_trie.row_num(),
//                             new_trie.size_bytes()
//                         );

//                         self.add_trie(new_id, order.clone(), new_trie);

//                         permanent_ids.insert(tree_id, new_id);
//                         computation_results
//                             .insert(tree_id, ComputationResult::Permanent(new_id, order.clone()));
//                     }
//                 }
//             } else {
//                 log::info!("Trie does not contain any elements");

//                 computation_results.insert(tree_id, ComputationResult::Empty);
//             }

//             TimedCode::instance().sub(&timed_string).stop();
//         }

//         Ok(permanent_ids)
//     }

//     /// Execute a given [ExecutionPlan]
//     /// but evaluate it only until the first row of the result table
//     /// or return None if it is empty.
//     /// The result table is considered to be the (unique) table marked as permanent output.
//     ///
//     /// Assumes that the given plan has only one output node.
//     /// Further assumes that each input table is already available in memory.
//     /// No tables will be saved in the database.
//     ///
//     /// TODO: This code is very similar to execute_plan,
//     /// but hard to abstract because of the timing...
//     pub fn execute_plan_first_match(&self, plan: ExecutionPlan) -> Result<Option<TableRow>, Error> {
//         let execution_trees = plan.split_at_write_nodes();

//         // The variables below associate the write node ids of the given plan with some additional information
//         let mut type_trees = HashMap::<usize, TypeTree>::new();
//         let mut computation_results = HashMap::<usize, ComputationResult>::new();
//         let mut removed_temp_ids = HashSet::<usize>::new();

//         for (tree_id, mut execution_tree) in execution_trees {
//             execution_tree.satisfy_leapfrog_triejoin();

//             if let Some(simplified_tree) = execution_tree.simplify(&removed_temp_ids) {
//                 execution_tree = simplified_tree;
//             } else {
//                 removed_temp_ids.insert(tree_id);

//                 continue;
//             }

//             let type_tree = TypeTree::from_execution_tree(self, &type_trees, &execution_tree)?;

//             match execution_tree.result() {
//                 ExecutionResult::Temporary => {
//                     let new_trie_opt = self.evaluate_execution_tree(
//                         &execution_tree,
//                         &type_tree,
//                         &computation_results,
//                     )?;

//                     if let Some(new_trie) = new_trie_opt {
//                         if new_trie.row_num() > 0 || new_trie.get_types().is_empty() {
//                             computation_results
//                                 .insert(tree_id, ComputationResult::Temporary(Some(new_trie)));
//                         } else {
//                             computation_results.insert(tree_id, ComputationResult::Temporary(None));
//                         }
//                     } else {
//                         computation_results.insert(tree_id, ComputationResult::Temporary(None));
//                     }
//                 }
//                 ExecutionResult::Permanent(_, _) => {
//                     return self.evaluate_execution_tree_first_match(
//                         &execution_tree,
//                         &type_tree,
//                         &computation_results,
//                     );
//                 }
//             }

//             type_trees.insert(tree_id, type_tree);
//         }

//         Ok(None)
//     }

//     /// Return a reference to a trie with the given id and order.
//     ///
//     /// # Panics
//     /// Panics if no table under the given id and order exists.
//     /// Panics if trie is not available in memory.
//     pub fn get_trie_order<'a>(&'a self, id: TableId, order: &ColumnOrder) -> &'a Trie {
//         let storage = self
//             .storage_handler
//             .table_storage(id, order)
//             .expect("Function assumes that there is a table with the given id and order.");

//         storage
//             .get_trie()
//             .expect("Function assumes that trie is in memory.")
//     }

//     /// Return a reference to a trie corresponding to the given id in arbitrary order.
//     ///
//     /// # Panics
//     /// Panics if no table under the given id exists.
//     /// Panics if trie is not available in memory.
//     pub(crate) fn get_trie(&self, id: TableId) -> (&Trie, ColumnOrder) {
//         let order = self
//             .storage_handler
//             .available_orders(id)
//             .expect("Function assumes that there is a table with the given id.")
//             .first()
//             .expect("There should be at least one order")
//             .clone();

//         let storage = self
//             .storage_handler
//             .table_storage(id, &order)
//             .expect("Order must be available");

//         (
//             storage
//                 .get_trie()
//                 .expect("Function assumes that trie is in memory."),
//             order,
//         )
//     }

//     /// Returns true if the table of the given id contains the given table row.
//     ///
//     /// # Panics
//     /// Panics if no table under the given id exists.
//     /// Panics if trie is not available in memory.
//     pub fn contains_row(&self, id: TableId, row: &TableRow) -> bool {
//         let (trie, order) = self.get_trie(id);
//         let row_reordered = order.permute(row);
//         trie.contains_row(row_reordered)
//     }

//     /// Return a reference to a trie identified by its id and order.
//     /// If the trie is not available in memory, this function will load it.
//     /// Panics if no table under the given id and order exists.
//     pub(crate) fn get_trie_or_load<'a>(
//         &'a mut self,
//         id: TableId,
//         order: &ColumnOrder,
//     ) -> Result<&'a Trie, ReadingError> {
//         self.storage_handler
//             .table_storage_mut(id, order)
//             .expect("Function assumes that there is a table with the given id and order.")
//             .into_memory(&mut self.dict)
//     }

//     /// Returns an iterator over the specified table.
//     /// Uses the default [ColumnOrder]
//     pub fn table_values(
//         &mut self,
//         id: TableId,
//     ) -> Result<impl Iterator<Item = Vec<DataValueT>> + '_, Error> {
//         struct OwnedRecords<'a, S>(
//             TrieRecords<S, ValueSerializer<Ref<'a, Dict>, &'a TableSchema>, DataValueT>,
//         );

//         impl<'a, S: TrieScan> Iterator for OwnedRecords<'a, S> {
//             type Item = Vec<DataValueT>;

//             fn next(&mut self) -> Option<Self::Item> {
//                 let rec = self.0.next_record()?;
//                 Some(rec.cloned().collect())
//             }
//         }

//         let _ = self.get_trie_or_load(id, &ColumnOrder::default())?;
//         let schema = self.table_schema(id);
//         let dict = self.dict();

//         Ok(OwnedRecords(
//             self.get_trie_order(id, &ColumnOrder::default())
//                 .records(ValueSerializer { schema, dict }),
//         ))
//     }

//     /// Convert a [StorageValueIteratorT] into a [DataValueIteratorT].
//     ///
//     /// TODO: This should vanish at some point in the transition to [DataValue].
//     /// Currently, it is still used in tracing, where we work with "Contant" values in query results.
//     pub fn storage_to_data_iterator<'a>(
//         &'a self,
//         name: DataTypeName,
//         iterator: StorageValueIteratorT<'a>,
//     ) -> DataValueIteratorT<'a> {
//         macro_rules! to_data_column_iter_no_string {
//             ($variant:ident, $iter:ident, $name:ident) => {{
//                 debug_assert!(name == DataTypeName::$variant);

//                 DataValueIteratorT::$variant($iter)
//             }};
//         }
//         macro_rules! to_data_column_iter {
//             ($variant:ident, $iter:ident, $name:ident) => {
//                 if name == DataTypeName::String {
//                     // should only clone the ref and not the dict (hopefully)
//                     let dict_ref_clone = Ref::clone(&self.dict());
//                     DataValueIteratorT::String(Box::new($iter.map(move |constant| {
//                         serialize_constant_with_dict(constant, Ref::clone(&dict_ref_clone))
//                     })))
//                 } else {
//                     to_data_column_iter_no_string!($variant, $iter, $name)
//                 }
//             };
//         }

//         match iterator {
//             StorageValueIteratorT::Id32(iter) => to_data_column_iter!(U32, iter, name),
//             StorageValueIteratorT::Id64(iter) => to_data_column_iter!(U64, iter, name),
//             StorageValueIteratorT::Int64(iter) => to_data_column_iter!(I64, iter, name),
//             StorageValueIteratorT::Float(iter) => {
//                 to_data_column_iter_no_string!(Float, iter, name)
//             }
//             StorageValueIteratorT::Double(iter) => {
//                 to_data_column_iter_no_string!(Double, iter, name)
//             }
//         }
//     }

//     /// Convert a [StorageValueIteratorT] into an iterator over [AnyDataValue].
//     pub(crate) fn storage_to_datavalue_iterator<'a>(
//         &'a self,
//         iterator: StorageValueIteratorT<'a>,
//     ) -> AnyDataValueIterator<'a> {
//         fn id_iterator_to_dv_iterator<'b, T>(
//             db: &'b DatabaseInstance,
//             iter: Box<dyn Iterator<Item = T> + 'b>,
//         ) -> AnyDataValueIterator<'b>
//         where
//             T: TryInto<usize> + Display + 'b,
//         {
//             // should only clone the ref and not the dict (hopefully)
//             let dict_ref_clone = Ref::clone(&db.dict());
//             AnyDataValueIterator(Box::new(iter.map(move |id| {
//                 if let Some(dv) = id.try_into()
//                 .ok()
//                 .and_then(|id_usize| dict_ref_clone.id_to_datavalue(id_usize))
//                 {
//                     dv
//                 } else {
//                     panic!("failed to find dictionary entry for an id that occurred in an internal table: please report this bug at https://github.com/knowsys/nemo/");
//                 }
//             })))
//         }

//         match iterator {
//             StorageValueIteratorT::Id32(iter) => {
//                 id_iterator_to_dv_iterator(self, iter)
//             },
//             StorageValueIteratorT::Id64(iter) => {
//                 id_iterator_to_dv_iterator(self, iter)
//             },
//             StorageValueIteratorT::Int64(iter) => {
//                 AnyDataValueIterator(Box::new(iter.map(move |integer| {
//                     AnyDataValue::new_integer_from_i64(integer)
//                 })))
//             },
//             StorageValueIteratorT::Float(_iter) => {
//                 panic!("32bit float not currently supported in AnyDataValue, so they should not occur in tables: please report this bug at https://github.com/knowsys/nemo/");
//             }
//             StorageValueIteratorT::Double(iter) => {
//                 AnyDataValueIterator(Box::new(iter.map(move |double| {
//                     AnyDataValue::new_double_from_f64(double.into()).expect("non-finite floats in the database: please report this bug at https://github.com/knowsys/nemo/")
//                 })))
//             }
//         }
//     }

//     fn get_in_memory_table_column_iterators(&self, id: TableId) -> Vec<AnyDataValueIterator> {
//         let trie = self.get_trie_order(id, &ColumnOrder::default());

//         trie.get_full_column_iterators()
//             .into_iter()
//             .map(|fci| self.storage_to_datavalue_iterator(fci))
//             .collect()
//     }

//     /// Get a list of column iterators for the full table (i.e. the expanded trie)
//     pub fn get_table_column_iterators(
//         &mut self,
//         id: TableId,
//     ) -> Result<Vec<AnyDataValueIterator>, ReadingError> {
//         // make sure trie is loaded
//         self.get_trie_or_load(id, &ColumnOrder::default())?;

//         Ok(self.get_in_memory_table_column_iterators(id))
//     }

//     /// Given a node in the execution tree returns the trie iterator
//     /// that if materialized will turn into the resulting trie of the represented computation
//     fn get_iterator_node<'a>(
//         &'a self,
//         execution_node: ExecutionNodeRef,
//         type_node: &TypeTreeNode,
//         computation_results: &'a HashMap<usize, ComputationResult>,
//     ) -> Result<Option<TrieScanEnum<'a>>, Error> {
//         // if type_node.schema.is_empty() {
//         //     // That there is no schema for this node implies that the table is empty
//         //     return Ok(None);
//         // }

//         let node_rc = execution_node.get_rc();
//         let node_operation = &node_rc.borrow().operation;

//         return match node_operation {
//             ExecutionOperation::FetchExisting(id, order) => {
//                 let trie_ref = self.get_trie_order(*id, order);
//                 if trie_ref.row_num() == 0 {
//                     return Ok(None);
//                 }

//                 let schema = type_node.schema.clone();
//                 let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema.get_storage_types());

//                 Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
//             }
//             ExecutionOperation::FetchNew(index) => {
//                 let comp_result = computation_results.get(index).unwrap();
//                 let trie_ref = match comp_result {
//                     ComputationResult::Temporary(trie_opt) => {
//                         if let Some(trie) = trie_opt {
//                             trie
//                         } else {
//                             return Ok(None);
//                         }
//                     }
//                     ComputationResult::Permanent(id, order) => self.get_trie_order(*id, order),
//                     ComputationResult::Empty => return Ok(None),
//                 };

//                 let schema = type_node.schema.clone();
//                 let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema.get_storage_types());

//                 Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
//             }
//             ExecutionOperation::Join(subtables, bindings) => {
//                 let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
//                 for (table_index, subtable) in subtables.iter().enumerate() {
//                     let subiterator_opt = self.get_iterator_node(
//                         subtable.clone(),
//                         &type_node.subnodes[table_index],
//                         computation_results,
//                     )?;

//                     if let Some(subiterator) = subiterator_opt {
//                         subiterators.push(subiterator);
//                     } else {
//                         // If subtables contain an empty table, then the join is empty
//                         return Ok(None);
//                     }
//                 }

//                 // If it only contains one table, then we dont need the join
//                 if subiterators.len() == 1 {
//                     return Ok(Some(subiterators.remove(0)));
//                 }

//                 Ok(Some(TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
//                     subiterators,
//                     bindings,
//                 ))))
//             }
//             ExecutionOperation::Union(subtables) => {
//                 let mut subiterators = Vec::<TrieScanEnum>::with_capacity(subtables.len());
//                 for (table_index, subtable) in subtables.iter().enumerate() {
//                     let subiterator_opt = self.get_iterator_node(
//                         subtable.clone(),
//                         &type_node.subnodes[table_index],
//                         computation_results,
//                     )?;

//                     if let Some(subiterator) = subiterator_opt {
//                         subiterators.push(subiterator);
//                     }
//                 }

//                 // The union of empty tables is empty
//                 if subiterators.is_empty() {
//                     return Ok(None);
//                 }

//                 // If it only contains one table, then we dont need the join
//                 if subiterators.len() == 1 {
//                     return Ok(Some(subiterators.remove(0)));
//                 }

//                 let union_scan = TrieScanUnion::new(subiterators);

//                 Ok(Some(TrieScanEnum::TrieScanUnion(union_scan)))
//             }
//             ExecutionOperation::Minus(subtable_left, subtable_right) => {
//                 if let Some(left_scan) = self.get_iterator_node(
//                     subtable_left.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )? {
//                     if let Some(right_scan) = self.get_iterator_node(
//                         subtable_right.clone(),
//                         &type_node.subnodes[1],
//                         computation_results,
//                     )? {
//                         Ok(Some(TrieScanEnum::TrieScanMinus(TrieScanMinus::new(
//                             left_scan, right_scan,
//                         ))))
//                     } else {
//                         Ok(Some(left_scan))
//                     }
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::Project(subnode, reorder) => {
//                 let subnode_rc = subnode.get_rc();
//                 let subnode_operation = &subnode_rc.borrow().operation;

//                 let trie = match subnode_operation {
//                     ExecutionOperation::FetchExisting(id, order) => self.get_trie_order(*id, order),
//                     ExecutionOperation::FetchNew(index) => {
//                         let comp_result = computation_results.get(index).unwrap();
//                         let trie_ref = match comp_result {
//                             ComputationResult::Temporary(trie_opt) => {
//                                 if let Some(trie) = trie_opt {
//                                     trie
//                                 } else {
//                                     return Ok(None);
//                                 }
//                             }
//                             ComputationResult::Permanent(id, order) => {
//                                 self.get_trie_order(*id, order)
//                             }
//                             ComputationResult::Empty => return Ok(None),
//                         };

//                         trie_ref
//                     }
//                     _ => {
//                         panic!("Project node has to have a Fetch node as its child.");
//                     }
//                 };

//                 let project_scan = TrieScanProject::new(trie, reorder.clone());

//                 Ok(Some(TrieScanEnum::TrieScanProject(project_scan)))
//             }
//             ExecutionOperation::Filter(subtable, conditions) => {
//                 let subiterator_opt = self.get_iterator_node(
//                     subtable.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )?;

//                 if let Some(subiterator) = subiterator_opt {
//                     let restrict_scan = TrieScanRestrictValues::new(
//                         &mut self.dict.borrow_mut(),
//                         subiterator,
//                         conditions,
//                     );
//                     Ok(Some(TrieScanEnum::TrieScanRestrictValues(restrict_scan)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::SelectEqual(subtable, classes) => {
//                 let subiterator_opt = self.get_iterator_node(
//                     subtable.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )?;

//                 if let Some(subiterator) = subiterator_opt {
//                     let select_scan = TrieScanSelectEqual::new(subiterator, classes);
//                     Ok(Some(TrieScanEnum::TrieScanSelectEqual(select_scan)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::AppendColumns(subtable, instructions) => {
//                 let subiterator_opt = self.get_iterator_node(
//                     subtable.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )?;
//                 let target_types = type_node.schema.clone();

//                 if let Some(subiterator) = subiterator_opt {
//                     let append_scan = TrieScanAppend::new(
//                         &mut self.dict.borrow_mut(),
//                         subiterator,
//                         instructions,
//                         target_types.get_storage_types(),
//                     );
//                     Ok(Some(TrieScanEnum::TrieScanAppend(append_scan)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::AppendNulls(subtable, num_nulls) => {
//                 let subiterator_opt = self.get_iterator_node(
//                     subtable.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )?;

//                 if let Some(subiterator) = subiterator_opt {
//                     let nulls_scan = TrieScanNulls::new(subiterator, *num_nulls, self.current_null);
//                     Ok(Some(TrieScanEnum::TrieScanNulls(nulls_scan)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::Subtract(subtable_main, subtables_subtract, infos) => {
//                 if let Some(main_scan) = self.get_iterator_node(
//                     subtable_main.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )? {
//                     let mut subiterators =
//                         Vec::<TrieScanEnum>::with_capacity(subtables_subtract.len());
//                     for (table_index, subtable) in subtables_subtract.iter().enumerate() {
//                         let subiterator_opt = self.get_iterator_node(
//                             subtable.clone(),
//                             &type_node.subnodes[table_index + 1],
//                             computation_results,
//                         )?;

//                         if let Some(subiterator) = subiterator_opt {
//                             subiterators.push(subiterator);
//                         }
//                     }

//                     if subiterators.is_empty() {
//                         return Ok(Some(main_scan));
//                     }

//                     let subtract_scan = TrieScanEnum::TrieScanSubtract(TrieScanSubtract::new(
//                         main_scan,
//                         subiterators,
//                         infos.clone(),
//                     ));

//                     Ok(Some(subtract_scan))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             ExecutionOperation::Aggregate(subtable, aggregation_instructions) => {
//                 let subiterator_opt = self.get_iterator_node(
//                     subtable.clone(),
//                     &type_node.subnodes[0],
//                     computation_results,
//                 )?;

//                 if let Some(subiterator) = subiterator_opt {
//                     let prune = TrieScanPrune::new(subiterator);

//                     let aggregate = TrieScanAggregate::new(
//                         prune,
//                         *aggregation_instructions,
//                         type_node.subnodes[0].schema.get_storage_types()
//                             [aggregation_instructions.aggregated_column_index],
//                     );

//                     // Wrap aggregate full trie scan because execution plan currently only support partial trie scans
//                     Ok(Some(TrieScanEnum::TrieScanAggregateWrapper(
//                         aggregate.into(),
//                     )))
//                 } else {
//                     Ok(None)
//                 }
//             }
//         };
//     }

//     /// Return the amount of memory consumed by the table under the given [TableId].
//     /// This also includes additional index structures but excludes tables that are currently stored on disk.
//     ///
//     /// # Panics
//     /// Panics if the given id does not exist.
//     pub fn memory_consumption(&self, id: TableId) -> ByteSize {
//         let status = self
//             .storage_handler
//             .map
//             .get(&id)
//             .expect("Function assumes that there is a table with the given id.");

//         status.size_bytes()
//     }
// }

// impl ByteSized for DatabaseInstance {
//     fn size_bytes(&self) -> ByteSize {
//         self.storage_handler.size_bytes()
//     }
// }

// #[cfg(test)]
// mod test {
//     use crate::{
//         columnar::column::Column,
//         datatypes::{DataTypeName, StorageValueT},
//         management::{
//             database::{ColumnOrder, TableId},
//             ByteSized, ExecutionPlan,
//         },
//         tabular::{
//             operations::JoinBindings,
//             table_types::trie::Trie,
//             traits::{table::Table, table_schema::TableSchema},
//         },
//         util::{make_column_with_intervals_t, mapping::permutation::Permutation},
//     };

//     use super::{DatabaseInstance, OrderedReferenceManager, TableStorage};

//     #[test]
//     fn basic_add_delete() {
//         let column_a = make_column_with_intervals_t(&[1, 2, 3], &[0]);
//         let column_b = make_column_with_intervals_t(&[1, 2, 3, 4, 5, 6], &[0]);

//         let trie_a = Trie::new(vec![column_a]);
//         let trie_b = Trie::new(vec![column_b]);

//         let mut instance = DatabaseInstance::new();
//         let mut reference_id = TableId::default();

//         let mut schema_a = TableSchema::new();
//         schema_a.add_entry(DataTypeName::U64);

//         let trie_a_id = instance.register_table("A", schema_a);
//         instance.add_trie(trie_a_id, ColumnOrder::default(), trie_a);

//         assert_eq!(trie_a_id, reference_id.increment());
//         assert_eq!(instance.table_name(trie_a_id), "A");
//         assert_eq!(
//             instance
//                 .get_trie_order(trie_a_id, &ColumnOrder::default())
//                 .row_num(),
//             3
//         );

//         let last_size = instance.size_bytes();

//         let mut schema_b = TableSchema::new();
//         schema_b.add_entry(DataTypeName::U64);

//         let trie_b_id = instance.register_table("B", schema_b);
//         instance.add_trie(trie_b_id, ColumnOrder::default(), trie_b);

//         assert_eq!(trie_b_id, reference_id.increment());
//         assert!(instance.size_bytes() > last_size);
//         assert_eq!(
//             instance
//                 .get_trie_order(trie_b_id, &ColumnOrder::default())
//                 .row_num(),
//             6
//         );

//         let last_size = instance.size_bytes();

//         instance.delete(trie_a_id);
//         assert!(instance.size_bytes() < last_size);
//     }

//     fn test_casting_execution_plan() -> (ExecutionPlan, usize) {
//         // ExecutionPlan:
//         // Union
//         //  -> Minus
//         //      -> Trie_x [U64, U32, U64]
//         //      -> Trie_y [U32, U64, U32]
//         //  -> Join [[0, 1], [1, 2]]
//         //      -> Union
//         //          -> Trie_a [U32, U32]
//         //          -> Trie_b [U32, U64]
//         //      -> Union
//         //          -> Trie_b [U32, U64]
//         //          -> Trie_c [U32, U32]

//         let mut table_id = TableId::default();
//         let id_a = table_id.increment();
//         let id_b = table_id.increment();
//         let id_c = table_id.increment();
//         let id_x = table_id.increment();
//         let id_y = table_id.increment();

//         let mut execution_tree = ExecutionPlan::default();

//         let node_load_a = execution_tree.fetch_existing(id_a);
//         let node_load_b_1 = execution_tree.fetch_existing(id_b);
//         let node_load_b_2 = execution_tree.fetch_existing(id_b);
//         let node_load_c = execution_tree.fetch_existing(id_c);
//         let node_load_x = execution_tree.fetch_existing(id_x);
//         let node_load_y = execution_tree.fetch_existing(id_y);

//         let node_minus = execution_tree.minus(node_load_x, node_load_y);

//         let node_left_union = execution_tree.union(vec![node_load_a, node_load_b_1]);
//         let node_right_union = execution_tree.union(vec![node_load_b_2, node_load_c]);

//         let node_join = execution_tree.join(
//             vec![node_left_union, node_right_union],
//             JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
//         );

//         let node_root = execution_tree.union(vec![node_join, node_minus]);
//         let result_id = execution_tree.write_permanent(node_root, "Test", "Test");

//         (execution_tree, result_id)
//     }

//     #[ignore]
//     #[test]
//     fn test_casting() {
//         let trie_a = Trie::from_rows(&[vec![StorageValueT::Id32(1), StorageValueT::Id32(2)]]);
//         let trie_b = Trie::from_rows(&[
//             vec![StorageValueT::Id32(2), StorageValueT::Id64(1 << 35)],
//             vec![StorageValueT::Id32(3), StorageValueT::Id64(2)],
//         ]);
//         let trie_c = Trie::from_rows(&[vec![StorageValueT::Id32(2), StorageValueT::Id64(4)]]);
//         let trie_x = Trie::from_rows(&[
//             vec![
//                 StorageValueT::Id64(1),
//                 StorageValueT::Id32(2),
//                 StorageValueT::Id64(4),
//             ],
//             vec![
//                 StorageValueT::Id64(3),
//                 StorageValueT::Id32(2),
//                 StorageValueT::Id64(4),
//             ],
//             vec![
//                 StorageValueT::Id64(1 << 36),
//                 StorageValueT::Id32(6),
//                 StorageValueT::Id64(12),
//             ],
//         ]);
//         let trie_y = Trie::from_rows(&[
//             vec![
//                 StorageValueT::Id32(1),
//                 StorageValueT::Id64(2),
//                 StorageValueT::Id32(4),
//             ],
//             vec![
//                 StorageValueT::Id32(2),
//                 StorageValueT::Id64(1 << 37),
//                 StorageValueT::Id32(7),
//             ],
//         ]);

//         let schema_a = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
//         let schema_b = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
//         let schema_c = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
//         let schema_x = TableSchema::from_vec(vec![
//             DataTypeName::U64,
//             DataTypeName::U32,
//             DataTypeName::U64,
//         ]);
//         let schema_y = TableSchema::from_vec(vec![
//             DataTypeName::U32,
//             DataTypeName::U64,
//             DataTypeName::U32,
//         ]);

//         let mut instance = DatabaseInstance::new();
//         instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
//         instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);
//         instance.register_add_trie("TableC", schema_c, ColumnOrder::default(), trie_c);
//         instance.register_add_trie("TableX", schema_x, ColumnOrder::default(), trie_x);
//         instance.register_add_trie("TableY", schema_y, ColumnOrder::default(), trie_y);

//         let (plan, node_id) = test_casting_execution_plan();
//         let result = instance.execute_plan(plan);
//         assert!(result.is_ok());

//         let result_id = *result.unwrap().get(&node_id).unwrap();
//         let result_trie = instance.get_trie_order(result_id, &ColumnOrder::default());

//         let result_col_first = result_trie.get_column(0).as_u64().unwrap();
//         let result_col_second = result_trie.get_column(1).as_u32().unwrap();
//         let result_col_third = result_trie.get_column(2).as_u64().unwrap();

//         assert_eq!(
//             result_col_first
//                 .get_data_column()
//                 .iter()
//                 .collect::<Vec<u64>>(),
//             vec![1, 3, 1 << 36]
//         );
//         assert_eq!(
//             result_col_first
//                 .get_int_column()
//                 .iter()
//                 .collect::<Vec<usize>>(),
//             vec![0]
//         );

//         assert_eq!(
//             result_col_second
//                 .get_data_column()
//                 .iter()
//                 .collect::<Vec<u32>>(),
//             vec![2, 2, 6]
//         );
//         assert_eq!(
//             result_col_second
//                 .get_int_column()
//                 .iter()
//                 .collect::<Vec<usize>>(),
//             vec![0, 1, 2]
//         );

//         assert_eq!(
//             result_col_third
//                 .get_data_column()
//                 .iter()
//                 .collect::<Vec<u64>>(),
//             vec![4, 1 << 35, 4, 1 << 35, 12],
//         );
//         assert_eq!(
//             result_col_third
//                 .get_int_column()
//                 .iter()
//                 .collect::<Vec<usize>>(),
//             vec![0, 2, 4]
//         );
//     }
// }
