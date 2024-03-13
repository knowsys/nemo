//! This module defines [DatabaseInstance],
//! which is used to manage a collection of tables.

pub mod id;
pub mod sources;

pub(crate) mod execution_series;

mod order;
mod storage;

use std::{
    borrow::Borrow,
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    fmt::Debug,
};

use bytesize::ByteSize;

use crate::{
    datasources::table_providers::TableProvider,
    datavalues::AnyDataValue,
    error::Error,
    management::{bytesized::ByteSized, database::execution_series::ExecutionTreeNode},
    meta::timing::TimedCode,
    tabular::{
        operations::{projectreorder::ProjectReordering, OperationGenerator},
        rowscan::RowScan,
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

use super::execution_plan::{ColumnOrder, ExecutionPlan, ExecutionResult};

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

    /// Returns a mutable reference to the dictionary used for associating abstract constants with strings.
    pub fn dictionary_mut(&mut self) -> RefMut<'_, Dict> {
        self.dictionary.borrow_mut()
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

    /// Provide an iterator over the rows of the table with the given [PermanentTableId].
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn table_row_iterator(
        &mut self,
        id: PermanentTableId,
    ) -> Result<impl Iterator<Item = Vec<AnyDataValue>> + '_, Error> {
        // Make sure trie is loaded
        let storage_id = self
            .reference_manager
            .trie_id(&self.dictionary, id, ColumnOrder::default())
            .expect("No table with id {id} exists.");
        let trie = self.reference_manager.trie(storage_id);

        Ok(trie.row_iterator().map(|values| {
            values
                .into_iter()
                .map(|value| {
                    AnyDataValue::new_from_storage_value(value, &self.dictionary())
                        .expect("Values from tries should be sound.")
                })
                .collect()
        }))
    }

    /// Return whether a table of the given [PermanentTableId] contains a given row.
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn table_contains_row(&mut self, id: PermanentTableId, row: &[AnyDataValue]) -> bool {
        let storage_id = self
            .reference_manager
            .trie_id(&self.dictionary, id, ColumnOrder::default())
            .expect("No table with id {id} exists.");
        let trie = self.reference_manager.trie(storage_id);

        let dictionary: &Dict = &self.dictionary();

        let mut row_storage = Vec::with_capacity(row.len());
        for data_value in row.iter() {
            if let Some(storage_value) = data_value.try_to_storage_value_t(dictionary) {
                row_storage.push(storage_value);
            } else {
                // `value` is not know to the dictionary and therefore
                // not be part of a table.
                return false;
            }
        }

        trie.contains_row(&row_storage)
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
            .add_source(id, order, TableSource::new(provider, arity));
    }

    /// Add a table given as [SimpleTable].
    pub fn add_source_table(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        table: SimpleTable,
    ) {
        let arity = table.arity();

        self.reference_manager
            .add_source(id, order, TableSource::new(Box::new(table), arity));
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

/// Contains [Trie]s computed during the evaluation of an [ExecutionPlan]
/// and the [StorageId]s of the tables needed for that
#[derive(Debug)]
struct TemporaryStorage {
    /// Tables that were loaded from the [DatabaseInstance] before the computation
    loaded_tables: Vec<StorageId>,
    /// Tables that were computed during the execution of an [ExecutionPlan]
    computed_tables: Vec<Option<Trie>>,
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
    ) -> Result<Vec<StorageId>, Error> {
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
                self.reference_manager.trie(storage.loaded_tables[*load_id])
            }
            ExecutionTreeLeaf::FetchComputedTable(computed_id) => storage.computed_tables
                [*computed_id]
                .as_ref()
                .expect("Referenced trie shold have been computed."),
        };

        if !trie.is_empty() {
            Some(TrieScanEnum::Generic(trie.partial_iterator()))
        } else {
            None
        }
    }

    /// Return a [TrieScanEnum] representing the given [ExecutionTreeOperation] node.
    ///
    /// Returns `None` if a previous computation represented by this node was empty
    /// or it can be known that evaulating this operation would result in an empty table.
    fn evaluate_operation<'a>(
        &'a self,
        dictionary: &'a RefCell<Dict>,
        storage: &'a TemporaryStorage,
        operation: &ExecutionTreeOperation,
    ) -> Option<TrieScanEnum<'a>> {
        match operation {
            ExecutionTreeOperation::Leaf(leaf) => self.evaluate_tree_leaf(storage, leaf),
            ExecutionTreeOperation::Node {
                generator,
                subnodes,
            } => {
                let input_scans = subnodes
                    .iter()
                    .map(|subnode| self.evaluate_operation(dictionary, storage, subnode))
                    .collect();

                generator.generate(input_scans, dictionary)
            }
        }
    }

    /// Evaluate the tree of operations represented by the [ExecutionTree].
    /// Also accepts a list of [ProjectReordering]s that specify "dependent"
    /// [Trie]s that result from the original by applying the respective reordering.
    ///
    /// Returns a pair containing a (possibly empty) [Trie]
    /// and a list of reordered [Trie]s, one for each provided [ProjectReordering].
    fn execute_tree<'a>(
        &'a mut self,
        storage: &'a TemporaryStorage,
        tree: &ExecutionTree,
        dependent: Vec<ProjectReordering>,
    ) -> (Trie, Vec<Trie>) {
        let (trie, tries_dependent) = match &tree.root {
            ExecutionTreeNode::Operation(operation) => {
                let trie_scan = self.evaluate_operation(&self.dictionary, storage, operation);

                if dependent.is_empty() {
                    let trie =
                        trie_scan.map(|scan| Trie::from_partial_trie_scan(scan, tree.cut_layers));
                    (trie, vec![])
                } else {
                    if let Some(scan) = trie_scan {
                        Trie::from_partial_trie_scan_dependents(scan, dependent, tree.used > 0)
                    } else {
                        (None, vec![None; dependent.len()])
                    }
                }
            }
            ExecutionTreeNode::ProjectReorder { generator, subnode } => {
                debug_assert!(
                    dependent.is_empty(),
                    "Having dependant tables is not supported for projectreorder nodes."
                );

                let trie = if generator.is_noop() {
                    self.evaluate_tree_leaf(storage, subnode)
                        .map(|scan| Trie::from_partial_trie_scan(scan, tree.cut_layers))
                } else {
                    self.evaluate_tree_leaf(storage, subnode)
                        .map(|scan| generator.apply_operation(scan))
                };

                (trie, vec![])
            }
        };

        let trie = trie.unwrap_or_else(|| Trie::empty(0));
        let tries_dependent = tries_dependent
            .into_iter()
            .map(|trie| trie.unwrap_or_else(|| Trie::empty(0)))
            .collect();

        (trie, tries_dependent)
    }

    fn log_new_trie(tree: &ExecutionTree, trie: &Trie) {
        if !trie.is_empty() {
            match &tree.result {
                ExecutionResult::Temporary => {
                    log::info!(
                        "Saved temporary table: {} entries ({})",
                        trie.num_rows(),
                        trie.size_bytes()
                    );
                }
                ExecutionResult::Permanent(_, name) => {
                    log::info!(
                        "Saved permanent table {name} with {} entries ({})",
                        trie.num_rows(),
                        trie.size_bytes()
                    );
                }
            }
        } else {
            log::info!("Trie does not contain any elements");
        }
    }

    /// Evaluate the given [ExecutionPlan].
    pub fn execute_plan(
        &mut self,
        plan: ExecutionPlan,
    ) -> Result<HashMap<ExecutionId, PermanentTableId>, Error> {
        let execution_series = plan.finalize();

        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .start();

        let mut temporary_storage = TemporaryStorage {
            loaded_tables: self.collect_requiured_tries(&execution_series.loaded_tries)?,
            computed_tables: vec![None; execution_series.trees.len()],
        };

        TimedCode::instance()
            .sub("Reasoning/Execution/Load Table")
            .stop();

        for (tree_index, tree) in execution_series.trees.iter().enumerate() {
            if temporary_storage.computed_tables[tree_index].is_some() {
                continue;
            }

            log::info!("Execution step: {}", tree.operation_name);

            let dependant_reorderings = tree
                .dependents
                .iter()
                .map(|(_, projectreordering)| projectreordering.clone())
                .collect::<Vec<_>>();

            let timed_string = format!("Reasoning/Execution/{}", tree.operation_name);
            TimedCode::instance().sub(&timed_string).start();
            let (result_tree, results_dependant) =
                self.execute_tree(&temporary_storage, tree, dependant_reorderings);
            TimedCode::instance().sub(&timed_string).stop();

            Self::log_new_trie(tree, &result_tree);

            temporary_storage.computed_tables[tree_index] = Some(result_tree);
            for ((computed_id, _), result_dependant) in
                tree.dependents.iter().zip(results_dependant)
            {
                temporary_storage.computed_tables[*computed_id] = Some(result_dependant);
            }
        }

        let mut result = HashMap::new();
        for (tree, trie) in execution_series
            .trees
            .into_iter()
            .zip(temporary_storage.computed_tables)
        {
            match tree.result {
                ExecutionResult::Temporary => {} // Temporary table will be dropped at the end of the function
                ExecutionResult::Permanent(order, name) => {
                    let trie = trie.expect("Trie should have been computed.");
                    if !trie.is_empty() {
                        let permanent_id = self.register_add_trie(&name, order, trie);
                        let execution_id = tree.id;

                        result.insert(execution_id, permanent_id);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Evaluate a given [ExecutionPlan] until the first row is found and return it.
    ///
    /// Assumes that it only contains one permanent output node.
    ///
    /// Returns `None` if this evaluates to an empty table.
    pub fn execute_first_match(&mut self, plan: ExecutionPlan) -> Option<Vec<AnyDataValue>> {
        let execution_series = plan.finalize();

        let mut temporary_storage = TemporaryStorage {
            loaded_tables: self
                .collect_requiured_tries(&execution_series.loaded_tries)
                .ok()?,
            computed_tables: vec![None; execution_series.trees.len()],
        };

        for (tree_index, tree) in execution_series.trees.into_iter().enumerate() {
            match &tree.result {
                ExecutionResult::Temporary => {
                    let (result, _) = self.execute_tree(&temporary_storage, &tree, vec![]);

                    temporary_storage.computed_tables[tree_index] = Some(result);
                }
                ExecutionResult::Permanent(_, _) => {
                    let row_storage = match &tree.root {
                        ExecutionTreeNode::Operation(operation) => {
                            let trie_scan = self.evaluate_operation(
                                &self.dictionary,
                                &temporary_storage,
                                operation,
                            );
                            trie_scan.and_then(|scan| {
                                Iterator::next(&mut RowScan::new(scan, tree.cut_layers))
                            })
                        }
                        ExecutionTreeNode::ProjectReorder { generator, subnode } => self
                            .evaluate_tree_leaf(&temporary_storage, subnode)
                            .and_then(|scan| generator.apply_operation_first(scan)),
                    }?;

                    let row_datavalue = row_storage
                        .into_iter()
                        .map(|value| {
                            AnyDataValue::new_from_storage_value(value, self.dictionary().borrow())
                                .ok()
                        })
                        .collect::<Option<Vec<_>>>()?;

                    return Some(row_datavalue);
                }
            }
        }

        None
    }
}

impl ByteSized for DatabaseInstance {
    fn size_bytes(&self) -> ByteSize {
        // TODO: Add size of the dictionary
        self.reference_manager.size_bytes()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datatypes::StorageValueT,
        management::{
            bytesized::ByteSized,
            database::id::{PermanentTableId, TableId},
            execution_plan::ColumnOrder,
        },
        tabular::trie::Trie,
    };

    use super::DatabaseInstance;

    #[test]
    fn basic_add_delete() {
        let table_rows_a = vec![vec![
            StorageValueT::Id32(1),
            StorageValueT::Id32(2),
            StorageValueT::Id32(3),
        ]];
        let table_rows_b = vec![vec![
            StorageValueT::Id32(1),
            StorageValueT::Id32(2),
            StorageValueT::Id32(3),
            StorageValueT::Id32(4),
            StorageValueT::Id32(5),
            StorageValueT::Id32(6),
        ]];

        let trie_a = Trie::from_rows(table_rows_a);
        let trie_b = Trie::from_rows(table_rows_b);

        let mut instance = DatabaseInstance::default();
        let mut reference_id = PermanentTableId::default();

        let trie_a_id = instance.register_table("A", 1);
        instance.add_trie(trie_a_id, ColumnOrder::default(), trie_a);

        assert_eq!(trie_a_id, reference_id.increment());
        assert_eq!(instance.table_name(trie_a_id), "A");

        let last_size = instance.size_bytes();

        let trie_b_id = instance.register_table("B", 1);
        instance.add_trie(trie_b_id, ColumnOrder::default(), trie_b);

        assert_eq!(trie_b_id, reference_id.increment());
        assert!(instance.size_bytes() > last_size);
    }
}
