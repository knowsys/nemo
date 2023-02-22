use std::collections::{hash_map::Entry, HashMap};
use std::fmt::{Debug, Display};
use std::fs::File;
use std::path::PathBuf;

use bytesize::ByteSize;

use crate::io::csv::read;
use crate::meta::logging::{log_add_reference, log_load_table};
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
        let old = self.clone();
        self.0 = self.0 + 1;
        old
    }
}

/// A trie with its associated [`ColumnOrder`]
#[derive(Debug)]
struct OrderedTrie {
    trie: Trie,
    order: ColumnOrder,
}

/// Indicates the file format of a table stored on disc.
#[derive(Debug)]
pub enum TableSource {
    /// Table is contained in csv file
    CSV(PathBuf),
    /// Table is stored as facts in an rls file
    /// TODO: To not invoke the parser twice I just put the parsed "row-table" here. Does not seem quite right
    RLS(Vec<Vec<DataValueT>>),
}

#[derive(Debug)]
enum TableStatus {
    /// Table is materialized in memory in given following orders.
    InMemory(Vec<OrderedTrie>),
    /// Table has to be loaded from disk.
    OnDisk(Vec<TableSource>),
    /// Table has the same contents as another table except for reordering.
    /// Meaning that "ThisTable = Project(ReferencedTable, Reordering)"
    Reference(TableId, Reordering),
}

/// Struct that contains useful information about a trie
/// as well as the actual owner of the trie.
#[derive(Debug)]
struct TableInfo {
    /// The table.
    pub status: TableStatus,
    /// The name of the table.
    pub name: String,
    /// The schema of the table
    pub schema: TableSchema,
}

impl TableInfo {
    /// Create new [`TableInfo`].
    pub fn new(status: TableStatus, name: String, schema: TableSchema) -> Self {
        Self {
            status,
            name,
            schema,
        }
    }
}

/// Represents a collection of tables
#[derive(Debug)]
pub struct DatabaseInstance<Dict: Dictionary> {
    /// Structure which owns all the tries; accessed through Id.
    id_to_table: HashMap<TableId, TableInfo>,

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
            id_to_table: HashMap::new(),
            dict_constants,
            current_null,
            current_id: TableId::default(),
        }
    }

    /// Return the current number of tables.
    pub fn num_tables(&self) -> usize {
        self.id_to_table.len()
    }

    /// Return the name of a table given its id.
    /// Panics if the id does not exist.
    pub fn get_name(&self, id: TableId) -> &str {
        &self.id_to_table.get(&id).unwrap().name
    }

    /// Return the schema of a table identified by the given [`TableName`].
    /// Panics if the id does not exist.
    pub fn get_schema(&self, id: TableId, order: &ColumnOrder) -> TableSchema {
        self.id_to_table
            .get(&id)
            .map(|i| i.schema.reordered(&order.as_reordering(i.schema.arity())))
            .unwrap()
    }

    /// Add the given table to the instance with a name and schema.
    fn add(&mut self, status: TableStatus, name: &str, schema: TableSchema) -> TableId {
        self.id_to_table.insert(
            self.current_id,
            TableInfo::new(status, String::from(name), schema),
        );

        self.current_id.increment()
    }

    /// Adds another order of a table.
    /// Panics if the id does not exist or the table or the status is not in memory.
    pub fn add_trie_order(&mut self, id: TableId, trie: Trie, order: ColumnOrder) {
        if let TableStatus::InMemory(orders) = &mut self.id_to_table.get_mut(&id).unwrap().status {
            orders.push(OrderedTrie { trie, order });
        }
    }

    /// Add a new in-memory [`Trie`] to the database instance.
    pub fn add_trie(
        &mut self,
        trie: Trie,
        order: ColumnOrder,
        name: &str,
        schema: TableSchema,
    ) -> TableId {
        let status = TableStatus::InMemory(vec![OrderedTrie { trie, order }]);
        self.add(status, name, schema)
    }

    /// Add a new table as a reference to another table.
    /// Panics if the referenced tables does not exist.
    pub fn add_reference(
        &mut self,
        referenced_id: TableId,
        new_name: &str,
        reorder: Reordering,
    ) -> TableId {
        debug_assert!(reorder.is_permutation());
        debug_assert!(
            reorder.len_source()
                == self
                    .get_schema(referenced_id, &ColumnOrder::default())
                    .arity()
        );

        log_add_reference(self.get_name(referenced_id), new_name, &reorder);

        // If the referenced table is itself a reference to another table
        // then we resolve the first reference and point the new table to the referenced table
        let (&final_id, final_reorder) =
            if let TableStatus::Reference(another_table, another_order) = &self
                .id_to_table
                .get(&referenced_id)
                .expect("Function assumes that referenced table exists.")
                .status
            {
                let combined_reorder = another_order.chain(&reorder);

                (another_table, combined_reorder)
            } else {
                (&referenced_id, reorder)
            };

        let new_schema = self
            .get_schema(final_id, &ColumnOrder::default())
            .reordered(&final_reorder);
        let status = TableStatus::Reference(final_id, final_reorder);

        self.add(status, new_name, new_schema)
    }

    /// Add a new table as a collection of file sources.
    pub fn add_from_sources(
        &mut self,
        sources: Vec<TableSource>,
        name: &str,
        schema: TableSchema,
    ) -> TableId {
        let status = TableStatus::OnDisk(sources);
        self.add(status, name, schema)
    }

    /// Deletes a table.
    /// TODO: For now, this does not care about keeping references intact.
    /// Panics if the table does not exist.
    pub fn delete(&mut self, id: TableId) {
        if let Entry::Occupied(key_entry) = self.id_to_table.entry(id) {
            key_entry.remove();
        } else {
            panic!("Table to be deleted should exist.");
        }
    }

    /// Helper function which looks through a slice of [`OrderedTrie`]
    /// and returns a reference to the trie with the given order, if available.
    fn search_ordered_tries<'a>(
        ordered_tries: &'a [OrderedTrie],
        order: &ColumnOrder,
    ) -> Option<&'a Trie> {
        for ordered_trie in ordered_tries {
            if *order == ordered_trie.order {
                return Some(&ordered_trie.trie);
            }
        }

        None
    }

    /// Helper function whcih looks through a clide of [`OrderedTrie`]
    /// and returns a reference to the [`ColumnOrder`] which is the closest to given one.
    fn search_closest_order<'a>(
        ordered_tries: &'a [OrderedTrie],
        order: &ColumnOrder,
    ) -> &'a ColumnOrder {
        let mut closest_order = &ordered_tries[0].order;
        let mut distance = usize::MAX;

        for ordered_trie in ordered_tries.iter() {
            let current_distance = ordered_trie.order.distance(order);

            if current_distance < distance {
                closest_order = &ordered_trie.order;
                distance = current_distance;
            }
        }

        closest_order
    }

    /// Return a reference to a [`Trie`] identified by the given [`TableId`] with a particular [`ColumnOrder`].
    /// Panics if
    ///     * the given id does not exist.
    ///     * The trie is not available with the existing order.
    ///     * The table is currently not in memory.
    pub fn get_trie_unchecked<'a>(&'a self, id: TableId, order: &ColumnOrder) -> &'a Trie {
        if let TableStatus::InMemory(ordered_tries) = &self.id_to_table.get(&id).unwrap().status {
            Self::search_ordered_tries(ordered_tries, order).unwrap()
        } else {
            panic!("Requested trie is not in memory.");
        }
    }

    /// Makes sure that the table identified by a [`TableId`] and [`ColumnOrder`] is present.
    /// This is accomplished by
    ///     * Reordering the trie if the requested order is not available.
    ///     * Loading the trie from disk (and possibly reordering) if needed.
    ///     * Resolving references.
    /// Panics if the requested id does not exist.
    fn load_into_memory<'a>(&'a mut self, id: TableId, order: &ColumnOrder) -> Result<(), Error> {
        let info = self.id_to_table.get_mut(&id).unwrap();

        // First we get we get the trie that was asked for in memory but maybe not in the right order
        let (trie, order) = match &info.status {
            TableStatus::InMemory(ordered_tries) => {
                todo!()
            }
            TableStatus::OnDisk(sources) => {
                debug_assert!(!sources.is_empty());

                let loaded_order = ColumnOrder::default();

                let new_trie = if sources.len() == 1 {
                    self.load_from_disk(&sources[0], &info.schema)?
                } else {
                    // If the trie results form multiple sources then we load each source indivdually and then compute the union over all tries

                    let mut loaded_tries = Vec::<Trie>::with_capacity(sources.len());
                    for source in sources {
                        loaded_tries.push(self.load_from_disk(source, &info.schema)?);
                    }

                    let loaded_tries_iters: Vec<TrieScanEnum> = loaded_tries
                        .iter()
                        .map(|t| TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(t)))
                        .collect();
                    let union_iter =
                        TrieScanEnum::TrieScanUnion(TrieScanUnion::new(loaded_tries_iters));

                    materialize(&mut union_iter).unwrap()
                };

                info.status = TableStatus::InMemory(vec![OrderedTrie {
                    trie: new_trie,
                    order: loaded_order.clone(),
                }]);

                let new_trie_ref = if let TableStatus::InMemory(ordered_tries) = info.status {
                    &ordered_tries[0].trie
                } else {
                    unreachable!()
                };

                (new_trie_ref, loaded_order)
            }
            TableStatus::Reference(_, _) => todo!(),
        };

        Ok(())
    }

    /// Load table from a given on-disk source
    /// TODO: This function should change when the type system gets introduced on the logical layer
    fn load_from_disk(
        &mut self,
        source: &TableSource,
        schema: &TableSchema,
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
                        read(
                            &datatypes,
                            &mut crate::io::csv::reader(gz_decoder),
                            &mut self.dict_constants,
                        )?
                    } else {
                        read(
                            &datatypes,
                            &mut crate::io::csv::reader(File::open(file.as_path())?),
                            &mut self.dict_constants,
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

    /// Returns a mutable reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants_mut(&mut self) -> &mut Dict {
        &mut self.dict_constants
    }

    /// Returns a reference to the dictionary used for associating abstract constants with strings.
    pub fn get_dict_constants(&self) -> &Dict {
        &self.dict_constants
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
    /// Returns a map that assigns each permanent
    /// This may fail if certain operations are performed on tries with incompatible types
    /// or if the plan references tries that do not exist.
    pub fn execute_plan(&mut self, plan: &ExecutionPlan) -> Result<HashMap<usize, TableId>, Error> {
        let mut permanent_ids = HashMap::<usize, TableId>::new();
        let mut type_trees = HashMap::<usize, TypeTree>::new();
        let mut computation_results = HashMap::<usize, ComputationResult>::new();

        for (&tree_index, tree) in plan.iter() {
            let timed_string = format!("Reasoning/Execution/{}", tree.name());
            TimedCode::instance().sub(&timed_string).start();
            log::info!("Executing plan \"{}\":", tree.name());

            let type_tree = TypeTree::from_execution_tree(self, &type_trees, tree)?;
            let schema = type_tree.schema.clone();

            let mut num_null_columns = 0u64;

            // Calculate the new trie
            let new_trie_opt = if let Some(root) = tree.root() {
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
                match tree.result() {
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

                        let new_id = self.add_trie(new_trie, order.clone(), name, schema);

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
                let trie_ref = self.get_trie_unchecked(*id, order);
                let schema = type_node.schema.get_column_types();
                let trie_scan = TrieScanGeneric::new_cast(trie_ref, schema);

                Ok(Some(TrieScanEnum::TrieScanGeneric(trie_scan)))
            }
            ExecutionNode::FetchNew(index) => {
                let comp_result = computation_results.get(index).unwrap();
                let trie_ref = match comp_result {
                    ComputationResult::Temporary(trie) => trie,
                    ComputationResult::Permanent(id, order) => self.get_trie_unchecked(*id, order),
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
                    ExecutionNode::FetchExisting(id, order) => self.get_trie_unchecked(*id, order),
                    ExecutionNode::FetchNew(index) => {
                        let comp_result = computation_results.get(index).unwrap();
                        let trie_ref = match comp_result {
                            ComputationResult::Temporary(trie) => trie,
                            ComputationResult::Permanent(id, order) => {
                                self.get_trie_unchecked(*id, order)
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
        self.id_to_table.iter().fold(ByteSize(0), |acc, (_, info)| {
            acc + match &info.status {
                TableStatus::InMemory(ordered_tries) => ordered_tries
                    .iter()
                    .map(|o| o.trie.size_bytes())
                    .fold(ByteSize(0), |acc, x| acc + x),
                TableStatus::OnDisk(_) => ByteSize(0),
                TableStatus::Reference(_, _) => ByteSize(0),
            }
        })
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

        let trie_a_id = instance.add_trie(trie_a, ColumnOrder::default(), "A", schema_a);

        assert_eq!(trie_a_id, reference_id.increment());
        assert_eq!(instance.get_name(trie_a_id), "A");
        assert_eq!(
            instance
                .get_trie_unchecked(trie_a_id, &ColumnOrder::default())
                .row_num(),
            3
        );

        let last_size = instance.size_bytes();

        let mut schema_b = TableSchema::new();
        schema_b.add_entry(DataTypeName::U64, false, false);
        let trie_b_id = instance.add_trie(trie_b, ColumnOrder::default(), "B", schema_b);

        assert_eq!(trie_b_id, reference_id.increment());
        assert!(instance.size_bytes() > last_size);
        assert_eq!(
            instance
                .get_trie_unchecked(trie_b_id, &ColumnOrder::default())
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
        instance.add_trie(trie_a, ColumnOrder::default(), "TableA", schema_a);
        instance.add_trie(trie_b, ColumnOrder::default(), "TableB", schema_b);
        instance.add_trie(trie_c, ColumnOrder::default(), "TableC", schema_c);
        instance.add_trie(trie_x, ColumnOrder::default(), "TableX", schema_x);
        instance.add_trie(trie_y, ColumnOrder::default(), "TableY", schema_y);

        let plan = test_casting_execution_plan();
        let result = instance.execute_plan(&plan);
        assert!(result.is_ok());

        let result_id = *result.unwrap().get(&0).unwrap();
        let result_trie = instance.get_trie_unchecked(result_id, &ColumnOrder::default());

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
