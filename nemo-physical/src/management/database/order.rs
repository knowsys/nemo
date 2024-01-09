//! This module defines and implements the [OrderedReferenceManager].

use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{
    management::execution_plan::ColumnOrder,
    tabular::{
        operations::projectreorder::{GeneratorProjectReorder, ProjectReordering},
        trie::Trie,
    },
    util::mapping::{permutation::Permutation, traits::NatMapping}, meta::TimedCode,
};

use super::{id::PermanentTableId, sources::TableSource, storage::TableStorage, Dict};

/// [OrderedReferenceManager] stores its table in [Vec].
/// This id refers to an index in this vector.
type OrderedReferenceId = usize;
/// Associates a [ColumnOrder] with its [OrderedReferenceId]
type ColumnOrderMap = HashMap<ColumnOrder, OrderedReferenceId>;

/// Encodes that a table has the same contents as another except for reordering of the columns
#[derive(Debug)]
struct Reference {
    /// The id of the referenced table
    id: PermanentTableId,
    /// The [Permutation] used to obtain the current table
    /// from the referenced table
    permutation: Permutation,
}

/// Contains the actual tables (as [TableStorage]s) used in the [DatabaseInstance][super::DatabaseInstance]
///
/// Note that every table every table is potentially available in multiple [ColumnOrder]s.
/// Also, this data structure handles references,
/// i.e. cases where a table is the same as another except for reordering.
#[derive(Debug, Default)]
pub(super) struct OrderedReferenceManager {
    /// All the stores tables
    stored_tables: Vec<TableStorage>,

    /// The [ColumnOrderMap] associated with each [PermanentTableId]
    /// contains all the [ColumnOrder]s this particular table is available in
    storage_map: HashMap<PermanentTableId, ColumnOrderMap>,
    /// Associates
    reference_map: HashMap<PermanentTableId, Reference>,
}

impl OrderedReferenceManager {
    /// Return a list of all the available [ColumnOrder] for a given table.
    pub fn available_orders(&self, id: PermanentTableId) -> Vec<ColumnOrder> {
        if let Some(order_map) = self.map_id.get(&id) {
            order_map.keys().collect()
        } else {
            Vec::new()
        }
    }

    /// Return the number of rows contained in this table.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn count_rows(&self, id: PermanentTableId) -> usize {
        todo!()
    }

    /// Add a [TableStorage] under a given [PermanentTableId] and [ColumnOrder].
    ///
    /// Returns the [OrderedReferenceId] under which the new table has been stored.
    ///
    /// # Panics
    /// Panics if there already was a table under that id and order.
    fn add_storage(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        storage: TableStorage,
    ) -> OrderedReferenceId {
        match self.storage_map.entry(id) {
            Entry::Occupied(mut entry) => match entry.get_mut().entry(order) {
                Entry::Occupied(_) => {
                    panic!("Table with id {id} and order {order} already exists.");
                }
                Entry::Vacant(order_map) => {
                    let next_storage_id = self.stored_tables.len();
                    order_map.insert(next_storage_id);
                }
            },
            Entry::Vacant(entry) => {
                let next_storage_id = self.stored_tables.len();
                let mut order_map = HashMap::new();
                order_map.insert(order, next_storage_id);

                entry.insert(order_map);
            }
        }

        self.stored_tables.push(storage);
    }

    /// Add a [Trie] of a given [PermanentTableId] and [ColumnOrder].
    pub fn add_trie(&mut self, id: PermanentTableId, order: ColumnOrder, trie: Trie) -> &Trie {
        let new_id = self.add_storage(id, order, TableStorage::InMemory(trie));
        self.stored_tables[new_id]
            .trie_in_memory()
            .expect("In memory trie has been added above")
    }

    /// Add a table represented by a list of [TableSource]s
    pub fn add_sources(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        sources: Vec<TableSource>,
    ) {
        self.add_storage(id, order, TableStorage::FromSources(sources));
    }

    /// Add a (ordered) reference to an existing table.
    pub fn add_reference(
        &mut self,
        existing_id: PermanentTableId,
        reference_id: PermanentTableId,
        permutation: Permutation,
    ) {
        if let Some(reference) = self.reference_map.get(&existing_id) {
            self.reference_map.insert(
                reference_id,
                Reference {
                    id: reference.id,
                    permutation: reference.permutation.chain(&permutation),
                },
            );
        } else {
            self.reference_map.insert(
                reference_id,
                Reference {
                    id: existing_id,
                    permutation,
                },
            );
        }
    }

    /// For a given [ColumnOrder] searches a given list of [ColumnOrder]s
    /// and returns the one that is "closest" to it.
    ///
    /// If the the [ColumnOrder] is present in the list then it will be returned.
    ///
    /// Returns `None` if the list of orders is empty.
    fn closest_order<'a, OrderIter: Iterator<Item = &'a ColumnOrder>>(
        orders: OrderIter,
        order: &ColumnOrder,
    ) -> Option<&'a ColumnOrder> {
        /// Provides a measure of how "difficult" it is to transform a column with this order into another.
        /// Say, `from = {0->2, 1->1, 2->0}` and `to = {0->1, 1->0, 2->2}`.
        /// Starting from position 0 in "from" one needs to skip one layer to reach the 2 in "to" (+1).
        /// Then we need to go back two layers to reach the 1 (+2)
        /// Finally, we go one layer down to reach 0 (+-0).
        /// This gives us an overall score of 3.
        /// Returned value is 0 if and only if from == to.
        #[allow(clippy::cast_possible_wrap)]
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

        orders
            .iter()
            .min_by(|x, y| distance(x, order).cmp(&distance(y, order)))
    }

    fn reorder_to(source: ColumnOrder, target: ColumnOrder, arity: usize) -> ProjectReordering {}

    /// Return the [Trie] for a given [PermanentTableId] and [ColumnOrder]
    ///
    /// This function
    ///     * resolves references
    ///     * loads tables that are not yet present as [Trie]s
    ///     * reorders [Trie] if it is not available in the requested [ColumnOrder]
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn trie(
        &mut self,
        dictionary: &RefCell<Dict>,
        mut id: PermanentTableId,
        mut column_order: ColumnOrder,
    ) -> &Trie {
        if let Some(reference) = self.reference_map.get(&id) {
            id = reference.id;
            column_order = column_order.chain_permutation(&reference.permutation.invert());
        }

        if let Some(order_map) = self.storage_map.get(&id) {
            if let Some(&order_id) = order_map.get(&column_order) {
                return self.stored_tables[order_id].trie(dictionary);
            } else {
                let closest_order = Self::closest_order(order_map.keys(), &column_order);
                let closest_trie = self.storage_map[order_map
                    .get(&closest_order)
                    .expect("clostest_order must be an order that exists")]
                .trie(dictionary);

                let generator = GeneratorProjectReorder::from_reordering(
                    closest_order,
                    column_order,
                    closest_trie.arity(),
                );

                if !generator.is_noop() {
                    TimedCode::instance()
                        .sub("Reasoning/Execution/Required Reorder")
                        .start();

                    let trie_reordered = generator.apply_operation(closest_trie.iter());
                    let result_trie = self.add_trie(id, column_order, trie_reordered);

                    TimedCode::instance()
                        .sub("Reasoning/Execution/Required Reorder")
                        .stop();

                    return result_trie;
                } else {
                    return closest_trie;
                };
            }
        }

        panic!("No table with id {id} exists.");
    }
}
