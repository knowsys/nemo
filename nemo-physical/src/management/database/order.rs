//! This module defines and implements the [OrderedReferenceManager].

use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
};

use bytesize::ByteSize;

use crate::{
    error::Error,
    management::{
        bytesized::sum_bytes, bytesized::ByteSized, execution_plan::ColumnOrder,
        util::closest_order,
    },
    meta::TimedCode,
    tabular::{operations::projectreorder::GeneratorProjectReorder, trie::Trie},
    util::mapping::{permutation::Permutation, traits::NatMapping},
};

use super::{id::PermanentTableId, sources::TableSource, storage::TableStorage, Dict};

/// [OrderedReferenceManager] stores its tables in a [Vec].
/// This id refers to an index in this vector.
pub(super) type StorageId = usize;
/// Associates a [ColumnOrder] with its [StorageId]
type ColumnOrderMap = HashMap<ColumnOrder, StorageId>;

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
    /// as well as the [StorageId] corresponding to a table in `stored_tables`.
    ///
    /// Every table contained in this map refers to an actual table and is not a reference.
    storage_map: HashMap<PermanentTableId, ColumnOrderMap>,
    /// Contains information about all reference tables
    /// by mapping [PermanentTableId] to [Reference]s.
    reference_map: HashMap<PermanentTableId, Reference>,
}

impl OrderedReferenceManager {
    /// If the table with the given [PermanentTableId] is a reference,
    /// this returns the id and order of the actually stored table.
    ///
    /// If the table is not a reference, then this function will simply
    /// return the given arguments.
    fn resolve_reference(
        &self,
        id: PermanentTableId,
        order: ColumnOrder,
    ) -> (PermanentTableId, ColumnOrder) {
        if let Some(reference) = self.reference_map.get(&id) {
            (
                reference.id,
                order.chain_permutation(&reference.permutation),
            )
        } else {
            (id, order)
        }
    }

    /// Return a list of all the available [ColumnOrder] for a given table.
    #[cfg(test)]
    pub fn available_orders(&self, id: PermanentTableId) -> Vec<ColumnOrder> {
        let (id_stored, reorder) = self.resolve_reference(id, ColumnOrder::default());

        if let Some(order_map) = self.storage_map.get(&id_stored) {
            if reorder.is_identity() {
                order_map.keys().cloned().collect()
            } else {
                order_map
                    .keys()
                    .cloned()
                    .map(|order| {
                        reorder
                            .invert()
                            .chain_permutation(&order.chain_permutation(&reorder))
                    })
                    .collect()
            }
        } else {
            Vec::new()
        }
    }

    /// Return the number of rows contained in this table.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn count_rows(&self, id: PermanentTableId) -> usize {
        let (id, _) = self.resolve_reference(id, ColumnOrder::default());

        if let Some(order_map) = self.storage_map.get(&id) {
            for (_, &storage_id) in order_map {
                return self.stored_tables[storage_id].count_rows();
            }

            unreachable!("At least one entry must exist");
        } else {
            unreachable!("Reference has been resolved");
        }
    }

    /// Return the amount of memory consumed by the table under the given [PermanentTableId].
    /// This also includes additional index structures but excludes tables that are currently stored on disk.
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn memory_consumption(&self, id: PermanentTableId) -> ByteSize {
        let (id, _) = self.resolve_reference(id, ColumnOrder::default());

        let mut result = ByteSize::b(0);
        for (_, &storage_id) in self
            .storage_map
            .get(&id)
            .expect("No table with the id {id} exists.")
        {
            result += self.stored_tables[storage_id].size_bytes();
        }

        result
    }

    /// For a given [PermanentTableId] and [ColumnOrder],
    /// either returns the [StorageId] that is associated with it
    /// or the [StorageId] of the new (empty) [TableStorage] object.
    ///
    /// # Panics
    /// Panics if there already was a table under that id and order.
    fn add_storage(&mut self, id: PermanentTableId, order: ColumnOrder) -> StorageId {
        let (id, order) = self.resolve_reference(id, order);

        let storage_id = match self.storage_map.entry(id) {
            Entry::Occupied(mut entry) => match entry.get_mut().entry(order) {
                Entry::Occupied(storage_id) => return *storage_id.get(),
                Entry::Vacant(order_map) => {
                    let next_storage_id = self.stored_tables.len();
                    order_map.insert(next_storage_id);

                    next_storage_id
                }
            },
            Entry::Vacant(entry) => {
                let next_storage_id = self.stored_tables.len();
                let mut order_map = HashMap::new();
                order_map.insert(order, next_storage_id);

                entry.insert(order_map);

                next_storage_id
            }
        };

        self.stored_tables.push(TableStorage::Empty);
        storage_id
    }

    /// Add a [Trie] of a given [PermanentTableId] and [ColumnOrder]
    /// and return its [StorageId].
    ///
    /// This overwrites any existing tables with the same id and order.
    pub fn add_trie(&mut self, id: PermanentTableId, order: ColumnOrder, trie: Trie) -> StorageId {
        let storage_id = self.add_storage(id, order);
        self.stored_tables[storage_id] = TableStorage::InMemory(trie);

        storage_id
    }

    /// Add a table represented by a list of [TableSource]s
    /// and return its [StorageId].
    ///
    /// This overwrites any existing tables with the same id and order.
    pub fn add_sources(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        sources: Vec<TableSource>,
    ) -> StorageId {
        let storage_id = self.add_storage(id, order);
        self.stored_tables[storage_id] = TableStorage::FromSources(sources);

        storage_id
    }

    /// Add a single [TableSource] to an existing table and return its [StorageId].
    ///
    /// In-memory tables under the same [PermanentTableId] and [ColumnOrder]
    /// will be overwritten.
    ///
    /// Table that are given as a list of sources will have this source appended to it.
    pub fn add_source(
        &mut self,
        id: PermanentTableId,
        order: ColumnOrder,
        source: TableSource,
    ) -> StorageId {
        let storage_id = self.add_storage(id, order);
        match &mut self.stored_tables[storage_id] {
            TableStorage::FromSources(sources) => sources.push(source),
            _ => self.stored_tables[storage_id] = TableStorage::FromSources(vec![source]),
        }

        storage_id
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
                    permutation: reference.permutation.chain_permutation(&permutation),
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

    /// Return the [StorageId] of a [Trie]
    /// corresponding to the given [PermanentTableId] and [ColumnOrder].
    ///
    /// This function
    ///     * resolves references
    ///     * loads tables that are not yet present as [Trie]s
    ///     * reorders [Trie] if it is not available in the requested [ColumnOrder]
    ///
    /// # Panics
    /// Panics if the given id does not exist.
    pub fn trie_id(
        &mut self,
        dictionary: &RefCell<Dict>,
        id: PermanentTableId,
        column_order: ColumnOrder,
    ) -> Result<StorageId, Error> {
        let (id, column_order) = self.resolve_reference(id, column_order);

        if let Some(order_map) = self.storage_map.get(&id) {
            if let Some(&storage_id) = order_map.get(&column_order) {
                self.stored_tables[storage_id].trie(dictionary)?;
                return Ok(storage_id);
            } else {
                let (_, closest_order) = closest_order(order_map.keys(), &column_order)
                    .expect("Trie should exist at least in one order.");
                let closest_storage_id = *order_map
                    .get(&closest_order)
                    .expect("clostest_order must be an order that exists");
                let closest_arity = self.stored_tables[closest_storage_id].arity();

                let generator = GeneratorProjectReorder::from_reordering(
                    closest_order.clone(),
                    column_order.clone(),
                    closest_arity,
                );

                if !generator.is_noop() {
                    TimedCode::instance()
                        .sub("Reasoning/Execution/Required Reorder")
                        .start();

                    let closest_trie = self.stored_tables[closest_storage_id].trie(dictionary)?;
                    let trie_reordered = generator.apply_operation(closest_trie.full_iterator());
                    let result_storage_id = self.add_trie(id, column_order, trie_reordered);

                    TimedCode::instance()
                        .sub("Reasoning/Execution/Required Reorder")
                        .stop();

                    return Ok(result_storage_id);
                } else {
                    self.stored_tables[closest_storage_id].trie(dictionary)?;
                    return Ok(closest_storage_id);
                };
            }
        }

        panic!("No table with id {id} exists.");
    }

    /// Given a [StorageId] return a reference to a [Trie].
    ///
    /// # Panics
    /// Panics if the table is not available as a trie.
    /// This can be ensured by obtaining this id from the function `trie_id`.
    pub fn trie(&self, id: StorageId) -> &Trie {
        self.stored_tables[id]
            .trie_in_memory()
            .expect("This function assumes that the given id corresponds to an in memory trie")
    }
}

impl ByteSized for OrderedReferenceManager {
    fn size_bytes(&self) -> ByteSize {
        sum_bytes(self.stored_tables.iter().map(|table| table.size_bytes()))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        management::{
            database::{
                id::{PermanentTableId, TableId},
                order::OrderedReferenceManager,
                sources::{SimpleTable, TableSource},
            },
            execution_plan::ColumnOrder,
        },
        util::mapping::permutation::Permutation,
    };

    fn empty_source() -> TableSource {
        TableSource::from_simple_table(SimpleTable::new(1))
    }

    #[test]
    fn test_reference_manager() {
        let mut current_id = PermanentTableId::default();
        let mut manager = OrderedReferenceManager::default();

        let id_present = current_id.increment();

        let order_first = ColumnOrder::from_vector(vec![4, 2, 1, 0, 3]);
        manager.add_source(id_present, order_first, empty_source());

        let id_reference = current_id.increment();
        let permutation_reference = Permutation::from_vector(vec![2, 3, 4, 1, 0]);
        manager.add_reference(id_present, id_reference, permutation_reference);

        let order_second = ColumnOrder::from_vector(vec![4, 0, 1, 2, 3]);
        manager.add_source(id_present, order_second, empty_source());

        let order_third = ColumnOrder::from_vector(vec![3, 4, 0, 1, 2]);
        manager.add_source(id_reference, order_third, empty_source());

        let reference_available_orders = vec![
            ColumnOrder::from_vector(vec![3, 4, 1, 0, 2]),
            ColumnOrder::from_vector(vec![3, 0, 1, 4, 2]),
            ColumnOrder::from_vector(vec![0, 2, 1, 3, 4]),
        ];

        let available_orders = manager.available_orders(id_reference);
        assert_eq!(available_orders.len(), 3);
        for order in available_orders {
            assert!(reference_available_orders.iter().any(|o| o == &order));
        }

        let requested_order = ColumnOrder::from_vector(vec![0, 3, 1, 2, 4]);
        let expected_order = ColumnOrder::from_vector(vec![1, 2, 4, 3, 0]);
        let resolved = manager.resolve_reference(id_reference, requested_order);
        assert_eq!(resolved, (id_present, expected_order));

        let id_second_reference = current_id.increment();
        let permutation_second_reference = Permutation::from_vector(vec![4, 3, 1, 2, 0]);
        manager.add_reference(
            id_reference,
            id_second_reference,
            permutation_second_reference,
        );

        let reference_available_orders = vec![
            ColumnOrder::from_vector(vec![3, 4, 0, 2, 1]),
            ColumnOrder::from_vector(vec![3, 0, 4, 2, 1]),
            ColumnOrder::from_vector(vec![0, 1, 3, 2, 4]),
        ];

        let available_orders = manager.available_orders(id_second_reference);
        assert_eq!(available_orders.len(), 3);
        for order in available_orders {
            assert!(reference_available_orders.iter().any(|o| o == &order));
        }
    }
}
