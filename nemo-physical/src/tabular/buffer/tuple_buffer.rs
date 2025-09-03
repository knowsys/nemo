//! This module defines [TupleBuffer].

use std::{cmp::Ordering, collections::HashMap};

use crate::{
    datatypes::{
        Double, Float, StorageTypeName, StorageValueT, storage_type_name::NUM_STORAGETYPES,
    },
    datavalues::AnyDataValue,
    function::{evaluation::StackProgram, tree::FunctionTree},
    management::database::Dict,
};

use super::sorted_tuple_buffer::SortedTupleBuffer;

/// Stores the columns for every table in [TupleBuffer]
///
/// TODO: We currently simply use a vector for storing the columnar data.
/// This might not be ideal because every time the capacity of a vector is exceeded,
/// it copies all preexisting values to a new location which can hold twice as many values.
/// Instead, one should consider a arena bump allocator.
/// From there, one could either move the entries of that allocator into a [Vec]
/// for sorting and index based access, or implement those methods on the allocator itself.
#[derive(Debug, Default)]
struct TypedTableStorage {
    /// List of all u32 value columns used across the tables.
    columns_id32: Vec<Vec<u32>>,
    /// List of all u64 value columns used across the tables.
    columns_id64: Vec<Vec<u64>>,
    /// List of all Float value columns used across the tables.
    columns_float: Vec<Vec<Float>>,
    /// List of all Double value columns used across the tables.
    columns_double: Vec<Vec<Double>>,
    /// List of all i64 value columns used across the tables.
    columns_i64: Vec<Vec<i64>>,
}

impl TypedTableStorage {
    /// Add a tuple to a table identified by `table_record` and the types used in `tuple`.
    pub(crate) fn push_tuple(&mut self, tuple: &[StorageValueT], table_record: &TypedTableRecord) {
        for (&value, &id) in tuple.iter().zip(table_record.storage_indices.iter()) {
            match value {
                StorageValueT::Id32(value) => self.columns_id32[id].push(value),
                StorageValueT::Id64(value) => self.columns_id64[id].push(value),
                StorageValueT::Int64(value) => self.columns_i64[id].push(value),
                StorageValueT::Float(value) => self.columns_float[id].push(value),
                StorageValueT::Double(value) => self.columns_double[id].push(value),
            }
        }
    }

    /// Add a new column and return its index
    fn add_new_column<T>(columns: &mut Vec<Vec<T>>) -> usize {
        let column_index = columns.len();
        columns.push(Vec::new());

        column_index
    }

    /// Add empty columns of the appropriate types.
    pub(crate) fn initialize_new_subtable(
        &mut self,
        storage_types: &[StorageTypeName],
    ) -> TypedTableRecord {
        let mut storage_indices = Vec::<usize>::with_capacity(storage_types.len());

        for storage_type in storage_types {
            let column_index = match storage_type {
                StorageTypeName::Id32 => Self::add_new_column(&mut self.columns_id32),
                StorageTypeName::Id64 => Self::add_new_column(&mut self.columns_id64),
                StorageTypeName::Int64 => Self::add_new_column(&mut self.columns_i64),
                StorageTypeName::Float => Self::add_new_column(&mut self.columns_float),
                StorageTypeName::Double => Self::add_new_column(&mut self.columns_double),
            };

            storage_indices.push(column_index);
        }

        TypedTableRecord {
            storage_types: storage_types.to_vec(),
            storage_indices,
            current_length: 0,
        }
    }

    /// Return the stored value in from a given type, id and tuple index.
    fn stored_value(
        &self,
        storage_type: StorageTypeName,
        column_id: usize,
        local_index: usize,
    ) -> StorageValueT {
        match storage_type {
            StorageTypeName::Id32 => StorageValueT::Id32(self.columns_id32[column_id][local_index]),
            StorageTypeName::Id64 => StorageValueT::Id64(self.columns_id64[column_id][local_index]),
            StorageTypeName::Int64 => {
                StorageValueT::Int64(self.columns_i64[column_id][local_index])
            }
            StorageTypeName::Float => {
                StorageValueT::Float(self.columns_float[column_id][local_index])
            }
            StorageTypeName::Double => {
                StorageValueT::Double(self.columns_double[column_id][local_index])
            }
        }
    }

    /// Compare two stored values of the same type-
    pub(crate) fn compare_stored_values(
        &self,
        storage_type: StorageTypeName,
        column_id_first: usize,
        column_id_second: usize,
        local_index_first: usize,
        local_index_second: usize,
    ) -> Ordering {
        match storage_type {
            StorageTypeName::Id32 => self.columns_id32[column_id_first][local_index_first]
                .cmp(&self.columns_id32[column_id_second][local_index_second]),
            StorageTypeName::Id64 => self.columns_id64[column_id_first][local_index_first]
                .cmp(&self.columns_id64[column_id_second][local_index_second]),
            StorageTypeName::Int64 => self.columns_i64[column_id_first][local_index_first]
                .cmp(&self.columns_i64[column_id_second][local_index_second]),
            StorageTypeName::Float => self.columns_float[column_id_first][local_index_first]
                .cmp(&self.columns_float[column_id_second][local_index_second]),
            StorageTypeName::Double => self.columns_double[column_id_first][local_index_first]
                .cmp(&self.columns_double[column_id_second][local_index_second]),
        }
    }
}

/// Object for associating a series of [StorageTypeName]s to a specific subtable
#[derive(Debug)]
struct TypedTableLookup {
    /// A linearlized trie data structure to find the subtable id for a given list of [StorageTypeName]s
    ///
    /// Each type in [StorageTypeName] is assigned a number between 0 and [NUM_STORAGETYPES] by `Self::storage_type_number`.
    /// This vector represents a tree with branching degree of [NUM_STORAGETYPES] but can be partial: a slice of length [NUM_STORAGETYPES]
    /// can encode one inner node in the tree, where the usizes refer to the offset of the child node of the tree
    /// for each [StorageTypeName], or (on the last level) to the subtable id of a  [TypedTableRecord].
    /// In each case, a value of `Self::NO_SUCCESSOR` indicates that no child node/no table has been alocated yet
    /// for this type combination or partial path.
    lookup_trie: Vec<usize>,
}

impl TypedTableLookup {
    const NO_SUCCESSOR: usize = usize::MAX;

    /// Create a new [TypedTableLookup].
    pub(crate) fn new() -> Self {
        let lookup_trie = vec![Self::NO_SUCCESSOR; NUM_STORAGETYPES];

        Self { lookup_trie }
    }

    /// Return the subtable id corresponding to the provided series of [StorageTypeName]s.
    /// If no subtable for the combination of types exist, then it will be given a new subtable id,
    /// which will be the same as the parameter `num_subtables`, which is the number of subtables
    /// before this function call.
    pub(crate) fn subtable_id(
        &mut self,
        storage_types: &[StorageTypeName],
        num_subtables: usize,
    ) -> Option<usize> {
        let mut lookup_trie_block: usize = 0;

        for (lookup_trie_layer, storage_type) in storage_types.iter().enumerate() {
            let next_block = self.lookup_trie[lookup_trie_block + storage_type.order()];

            if next_block == Self::NO_SUCCESSOR {
                self.add_new_lookup_trie_(
                    &storage_types[lookup_trie_layer..],
                    lookup_trie_block,
                    num_subtables,
                );

                return None;
            }

            lookup_trie_block = next_block;
        }

        Some(lookup_trie_block)
    }

    fn add_new_lookup_trie_(
        &mut self,
        missing_storage_types: &[StorageTypeName],
        current_block: usize,
        new_id: usize,
    ) {
        // Start index of the first new block
        let new_block = self.lookup_trie.len();

        // Add empty trie node slots for all remaining levels.
        // Note that the block we ended up on before calling this function has already been allocated.
        self.lookup_trie.resize(
            self.lookup_trie.len() + (missing_storage_types.len() - 1) * NUM_STORAGETYPES,
            Self::NO_SUCCESSOR,
        );

        if let Some((last_type, missing_types)) = missing_storage_types.split_last() {
            let mut current_block = current_block;
            let mut next_block = new_block;

            for storage_type in missing_types {
                // For every layer that is not the last add a pointer to the next block
                self.lookup_trie[current_block + storage_type.order()] = next_block;

                current_block = next_block;
                next_block += NUM_STORAGETYPES;
            }

            // In the last layer add a pointer to the new id
            self.lookup_trie[current_block + last_type.order()] = new_id;
        }
    }
}

/// Represents a typed subtable of [TupleBuffer]
#[derive(Debug)]
pub(super) struct TypedTableRecord {
    /// For each column, contains its data type
    pub(super) storage_types: Vec<StorageTypeName>,
    /// For each column, contains an index into the `column_*` members of [TypedTableStorage]
    pub(super) storage_indices: Vec<usize>,

    /// The amount of tuple currently contained in this subtable
    current_length: usize,
}

/// A filter or transformation applied to a position
#[derive(Debug)]
pub struct TransformPosition {
    position: usize,
    program: StackProgram,
}

impl TransformPosition {
    /// Construct a new transformation.
    pub fn new(position: usize, value: FunctionTree<usize>) -> Self {
        let reference_map = value
            .references()
            .into_iter()
            .map(|position| (position, position))
            .collect::<HashMap<_, _>>();
        let program = StackProgram::from_function_tree(&value, &reference_map, None);

        Self { position, program }
    }
}

/// A pattern that can be used to filter and transform the tuples in a [TupleBuffer].
#[derive(Debug)]
pub struct FilterTransformPattern {
    filter: StackProgram,
    transformations: Vec<TransformPosition>,
}

impl FilterTransformPattern {
    /// Construct a new [FilterTransformPattern] from a filter and transformations.
    pub fn new(filter: FunctionTree<usize>, transformations: Vec<TransformPosition>) -> Self {
        let reference_map = filter
            .references()
            .into_iter()
            .map(|position| (position, position))
            .collect::<HashMap<_, _>>();
        let filter_program = StackProgram::from_function_tree(&filter, &reference_map, None);
        Self {
            filter: filter_program,
            transformations,
        }
    }
}

/// Represents a row-based table containing values of arbitrary data types
#[derive(Debug)]
pub(crate) struct TupleBuffer {
    /// Conceptionally, one may imagine the table represented by the [TupleBuffer]
    /// to be split into several subtables that only contain rows with certain fixed types.
    /// E.g. one subtable might contain tuples of type ([StorageTypeName::Id32], [StorageTypeName::Int64])
    /// and another ([StorageTypeName::Id64], [StorageTypeName::Id64]).
    /// Each entry in this vector represents one such subtable.
    /// Its index in this vector is then its subtable id.
    typed_subtables: Vec<TypedTableRecord>,

    /// Maps a series of [StorageTypeName] to a subtable id in `typed_subtables`
    table_lookup: TypedTableLookup,
    /// Stores the contents of the subtables in `typed_subtables`
    table_storage: TypedTableStorage,

    /// Current tuple
    current_tuple: Box<[StorageValueT]>,
    /// Current tuple, as data values
    current_tuple_data_values: Box<[AnyDataValue]>,
    /// T ypes of the current tuple
    current_tuple_types: Box<[StorageTypeName]>,
    /// Column Index of the currently written tuple
    current_tuple_index: usize,

    /// Patterns to filter and transform the tuples before committing.
    patterns: Vec<FilterTransformPattern>,
}

impl TupleBuffer {
    /// Create a new [TupleBuffer].
    pub(crate) fn new(column_number: usize) -> Self {
        Self::with_patterns(column_number, Vec::new())
    }

    /// Create a new [TupleBuffer] with the given [FilterTransformPattern]s.
    pub(crate) fn with_patterns(
        column_number: usize,
        patterns: Vec<FilterTransformPattern>,
    ) -> Self {
        Self {
            typed_subtables: Vec::new(),
            table_lookup: TypedTableLookup::new(),
            table_storage: TypedTableStorage::default(),
            current_tuple: vec![StorageValueT::Id32(0); column_number].into_boxed_slice(), // Picked arbitrarily
            current_tuple_data_values: vec![AnyDataValue::new_integer_from_u64(0); column_number]
                .into_boxed_slice(), // Picked arbitrarily
            current_tuple_types: vec![StorageTypeName::Id32; column_number].into_boxed_slice(), // Picked arbitrarily
            current_tuple_index: 0,
            patterns,
        }
    }

    /// Write the data of `current_tuple` into `table_storage`,
    /// potentially creating a new entry in `typed_subtables`
    /// and the appropriate information into `table_lookup`.
    fn write_tuple(&mut self) {
        let current_record = if let Some(subtable_id) = self
            .table_lookup
            .subtable_id(&self.current_tuple_types, self.typed_subtables.len())
        {
            &mut self.typed_subtables[subtable_id]
        } else {
            let new_record = self
                .table_storage
                .initialize_new_subtable(&self.current_tuple_types);
            self.typed_subtables.push(new_record);

            self.typed_subtables
                .last_mut()
                .expect("This vector received a new entry in the line above")
        };

        current_record.current_length += 1;
        self.table_storage
            .push_tuple(&self.current_tuple, current_record);
    }

    /// Provide the next value for the current tuple. Values are added in in order.
    /// When the value for the last column was provided, the tuple is committed to the buffer.
    /// Alternatively, a partially built tuple can be abandonded by calling `drop_current_tuple`.
    pub(crate) fn add_tuple_value(&mut self, value: StorageValueT) {
        self.current_tuple_types[self.current_tuple_index] = value.get_type();
        self.current_tuple[self.current_tuple_index] = value;
        self.current_tuple_index += 1;

        if self.current_tuple_index >= self.column_number() {
            self.current_tuple_index = 0;
            self.write_tuple();
        }
    }

    /// Add the next value for the current tuple, also providing the
    /// dictionary and the underlying [AnyDataValue] (allowing
    /// filtering and transformations).
    pub(crate) fn add_tuple_data_value(&mut self, dictionary: &mut Dict, data_value: AnyDataValue) {
        let value = data_value.to_storage_value_t_dict(dictionary);
        self.current_tuple_data_values[self.current_tuple_index] = data_value;
        self.current_tuple[self.current_tuple_index] = value;
        self.current_tuple_types[self.current_tuple_index] = value.get_type();
        self.current_tuple_index += 1;

        if self.current_tuple_index >= self.column_number() {
            self.current_tuple_index = 0;

            if self.match_filters_and_transform(dictionary) {
                log::trace!(
                    "tuple {:?} matched a filter",
                    self.current_tuple_data_values
                );
                self.write_tuple();
            }
        }
    }

    /// Forget about any previously added values that have not formed a complete tuple yet,
    /// and start a new tuple from the beginning.
    pub(crate) fn drop_current_tuple(&mut self) {
        self.current_tuple_index = 0;
    }

    /// Finish writing to the [TupleBuffer] and return a [SortedTupleBuffer].
    pub(crate) fn finalize(self) -> SortedTupleBuffer {
        SortedTupleBuffer::new(self)
    }

    /// Returns the number of columns on the table, i.e., the
    /// number of values that need to be written to make one tuple.
    pub(crate) fn column_number(&self) -> usize {
        self.current_tuple.len()
    }

    /// Returns the number of rows in the [TupleBuffer]
    pub(crate) fn size(&self) -> usize {
        self.typed_subtables
            .iter()
            .map(|record| record.current_length)
            .sum()
    }
}

impl TupleBuffer {
    /// Returns an iterator over the lnegth of each subtable.
    pub(super) fn subtable_lengths(&self) -> impl Iterator<Item = &usize> {
        self.typed_subtables
            .iter()
            .map(|record| &record.current_length)
    }

    /// For a given subtable id return the corresponding [TypedTableRecord].
    pub(super) fn subtable_record(&self, subtable_id: usize) -> &TypedTableRecord {
        &self.typed_subtables[subtable_id]
    }

    /// Return the stored value identfied by
    /// the subtable id, the local tuple index within the subtable and a column index.
    ///
    /// # Panics
    /// Panics if no entry for the specified parameters exists.
    pub(super) fn stored_value(
        &self,
        subtable_id: usize,
        local_tuple_index: usize,
        column_index: usize,
    ) -> StorageValueT {
        let subtable = &self.typed_subtables[subtable_id];

        self.table_storage.stored_value(
            subtable.storage_types[column_index],
            subtable.storage_indices[column_index],
            local_tuple_index,
        )
    }

    /// Compare two stored values of the same type.
    pub(super) fn compare_stored_values(
        &self,
        storage_type: StorageTypeName,
        column_id_first: usize,
        column_id_second: usize,
        local_index_first: usize,
        local_index_second: usize,
    ) -> Ordering {
        self.table_storage.compare_stored_values(
            storage_type,
            column_id_first,
            column_id_second,
            local_index_first,
            local_index_second,
        )
    }

    fn pattern_matches(&self, pattern: &FilterTransformPattern) -> bool {
        log::trace!(
            "checking pattern {pattern:?} on tuple {:?}",
            self.current_tuple_data_values
        );
        pattern
            .filter
            .evaluate_bool(&self.current_tuple_data_values, None)
            == Some(true)
    }

    fn match_filters_and_transform(&mut self, dictionary: &mut Dict) -> bool {
        if self.patterns.is_empty() {
            log::trace!("no patterns to check");
            // we don't have any patterns to check, let everything through
            return true;
        }

        for pattern in &self.patterns {
            if !self.pattern_matches(pattern) {
                log::trace!(
                    "pattern {pattern:?} did not match on tuple: {:?}",
                    self.current_tuple_data_values
                );
                continue;
            }

            for transformation in &pattern.transformations {
                let value = transformation
                    .program
                    .evaluate_data(&self.current_tuple_data_values)
                    .expect("should evaluate to a value");
                self.current_tuple[transformation.position] =
                    value.to_storage_value_t_dict(dictionary);
            }

            // first matching pattern wins
            return true;
        }

        false
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datatypes::StorageValueT,
        datavalues::AnyDataValue,
        dictionary::meta_dv_dict::MetaDvDictionary,
        function::tree::FunctionTree,
        tabular::buffer::tuple_buffer::{FilterTransformPattern, TransformPosition, TupleBuffer},
    };
    use test_log::test;

    #[test]
    fn tuple_buffer_internals() {
        let mut tuple_buffer = TupleBuffer::new(3);

        let v1 = StorageValueT::Id32(0);
        let v2 = StorageValueT::Int64(42);

        // new table #0, row #0
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        // new table #1, row #0
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v2);
        tuple_buffer.add_tuple_value(v1);
        // table #0, row #1
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        // new table #2, row #0
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v2);
        // table #2, row #1
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v2);
        // table #2, row #2
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v1);
        tuple_buffer.add_tuple_value(v2);

        assert_eq!(tuple_buffer.typed_subtables.len(), 3);

        assert_eq!(tuple_buffer.typed_subtables[0].current_length, 2);
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[0].storage_indices[0]]
                .len(),
            2
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[0].storage_indices[1]]
                .len(),
            2
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[0].storage_indices[2]]
                .len(),
            2
        );

        assert_eq!(tuple_buffer.typed_subtables[1].current_length, 1);
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[1].storage_indices[0]]
                .len(),
            1
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_i64
                [tuple_buffer.typed_subtables[1].storage_indices[1]]
                .len(),
            1
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[1].storage_indices[2]]
                .len(),
            1
        );

        assert_eq!(tuple_buffer.typed_subtables[2].current_length, 3);
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[2].storage_indices[0]]
                .len(),
            3
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_id32
                [tuple_buffer.typed_subtables[2].storage_indices[1]]
                .len(),
            3
        );
        assert_eq!(
            tuple_buffer.table_storage.columns_i64
                [tuple_buffer.typed_subtables[2].storage_indices[2]]
                .len(),
            3
        );

        assert_eq!(tuple_buffer.stored_value(0, 0, 0), v1);
        assert_eq!(tuple_buffer.stored_value(0, 0, 1), v1);
        assert_eq!(tuple_buffer.stored_value(0, 0, 2), v1);
        assert_eq!(tuple_buffer.stored_value(0, 1, 0), v1);
        assert_eq!(tuple_buffer.stored_value(0, 1, 1), v1);
        assert_eq!(tuple_buffer.stored_value(0, 1, 2), v1);

        assert_eq!(tuple_buffer.stored_value(1, 0, 0), v1);
        assert_eq!(tuple_buffer.stored_value(1, 0, 1), v2);
        assert_eq!(tuple_buffer.stored_value(1, 0, 2), v1);

        assert_eq!(tuple_buffer.stored_value(2, 0, 0), v1);
        assert_eq!(tuple_buffer.stored_value(2, 0, 1), v1);
        assert_eq!(tuple_buffer.stored_value(2, 0, 2), v2);
        assert_eq!(tuple_buffer.stored_value(2, 1, 0), v1);
        assert_eq!(tuple_buffer.stored_value(2, 1, 1), v1);
        assert_eq!(tuple_buffer.stored_value(2, 1, 2), v2);
        assert_eq!(tuple_buffer.stored_value(2, 2, 0), v1);
        assert_eq!(tuple_buffer.stored_value(2, 2, 1), v1);
        assert_eq!(tuple_buffer.stored_value(2, 2, 2), v2);
    }

    #[test]
    fn filter_and_transform() {
        let patterns = vec![
            FilterTransformPattern::new(
                FunctionTree::equals(
                    FunctionTree::constant(AnyDataValue::new_integer_from_u64(23)),
                    FunctionTree::reference(1),
                ),
                Vec::new(),
            ),
            FilterTransformPattern::new(
                FunctionTree::numeric_greaterthaneq(
                    FunctionTree::string_length(FunctionTree::reference(0)),
                    FunctionTree::constant(AnyDataValue::new_integer_from_u64(4)),
                ),
                vec![TransformPosition::new(
                    1,
                    FunctionTree::numeric_addition(
                        FunctionTree::reference(1),
                        FunctionTree::constant(AnyDataValue::new_integer_from_u64(1295)),
                    ),
                )],
            ),
        ];
        let mut tuple_buffer = TupleBuffer::with_patterns(2, patterns);
        let mut dictionary = MetaDvDictionary::new();

        let dv1 = AnyDataValue::new_plain_string("foo".to_string());
        let dv2 = AnyDataValue::new_integer_from_u64(23);
        let dv3 = AnyDataValue::new_integer_from_u64(42);
        let dv4 = AnyDataValue::new_plain_string("quux".to_string());

        tuple_buffer.add_tuple_data_value(&mut dictionary, dv1.clone());
        tuple_buffer.add_tuple_data_value(&mut dictionary, dv2.clone());

        tuple_buffer.add_tuple_data_value(&mut dictionary, dv1.clone());
        tuple_buffer.add_tuple_data_value(&mut dictionary, dv3.clone());

        tuple_buffer.add_tuple_data_value(&mut dictionary, dv4.clone());
        tuple_buffer.add_tuple_data_value(&mut dictionary, dv2.clone());

        tuple_buffer.add_tuple_data_value(&mut dictionary, dv4.clone());
        tuple_buffer.add_tuple_data_value(&mut dictionary, dv3.clone());

        assert_eq!(tuple_buffer.subtable_lengths().collect::<Vec<_>>(), &[&3]);

        assert_eq!(tuple_buffer.stored_value(0, 0, 0), StorageValueT::Id32(0));
        assert_eq!(tuple_buffer.stored_value(0, 0, 1), StorageValueT::Int64(23));

        assert_eq!(tuple_buffer.stored_value(0, 1, 0), StorageValueT::Id32(1));
        assert_eq!(tuple_buffer.stored_value(0, 1, 1), StorageValueT::Int64(23));

        assert_eq!(tuple_buffer.stored_value(0, 2, 0), StorageValueT::Id32(1));
        assert_eq!(
            tuple_buffer.stored_value(0, 2, 1),
            StorageValueT::Int64(1337)
        );
    }
}
