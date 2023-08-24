use std::cell::UnsafeCell;
use std::mem::size_of;
use std::ops::Deref;
use std::{debug_assert, iter};

use bytesize::ByteSize;

use crate::columnar::operations::{ColumnScanCast, ColumnScanCastEnum};
use crate::datatypes::storage_value::StorageValueIteratorT;
use crate::generate_cast_statements;
use crate::permutator::Permutator;

use crate::columnar::traits::columnscan::{ColumnScanCell, ColumnScanEnum};
use crate::columnar::{
    adaptive_column_builder::{ColumnBuilderAdaptive, ColumnBuilderAdaptiveT},
    column_types::interval::{ColumnWithIntervals, ColumnWithIntervalsT},
    traits::{
        column::Column,
        columnbuilder::ColumnBuilder,
        columnscan::{ColumnScan, ColumnScanT},
    },
};
use crate::datatypes::{storage_value::VecT, StorageTypeName, StorageValueT};
use crate::dictionary::value_serializer::{StorageValueMapping, TrieSerializer};
use crate::dictionary::ValueSerializer;
use crate::management::database::Dict;
use crate::management::ByteSized;
use crate::tabular::operations::TrieScanPrune;
use crate::tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum};
use crate::tabular::traits::table::Table;
use crate::tabular::traits::table_schema::TableSchema;
use crate::tabular::traits::trie_scan::TrieScan;

/// An iterator over values in a trie, with some datatype mapping.
#[derive(Debug)]
pub struct TrieRecords<Scan, Mapping, Output> {
    rows: Scan,
    last_record: Vec<Output>,
    mapping: Mapping,
}

impl<Scan, Mapping, Output> TrieRecords<Scan, Mapping, Output>
where
    Scan: TrieScan,
    Mapping: StorageValueMapping<Output>,
{
    /// Return an iterator over the next mapped record.
    pub fn next_record(&mut self) -> Option<std::slice::Iter<'_, Output>> {
        let arity = self.rows.column_types().len();
        let changed_idx = self.rows.advance_on_layer(arity - 1)?;

        self.last_record.truncate(changed_idx);

        for layer in changed_idx..arity {
            let value = self.rows.current(layer);
            let str_value = self.mapping.map(value, layer);
            self.last_record.push(str_value);
        }

        Some(self.last_record.iter())
    }
}

impl<D, S, Scan> TrieSerializer for TrieRecords<Scan, ValueSerializer<D, S>, String>
where
    D: Deref<Target = Dict>,
    S: Deref<Target = TableSchema>,
    Scan: TrieScan,
{
    type SerializedValue = String;
    type SerializedRecord<'r> = std::slice::Iter<'r, String> where Self: 'r;

    fn next_serialized(&mut self) -> Option<Self::SerializedRecord<'_>> {
        self.next_record()
    }
}

/// Implementation of a trie data structure.
/// The underlying data is oragnized in IntervalColumns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trie {
    types: Vec<StorageTypeName>,
    columns: Vec<ColumnWithIntervalsT>,
}

impl Trie {
    /// Construct a new Trie from a given schema and a vector of IntervalColumns.
    pub fn new(columns: Vec<ColumnWithIntervalsT>) -> Self {
        let types = columns.iter().map(|col| col.get_type()).collect();
        Self { types, columns }
    }

    /// Return reference to all columns.
    pub fn columns(&self) -> &Vec<ColumnWithIntervalsT> {
        &self.columns
    }

    /// Return mutable reference to all columns.
    pub fn columns_mut(&mut self) -> &mut Vec<ColumnWithIntervalsT> {
        &mut self.columns
    }

    /// Return reference to a column given an index.
    ///
    /// # Panics
    /// Panics if index is out of range
    pub fn get_column(&self, index: usize) -> &ColumnWithIntervalsT {
        &self.columns[index]
    }

    /// Returns the sum of the lengths of each column
    pub fn num_elements(&self) -> usize {
        let mut result = 0;

        for column in &self.columns {
            result += column.len();
        }

        result
    }

    /// Convert the trie into a vector of columns with equal length.
    pub fn as_column_vector(&self) -> Vec<VecT> {
        if self.columns.is_empty() {
            return Vec::new();
        }

        // outer vecs are build in reverse order
        let mut last_interval_lengths: Vec<usize> = self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|_| 1)
            .collect();

        macro_rules! last_column_for_datatype {
            ($variant:ident) => {{
                vec![VecT::$variant(
                    self.columns
                        .last()
                        .expect("we return early if columns are empty")
                        .iter()
                        .map(|val| match val {
                            StorageValueT::$variant(constant) => constant,
                            _ => panic!("Unsupported type"),
                        })
                        .collect(),
                )]
            }};
        }

        let mut result_columns: Vec<VecT> = match self
            .types
            .last()
            .expect("we return early if columns are empty")
        {
            StorageTypeName::U32 => last_column_for_datatype!(U32),
            StorageTypeName::U64 => last_column_for_datatype!(U64),
            StorageTypeName::I64 => last_column_for_datatype!(I64),
            StorageTypeName::Float => last_column_for_datatype!(Float),
            StorageTypeName::Double => last_column_for_datatype!(Double),
        };

        for column_index in (0..(self.columns.len() - 1)).rev() {
            let current_column = &self.columns[column_index];
            let current_type = self.types[column_index];
            let last_column = &self.columns[column_index + 1];

            let current_interval_lengths: Vec<usize> = (0..current_column.len())
                .map(|element_index_in_current_column| {
                    last_column
                        .int_bounds(element_index_in_current_column)
                        .map(|index_in_interval| last_interval_lengths[index_in_interval])
                        .sum()
                })
                .collect();

            let padding_lengths = current_interval_lengths.iter().map(|length| length - 1);

            macro_rules! push_column_for_datatype {
                ($variant:ident) => {{
                    result_columns.push(VecT::$variant(
                        current_column
                            .iter()
                            .zip(padding_lengths)
                            .flat_map(|(val, pl)| {
                                iter::once(match val {
                                    StorageValueT::$variant(constant) => constant,
                                    _ => panic!("Unsupported type"),
                                })
                                .chain(
                                    iter::repeat(match val {
                                        StorageValueT::$variant(constant) => constant,
                                        _ => panic!("Unsupported type"),
                                    })
                                    .take(pl),
                                )
                            })
                            .collect(),
                    ));
                }};
            }

            match current_type {
                StorageTypeName::U32 => push_column_for_datatype!(U32),
                StorageTypeName::U64 => push_column_for_datatype!(U64),
                StorageTypeName::I64 => push_column_for_datatype!(I64),
                StorageTypeName::Float => push_column_for_datatype!(Float),
                StorageTypeName::Double => push_column_for_datatype!(Double),
            };

            last_interval_lengths = current_interval_lengths;
        }

        result_columns.reverse();
        result_columns
    }

    /// Returns a [`TrieScan`] over this table.
    pub fn scan(&self) -> impl TrieScan + '_ {
        TrieScanPrune::new(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(self)))
    }

    /// Returns an iterator over serialized rows.
    pub fn records<'a, D, S, Output>(
        &'a self,
        serializer: ValueSerializer<D, S>,
    ) -> TrieRecords<impl TrieScan + 'a, ValueSerializer<D, S>, Output>
    where
        D: Deref<Target = Dict> + 'a,
        S: Deref<Target = TableSchema> + 'a,
    {
        let num_columns = self.columns.len();

        TrieRecords {
            rows: self.scan(),
            last_record: Vec::with_capacity(num_columns),
            mapping: serializer,
        }
    }

    fn get_number_of_repetitions_at_position(&self, col_idx: usize, data_idx: usize) -> usize {
        debug_assert!(!self.columns.is_empty());
        debug_assert!(col_idx < self.columns.len());
        debug_assert!(data_idx < self.columns[col_idx].len());

        if col_idx == self.columns.len() - 1 {
            1
        } else if col_idx == self.columns.len() - 2 {
            self.columns[col_idx + 1].int_bounds(data_idx).len()
        } else {
            self.columns[col_idx + 1]
                .int_bounds(data_idx)
                .map(|new_data_idx| {
                    self.get_number_of_repetitions_at_position(col_idx + 1, new_data_idx)
                })
                .sum()
        }
    }

    fn get_full_iterator_for_col_idx(&self, col_idx: usize) -> StorageValueIteratorT {
        debug_assert!(!self.columns.is_empty());
        debug_assert!(col_idx < self.columns.len());

        let col = &self.columns[col_idx];

        macro_rules! build_iter {
            ($variant:ident, $c:ident) => {
                StorageValueIteratorT::$variant(Box::new($c.iter().enumerate().flat_map(
                    move |(i, v)| {
                        std::iter::repeat(v)
                            .take(self.get_number_of_repetitions_at_position(col_idx, i))
                    },
                )))
            };
        }

        match col {
            ColumnWithIntervalsT::U32(c) => build_iter!(U32, c),
            ColumnWithIntervalsT::U64(c) => build_iter!(U64, c),
            ColumnWithIntervalsT::I64(c) => build_iter!(I64, c),
            ColumnWithIntervalsT::Float(c) => build_iter!(Float, c),
            ColumnWithIntervalsT::Double(c) => build_iter!(Double, c),
        }
    }

    /// Get Vector or column iterators that augment full table representation
    pub fn get_full_column_iterators(&self) -> Vec<StorageValueIteratorT> {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, _)| self.get_full_iterator_for_col_idx(i))
            .collect()
    }
}

impl ByteSized for Trie {
    fn size_bytes(&self) -> ByteSize {
        // TODO: Think about including TrieSchema here, but maybe it will move anyways
        ByteSize::b(size_of::<Self>() as u64)
            + self
                .columns
                .iter()
                .fold(ByteSize::b(0), |acc, column| acc + column.size_bytes())
    }
}

impl Table for Trie {
    fn from_cols(cols: Vec<VecT>) -> Self {
        debug_assert!({
            // assert that columns have the same length
            cols.get(0)
                .map(|col| {
                    let len = col.len();
                    cols.iter().all(|col| col.len() == len)
                })
                .unwrap_or(true)
        });

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(ColumnBuilderAdaptiveT::$variant(vec) => ColumnWithIntervalsT::$variant(
                        ColumnWithIntervals::new(
                            vec.finalize(),
                            $interval_builder.finalize(),
                        ),
                    )),+
                }
            }
        }

        // return empty trie, if no columns are supplied
        if cols.is_empty() {
            return Self::new(Vec::new());
        }

        // if the first col is empty, then all of them are; we return early in this case
        if cols[0].is_empty() {
            return Self::new(
                cols
                    .into_iter()
                    .map(|v| {
                        let empty_data_col = ColumnBuilderAdaptiveT::new(v.get_type(), Default::default(), Default::default());
                        let empty_interval_col = ColumnBuilderAdaptive::<usize>::default();
                        build_interval_column!(empty_data_col, empty_interval_col; U32; U64; I64; Float; Double)
                    })
                    .collect(),
            );
        }

        let column_len = cols[0].len();

        let permutator = Permutator::sort_from_multiple_vec(&cols)
            .expect("debug assert above ensures that cols have the same length");
        let sorted_cols: Vec<_> = cols
            .into_iter()
            .map(|col| {
                permutator
                    .permute_streaming(col)
                    .expect("length matches since permutator is constructed from these vectores")
            })
            .collect();

        // NOTE: we talk about "condensed" and "uncondensed" in the following
        // "uncondensed" refers to the input version of the column vectors (and their indices), i.e. they have the same length and no duplicates have been removed
        // "condensed" refers to the "cleaned up" version of the columns where duplicates have been removed (largely) and a trie structure is resembled
        // we name our variables accordingly to clarify which version a column vector or index corresponds to
        let mut last_uncondensed_interval_starts: Vec<usize> = vec![0];
        let mut condensed_data_builders: Vec<ColumnBuilderAdaptiveT> = vec![];
        let mut condensed_interval_starts_builders: Vec<ColumnBuilderAdaptive<usize>> = vec![];

        for mut sorted_col in sorted_cols.into_iter() {
            debug_assert_eq!(sorted_col.len(), column_len);
            let mut current_uncondensed_interval_starts = vec![0];
            let mut current_condensed_data: ColumnBuilderAdaptiveT = ColumnBuilderAdaptiveT::new(
                sorted_col.get_type(),
                Default::default(),
                Default::default(),
            );
            let mut current_condensed_interval_starts_builder: ColumnBuilderAdaptive<usize> =
                ColumnBuilderAdaptive::default();

            let mut uncondensed_interval_ends =
                last_uncondensed_interval_starts.iter().skip(1).copied();
            let mut uncondensed_interval_end =
                uncondensed_interval_ends.next().unwrap_or(column_len);

            let mut current_val = sorted_col
                .next()
                .expect("we return early if the first column (and thus all) are empty");
            current_condensed_data.add(current_val);
            current_condensed_interval_starts_builder.add(0);

            for (next_val, uncondensed_col_index) in sorted_col.zip(1..) {
                if next_val != current_val || uncondensed_col_index >= uncondensed_interval_end {
                    current_uncondensed_interval_starts.push(uncondensed_col_index);
                    current_val = next_val;
                    current_condensed_data.add(current_val);
                }

                // if the second condition above is true, we need to adjust additional intercal information
                if uncondensed_col_index >= uncondensed_interval_end {
                    current_condensed_interval_starts_builder
                        .add(current_condensed_data.count() - 1);
                    uncondensed_interval_end =
                        uncondensed_interval_ends.next().unwrap_or(column_len);
                }
            }

            last_uncondensed_interval_starts = current_uncondensed_interval_starts;
            condensed_data_builders.push(current_condensed_data);
            condensed_interval_starts_builders.push(current_condensed_interval_starts_builder);
        }

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(ColumnBuilderAdaptiveT::$variant(data_col) => ColumnWithIntervalsT::$variant(
                        ColumnWithIntervals::new(
                            data_col.finalize(),
                            $interval_builder.finalize(),
                        ),
                    )),+
                }
            }
        }

        Self::new(
            condensed_data_builders
                .into_iter()
                .zip(condensed_interval_starts_builders)
                .map(|(col, iv)| build_interval_column!(col, iv; U32; U64; I64; Float; Double))
                .collect(),
        )
    }

    fn from_rows(rows: &[Vec<StorageValueT>]) -> Self {
        debug_assert!(!rows.is_empty());

        let arity = rows[0].len();

        let mut cols: Vec<VecT> = (0..arity)
            .map(|i| VecT::new(rows[0][i].get_type()))
            .collect();

        for row in rows {
            for (i, element) in row.iter().enumerate() {
                cols[i].push(*element);
            }
        }

        Self::from_cols(cols)
    }

    fn row_num(&self) -> usize {
        self.columns.last().map_or(0, |c| c.len())
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.types
    }
}

impl FromIterator<ColumnWithIntervalsT> for Trie {
    fn from_iter<T: IntoIterator<Item = ColumnWithIntervalsT>>(iter: T) -> Self {
        let columns: Vec<ColumnWithIntervalsT> = iter.into_iter().collect();
        let types = columns.iter().map(|c| c.get_type()).collect();

        Self { columns, types }
    }
}

/// Implementation of [`PartialTrieScan`] for a [`Trie`].
#[derive(Debug)]
pub struct TrieScanGeneric<'a> {
    trie: &'a Trie,
    column_types: Vec<StorageTypeName>,
    layers: Vec<UnsafeCell<ColumnScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> TrieScanGeneric<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let column_types = trie.get_types().clone();
        Self::new_cast(trie, column_types)
    }

    /// Construct a new trie iterator but converts each column to the given types.
    pub fn new_cast(trie: &'a Trie, column_types: Vec<StorageTypeName>) -> Self {
        debug_assert!(trie.get_types().len() == column_types.len());
        log::trace!("TrieScanGeneric: casting to {:?}", column_types);

        let mut layers = Vec::<UnsafeCell<ColumnScanT<'a>>>::new();

        for (column_index, column_t) in trie.columns().iter().enumerate() {
            let src_column_type = trie.get_types()[column_index];
            let dst_column_type = column_types[column_index];

            if src_column_type == dst_column_type {
                layers.push(UnsafeCell::new(column_t.iter()));
            } else {
                macro_rules! add_layer_for_datatype {
                    ($src_name:ident, $dst_name:ident, $src_type:ty, $dst_type:ty) => {{
                        let reference_scan = if let ColumnScanT::$src_name(scan) = column_t.iter() {
                            scan
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($type));
                        };

                        let new_scan = ColumnScanT::$dst_name(ColumnScanCell::new(
                            ColumnScanEnum::ColumnScanCast(ColumnScanCastEnum::$src_name(
                                ColumnScanCast::<$src_type, $dst_type>::new(reference_scan),
                            )),
                        ));

                        layers.push(UnsafeCell::new(new_scan));
                    }};
                }

                generate_cast_statements!(add_layer_for_datatype; src_column_type, dst_column_type);
            }
        }

        Self {
            trie,
            column_types,
            layers,
            current_layer: None,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanGeneric<'a> {
    fn up(&mut self) {
        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));
    }

    fn down(&mut self) {
        match self.current_layer {
            None => {
                self.current_layer = Some(0);

                // This `reset` is necessary because of the following scenario:
                // Calling `up` at the first layer while the first layer still points to some position.
                // Going `down` from there without the `reset` would lead to the first scan
                // still pointing to the previous position instead of starting at `None` as expected.
                // This is not needed for the other path as calling `narrow` has a similar effect.
                self.layers[0].get_mut().reset();
            }
            Some(index) => {
                debug_assert!(
                    index < self.layers.len(),
                    "Called down while on the last layer"
                );

                let current_position = self.layers[index]
                    .get_mut()
                    .pos()
                    .expect("Going down is only allowed when on an element.");

                let next_index = index + 1;
                let next_layer_range = self
                    .trie
                    .get_column(next_index)
                    .int_bounds(current_position);

                self.layers[next_index].get_mut().narrow(next_layer_range);

                self.current_layer = Some(next_index);
            }
        }
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.layers[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.layers[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.column_types
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

#[cfg(test)]
mod test {
    use std::assert_eq;

    use super::{StorageValueIteratorT, Trie, TrieScanGeneric};
    use crate::columnar::traits::columnscan::ColumnScanT;
    use crate::datatypes::{storage_value::VecT, StorageValueT};
    use crate::tabular::traits::{partial_trie_scan::PartialTrieScan, table::Table};
    use crate::util::make_column_with_intervals_t;
    use test_log::test;

    #[test]
    /// Tests general functionality of trie, including:
    ///     * Construction
    ///     * Getting row number
    fn test_trie() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 2, 1], &[0, 2, 3, 4, 5]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);
        assert_eq!(trie.row_num(), 6);
    }

    /// helper methods that returns the following table as columns
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_cols() -> Vec<VecT> {
        vec![
            VecT::U64(vec![1, 1, 2, 1, 2]),
            VecT::U64(vec![3, 2, 3, 2, 6]),
            VecT::U64(vec![8, 7, 9, 8, 9]),
        ]
    }

    /// helper methods that returns the following table as rows
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_rows() -> Vec<Vec<StorageValueT>> {
        vec![
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(3),
                StorageValueT::U64(8),
            ],
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(2),
                StorageValueT::U64(7),
            ],
            vec![
                StorageValueT::U64(2),
                StorageValueT::U64(3),
                StorageValueT::U64(9),
            ],
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(2),
                StorageValueT::U64(8),
            ],
            vec![
                StorageValueT::U64(2),
                StorageValueT::U64(6),
                StorageValueT::U64(9),
            ],
        ]
    }

    /// helper methods that returns the following table as the expected trie
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_trie() -> Trie {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 3, 6], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 8, 8, 9, 9], &[0, 2, 3, 4]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        Trie::new(column_vec)
    }

    #[test]
    fn construct_trie_from_cols() {
        let cols = get_test_table_as_cols();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_cols(cols);

        assert_eq!(expected_trie, constructed_trie);
    }

    #[test]
    fn construct_trie_from_rows() {
        let rows = get_test_table_as_rows();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_rows(&rows);

        assert_eq!(expected_trie, constructed_trie);
    }

    fn scan_seek(int_scan: &mut TrieScanGeneric, value: u64) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.seek(value)
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_next(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_trie_iter() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 8, 7, 2, 1], &[0, 2, 5, 6, 7]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);
        let mut trie_iter = TrieScanGeneric::new(&trie);

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 3), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 6), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));
        assert!(scan_next(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 2), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());
    }

    #[test]
    fn get_number_of_repetitions_at_position() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 8, 7, 2, 1], &[0, 2, 5, 6, 7]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);

        assert_eq!(trie.get_number_of_repetitions_at_position(2, 0), 1);
        assert_eq!(trie.get_number_of_repetitions_at_position(2, 5), 1);
        assert_eq!(trie.get_number_of_repetitions_at_position(1, 0), 2);
        assert_eq!(trie.get_number_of_repetitions_at_position(1, 1), 3);
        assert_eq!(trie.get_number_of_repetitions_at_position(0, 0), 5);
    }

    fn collect_full_col_iterator(iter: StorageValueIteratorT) -> VecT {
        match iter {
            StorageValueIteratorT::U32(i) => VecT::U32(i.collect()),
            StorageValueIteratorT::U64(i) => VecT::U64(i.collect()),
            StorageValueIteratorT::I64(i) => VecT::I64(i.collect()),
            StorageValueIteratorT::Float(i) => VecT::Float(i.collect()),
            StorageValueIteratorT::Double(i) => VecT::Double(i.collect()),
        }
    }

    #[test]
    fn get_full_iterator_for_col_idx() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 8, 7, 2, 1], &[0, 2, 5, 6, 7]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);

        assert_eq!(
            collect_full_col_iterator(trie.get_full_iterator_for_col_idx(0)),
            VecT::U64(vec![1, 1, 1, 1, 1, 2, 3, 3])
        );
        assert_eq!(
            collect_full_col_iterator(trie.get_full_iterator_for_col_idx(1)),
            VecT::U64(vec![2, 2, 3, 3, 3, 4, 1, 2])
        );
        assert_eq!(
            collect_full_col_iterator(trie.get_full_iterator_for_col_idx(2)),
            VecT::U64(vec![3, 4, 5, 7, 8, 7, 2, 1])
        );
    }
}
