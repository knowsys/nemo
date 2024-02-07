use std::cell::UnsafeCell;
use std::cmp::Ordering;

use std::{debug_assert, iter};

use nemo_physical::columnar::column::Column;
use nemo_physical::columnar::columnbuilder::adaptive::ColumnBuilderAdaptive;
use nemo_physical::columnar::columnbuilder::ColumnBuilder;
use nemo_physical::columnar::columnscan::ColumnScan;
use nemo_physical::datatypes::storage_value::StorageValueIteratorT;
use nemo_physical::datatypes::{Double, Float, StorageTypeName, StorageValueT};
use nemo_physical::generate_datatype_forwarder;

use crate::old::builder::ColumnBuilderAdaptiveT;
use crate::old::interval::ColumnWithIntervals;
use crate::old::permutator::Permutator;

use super::column_scan::ColumnScanT;
use super::interval::ColumnWithIntervalsT;
use super::partial_trie_scan::{PartialTrieScan, TrieScanEnum};
use super::trie_scan_prune::{TrieScan, TrieScanPrune};

/// A row in a table
pub type TableRow = Vec<StorageValueT>;

/// Table that stores a relation.
pub trait Table: std::fmt::Debug {
    /// Build table from a list of columns.
    fn from_cols(cols: Vec<VecT>) -> Self;

    /// Build table from a list of rows.
    fn from_rows(rows: &[TableRow]) -> Self;

    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize;

    /// Returns the schema of the table.
    fn get_types(&self) -> &Vec<StorageTypeName>;

    /// Returns whether this table contains the given row.
    fn contains_row(&self, row: TableRow) -> bool;
}

/// Enum for vectors of different supported input types
#[derive(Debug, PartialEq, Eq)]
pub enum VecT {
    /// Case `Vec<u32>`
    Id32(Vec<u32>),
    /// Case `Vec<u64>`
    Id64(Vec<u64>),
    /// Case `Vec<i64>`
    Int64(Vec<i64>),
    /// Case `Vec<Float>`
    Float(Vec<Float>),
    /// Case `Vec<Double>`
    Double(Vec<Double>),
}

generate_datatype_forwarder!(forward_to_vec);

impl VecT {
    /// Creates a new empty VecT for the given StorageTypeName
    pub fn new(dtn: StorageTypeName) -> Self {
        match dtn {
            StorageTypeName::Id32 => Self::Id32(Vec::new()),
            StorageTypeName::Id64 => Self::Id64(Vec::new()),
            StorageTypeName::Int64 => Self::Int64(Vec::new()),
            StorageTypeName::Float => Self::Float(Vec::new()),
            StorageTypeName::Double => Self::Double(Vec::new()),
        }
    }

    /// Returns the type of the VecT as StorageTypeName
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::Id32(_) => StorageTypeName::Id32,
            Self::Id64(_) => StorageTypeName::Id64,
            Self::Int64(_) => StorageTypeName::Int64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }

    /// Removes the last element in the corresponding vector
    pub fn pop(&mut self) {
        forward_to_vec!(self, pop;)
    }

    /// Get the value at the given index as StorageValueT
    pub fn get(&self, index: usize) -> Option<StorageValueT> {
        match self {
            VecT::Id32(vec) => vec.get(index).copied().map(StorageValueT::Id32),
            VecT::Id64(vec) => vec.get(index).copied().map(StorageValueT::Id64),
            VecT::Int64(vec) => vec.get(index).copied().map(StorageValueT::Int64),
            VecT::Float(vec) => vec.get(index).copied().map(StorageValueT::Float),
            VecT::Double(vec) => vec.get(index).copied().map(StorageValueT::Double),
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [StorageValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: StorageValueT) {
        match self {
            VecT::Id32(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::U32 and StorageValueT::U32, but StorageValueT does not match",
                ))
            }
            VecT::Id64(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::U64 and StorageValueT::U64, but StorageValueT does not match",
                ))
            }
            VecT::Int64(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::I64 and StorageValueT::I64, but StorageValueT does not match",
                ))
            }
            VecT::Float(vec) => vec.push(value.try_into().expect(
                "expecting VecT::Float and StorageValueT::Float, but StorageValueT does not match",
            )),
            VecT::Double(vec) => vec.push(value.try_into().expect(
                "expecting VecT::Double and StorageValueT::Double, but StorageValueT does not match",
            )),
        };
    }

    /// Returns the lengths of the contained Vector
    pub fn len(&self) -> usize {
        forward_to_vec!(self, len)
    }

    /// Returns whether the vector is empty, or not
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compares two values at the given index-points with each other
    pub fn compare_idx(&self, idx_a: usize, idx_b: usize) -> Option<Ordering> {
        match self {
            VecT::Id32(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Id64(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Int64(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Float(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Double(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
        }
    }
}
impl Default for VecT {
    fn default() -> Self {
        Self::Id64(Vec::<u64>::default())
    }
}

/// Implementation of a trie data structure.
/// The underlying data is organized in IntervalColumns.
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
            StorageTypeName::Id32 => last_column_for_datatype!(Id32),
            StorageTypeName::Id64 => last_column_for_datatype!(Id64),
            StorageTypeName::Int64 => last_column_for_datatype!(Int64),
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
                StorageTypeName::Id32 => push_column_for_datatype!(Id32),
                StorageTypeName::Id64 => push_column_for_datatype!(Id64),
                StorageTypeName::Int64 => push_column_for_datatype!(Int64),
                StorageTypeName::Float => push_column_for_datatype!(Float),
                StorageTypeName::Double => push_column_for_datatype!(Double),
            };

            last_interval_lengths = current_interval_lengths;
        }

        result_columns.reverse();
        result_columns
    }

    /// Returns a [`TrieScan`] over this table.
    pub fn scan(&self) -> impl TrieScan + PartialTrieScan + '_ {
        TrieScanPrune::new(TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(self)))
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
            ColumnWithIntervalsT::Id32(c) => build_iter!(Id32, c),
            ColumnWithIntervalsT::Id64(c) => build_iter!(Id64, c),
            ColumnWithIntervalsT::Int64(c) => build_iter!(Int64, c),
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

impl Table for Trie {
    fn from_cols(cols: Vec<VecT>) -> Self {
        debug_assert!({
            // assert that columns have the same length
            cols.first()
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
                        build_interval_column!(empty_data_col, empty_interval_col; Id32; Id64; Int64; Float; Double)
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

                // if the second condition above is true, we need to adjust additional interval information
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
                .map(|(col, iv)| build_interval_column!(col, iv; Id32; Id64; Int64; Float; Double))
                .collect(),
        )
    }

    fn from_rows(rows: &[TableRow]) -> Self {
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

    fn contains_row(&self, row: TableRow) -> bool {
        debug_assert!(self.columns.len() == row.len());

        let mut trie_scan = self.scan();

        for entry in row {
            trie_scan.down();

            macro_rules! seek_for_datatype {
                ($variant:ident, $entry:ident) => {
                    if let ColumnScanT::$variant(column_scan) =
                        trie_scan.current_scan().expect("We called down")
                    {
                        if let Some(closest) = column_scan.seek($entry) {
                            if closest != $entry {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                };
            }

            match entry {
                StorageValueT::Id32(entry) => seek_for_datatype!(Id32, entry),
                StorageValueT::Id64(entry) => seek_for_datatype!(Id64, entry),
                StorageValueT::Int64(entry) => seek_for_datatype!(Int64, entry),
                StorageValueT::Float(entry) => seek_for_datatype!(Float, entry),
                StorageValueT::Double(entry) => seek_for_datatype!(Double, entry),
            }
        }

        true
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
                panic!("Not supported");
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
