//! This module implements [Trie]
//! as well as its iterator [TrieScanGeneric].

use std::cell::UnsafeCell;

use streaming_iterator::StreamingIterator;

use crate::{
    columnar::{
        columnscan::ColumnScanT,
        intervalcolumn::{
            interval_lookup::lookup_column::IntervalLookupColumn, IntervalColumnT,
            IntervalColumnTBuilderMatrix, IntervalColumnTBuilderTriescan,
        },
    },
    datasources::tuple_writer::TupleWriter,
    datatypes::{
        storage_type_name::{StorageTypeBitSet, STORAFE_TYPES},
        StorageTypeName, StorageValueT,
    },
    management::bytesized::{sum_bytes, ByteSized},
    tabular::{buffer::tuple_buffer::TupleBuffer, rowscan::RowScan},
    util::bitset::BitSet,
};

use super::{
    buffer::sorted_tuple_buffer::SortedTupleBuffer,
    operations::{projectreorder::ProjectReordering, trim::TrieScanTrim},
    rowscan::Row,
    triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
};

/// Defines the lookup method used in [IntervalColumnT]
type IntervalLookupMethod = IntervalLookupColumn;

/// A tree like data structure for storing tabular data
///
/// A path in the tree from root to leaf corresponds
/// to a row in the represented table.
#[derive(Debug, Clone)]
pub struct Trie {
    /// Each [IntervalColumnT] represents one column in the table.
    /// We refer to each such column as a layer.
    columns: Vec<IntervalColumnT<IntervalLookupMethod>>,
    /// Only relevant for nullary tables,
    /// whether it contains an empty row
    empty_row: bool,
}

impl Trie {
    /// Return the arity, that is the number of columns, in this trie.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Return the number of rows contained in this trie.
    pub fn num_rows(&self) -> usize {
        if let Some(last_column) = self.columns.last() {
            last_column.num_data()
        } else {
            if self.empty_row {
                1
            } else {
                0
            }
        }
    }

    /// Return whether the trie is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns whether a column of a particular [StorageTypeName] contains no data values.
    pub(crate) fn is_empty_layer(&self, layer: usize, storage_type: StorageTypeName) -> bool {
        self.columns[layer].is_empty_typed(storage_type)
    }

    /// Return a [PartialTrieScan] over this trie.
    pub(crate) fn partial_iterator(&self) -> TrieScanGeneric<'_> {
        let column_scans = self
            .columns
            .iter()
            .map(|column| UnsafeCell::new(column.iter()))
            .collect::<Vec<_>>();

        TrieScanGeneric::new(self, column_scans)
    }

    /// Return a [TrieScan] over this trie.
    #[allow(dead_code)]
    pub(crate) fn full_iterator(&self) -> TrieScanTrim {
        TrieScanTrim::new(TrieScanEnum::Generic(self.partial_iterator()))
    }

    /// Return a row based iterator over this trie.
    pub(crate) fn row_iterator(&self) -> impl Iterator<Item = Vec<StorageValueT>> + '_ {
        RowScan::new(self.partial_iterator(), 0)
    }

    /// Return whether this [Trie] contains a given row of values.
    pub(crate) fn contains_row(&self, row: &[StorageValueT]) -> bool {
        if self.arity() != row.len() {
            return false;
        }

        let mut trie_scan = self.partial_iterator();

        for value in row.iter() {
            trie_scan.down(value.get_type());
            let column_scan = unsafe {
                &mut *trie_scan
                    .current_scan()
                    .expect("We called down above")
                    .get()
            };

            if let Some(found) = column_scan.seek(*value) {
                if found == *value {
                    continue;
                }
            }

            return false;
        }

        true
    }
}

impl Trie {
    /// Create a new empty [Trie].
    pub(crate) fn empty(num_columns: usize) -> Self {
        Self {
            columns: (0..num_columns)
                .map(|_| IntervalColumnTBuilderMatrix::<IntervalLookupMethod>::default().finalize())
                .collect(),
            empty_row: false,
        }
    }

    /// Create a table with arity zero
    pub(crate) fn zero_arity(empty_row: bool) -> Self {
        Self {
            columns: Vec::default(),
            empty_row,
        }
    }

    /// Create a new [Trie] from a [SortedTupleBuffer].
    pub(crate) fn from_tuple_buffer(buffer: SortedTupleBuffer) -> Self {
        let mut intervalcolumn_builders = (0..buffer.column_number())
            .map(|_| IntervalColumnTBuilderMatrix::<IntervalLookupMethod>::default())
            .collect::<Vec<_>>();

        let mut last_tuple_intervals = Vec::new();
        let mut last_tuple_types = Vec::new();

        for (column_index, current_builder) in intervalcolumn_builders.iter_mut().enumerate() {
            let last_column = column_index == buffer.column_number() - 1;

            let mut current_tuple_intervals = Vec::<usize>::new();
            let mut current_tuple_types = Vec::<StorageTypeName>::new();

            let mut predecessor_index = 0;
            let mut current_type = StorageTypeName::Id32; // Initial value chosen arbitratily

            for (tuple_index, value) in buffer.get_column(column_index).enumerate() {
                if last_tuple_intervals.get(predecessor_index) == Some(&tuple_index) {
                    let previous_type = last_tuple_types[predecessor_index];
                    current_builder.finish_interval(previous_type);

                    predecessor_index += 1;
                }

                let value_type = value.get_type();
                let new_value = current_builder.add_value(value);

                if new_value && tuple_index > 0 && !last_column {
                    current_tuple_intervals.push(tuple_index);
                    current_tuple_types.push(current_type);
                }

                current_type = value_type;
            }

            if !last_column {
                current_tuple_types.push(current_type);
            }

            if column_index > 0 {
                current_builder.finish_interval(
                    *last_tuple_types
                        .last()
                        .expect("The type of the last pushed value is always added to the vector"),
                );
            } else {
                current_builder.commit_value();
            }

            last_tuple_intervals = current_tuple_intervals;
            last_tuple_types = current_tuple_types;
        }

        Self {
            columns: intervalcolumn_builders
                .into_iter()
                .map(|builder| builder.finalize())
                .collect(),
            empty_row: false,
        }
    }

    /// Create a new [Trie] from a [TupleWriter].
    pub(crate) fn from_tuple_writer(writer: TupleWriter) -> Self {
        Self::from_tuple_buffer(writer.finalize())
    }

    /// Create a new [Trie] based on a [PartialTrieScan].
    ///
    /// In the last `cut_layers` layers, this function will only check for the existence of a value
    /// and will not materialize it fully.
    /// To keep all the values, set `cut_layers` to 0.
    ///
    /// Assumes that the given `trie_scan` is not initialized
    pub(crate) fn from_partial_trie_scan(trie_scan: TrieScanEnum<'_>, cut_layers: usize) -> Self {
        let num_columns = trie_scan.arity() - cut_layers;

        if num_columns == 0 {
            return Self::zero_arity(true);
        }

        if let TrieScanEnum::AggregateWrapper(wrapper) = trie_scan {
            // `wrapper` can not be iterated as a partial trie scan, only as a full trie scan
            let trie = Trie::from_full_trie_scan(wrapper.trie_scan, cut_layers);
            return trie;
        }

        let mut rowscan = RowScan::new(trie_scan, cut_layers);

        let mut intervalcolumn_builders =
            Vec::<IntervalColumnTBuilderTriescan<IntervalLookupMethod>>::with_capacity(num_columns);

        if let Some(Row {
            row: first_row,
            change: _,
        }) = StreamingIterator::next(&mut rowscan)
        {
            let mut last_type: Option<StorageTypeName> = None;
            for &current_value in first_row {
                let mut new_builder =
                    IntervalColumnTBuilderTriescan::<IntervalLookupMethod>::new(last_type);

                last_type = Some(current_value.get_type());
                new_builder.add_value(current_value);

                intervalcolumn_builders.push(new_builder);
            }
        } else {
            return Self::empty(num_columns);
        }

        while let Some(Row {
            row: current_row,
            change: changed_layers,
        }) = StreamingIterator::next(&mut rowscan)
        {
            let mut last_type = StorageTypeName::Id32;

            for ((layer, current_builder), current_value) in intervalcolumn_builders
                .iter_mut()
                .enumerate()
                .zip(current_row)
                .skip(*changed_layers)
            {
                let current_type = current_value.get_type();

                if layer != *changed_layers {
                    current_builder.finish_interval(last_type);
                }

                current_builder.add_value(*current_value);

                last_type = current_type;
            }
        }

        Self {
            columns: intervalcolumn_builders
                .into_iter()
                .map(|builder| builder.finalize())
                .collect(),
            empty_row: false,
        }
    }

    /// Create a new [Trie] based on an a [TrieScan].
    ///
    /// In the last `cut_layers` layers, this function will only check for the existence of a value
    /// and will not materialize it fully.
    /// To keep all the values, set `cut_layers` to 0.
    ///
    /// Assumes that the given `trie_scan` is not initialized
    #[allow(dead_code)]
    pub(crate) fn from_full_trie_scan<Scan: TrieScan>(
        mut trie_scan: Scan,
        cut_layers: usize,
    ) -> Self {
        let num_columns = trie_scan.num_columns() - cut_layers;

        if num_columns == 0 {
            return Self::zero_arity(true);
        }

        let mut intervalcolumn_builders =
            Vec::<IntervalColumnTBuilderTriescan<IntervalLookupMethod>>::with_capacity(num_columns);

        if let Some(first_layer) = trie_scan.advance_on_layer(num_columns - 1) {
            debug_assert!(first_layer == 0);

            let mut last_type: Option<StorageTypeName> = None;
            for layer in 0..num_columns {
                let mut new_builder =
                    IntervalColumnTBuilderTriescan::<IntervalLookupMethod>::new(last_type);
                let current_value = trie_scan.current_value(layer);

                last_type = Some(current_value.get_type());
                new_builder.add_value(current_value);

                intervalcolumn_builders.push(new_builder);
            }
        } else {
            return Self::empty(trie_scan.num_columns());
        }

        while let Some(changed_layer) = trie_scan.advance_on_layer(num_columns - 1) {
            let mut last_type = StorageTypeName::Id32;

            for (layer, current_builder) in intervalcolumn_builders
                .iter_mut()
                .enumerate()
                .skip(changed_layer)
            {
                let current_value = trie_scan.current_value(layer);
                let current_type = current_value.get_type();

                if layer != changed_layer {
                    current_builder.finish_interval(last_type);
                }

                current_builder.add_value(current_value);

                last_type = current_type;
            }
        }

        Self {
            columns: intervalcolumn_builders
                .into_iter()
                .map(|builder| builder.finalize())
                .collect(),
            empty_row: false,
        }
    }

    /// Create a new [Trie] from a simple row based representation of the table.
    ///
    /// This function assumes that every row has the same number of entries.
    #[cfg(test)]
    pub(crate) fn from_rows(rows: Vec<Vec<StorageValueT>>) -> Self {
        let column_number = if let Some(first_row) = rows.first() {
            first_row.len()
        } else {
            return Self::zero_arity(false);
        };

        if column_number == 0 {
            return Self::zero_arity(true);
        }

        let mut tuple_buffer = TupleBuffer::new(column_number);

        for row in rows {
            debug_assert!(row.len() == column_number);

            for value in row {
                tuple_buffer.add_tuple_value(value);
            }
        }

        Self::from_tuple_buffer(tuple_buffer.finalize())
    }

    /// Create a [Trie] based on a [PartialTrieScan]
    /// and list of tries that result from it by applying the given [ProjectReordering]
    ///
    /// The first entry in the returned tuple is [Trie] resulting from the [PartialTrieScan]
    /// while the sedond entry is the list of reordered [Trie]s.
    /// If `keep_original` is set to `false`, then the first entry will be `None`.
    /// If the reustling [Trie]s will be empty then each of the [Trie]s will be `None`.
    pub(crate) fn from_partial_trie_scan_dependents(
        trie_scan: TrieScanEnum<'_>,
        reorderings: Vec<ProjectReordering>,
        keep_original: bool,
    ) -> (Option<Self>, Vec<Option<Self>>) {
        let reorderings = reorderings
            .iter()
            .map(|reordering| reordering.as_vector())
            .collect::<Vec<_>>();

        let num_columns = trie_scan.arity();
        let cut_layers = 0; // TODO: Compute last used layer across all reorderings

        if num_columns == 0 {
            return (
                Some(Self::zero_arity(true)),
                vec![Some(Self::zero_arity(true)); reorderings.len()],
            );
        }

        let mut rowscan = RowScan::new(trie_scan, cut_layers);

        let mut intervalcolumn_builders =
            Vec::<IntervalColumnTBuilderTriescan<IntervalLookupMethod>>::with_capacity(num_columns);
        let mut tuple_buffers = reorderings
            .iter()
            .map(|reordering| TupleBuffer::new(reordering.len()))
            .collect::<Vec<_>>();

        if let Some(Row {
            row: first_row,
            change: _,
        }) = StreamingIterator::next(&mut rowscan)
        {
            if keep_original {
                let mut last_type: Option<StorageTypeName> = None;
                for &current_value in first_row {
                    let mut new_builder =
                        IntervalColumnTBuilderTriescan::<IntervalLookupMethod>::new(last_type);

                    last_type = Some(current_value.get_type());
                    new_builder.add_value(current_value);

                    intervalcolumn_builders.push(new_builder);
                }
            }
        } else {
            return (None, vec![None; num_columns]);
        }

        while let Some(Row {
            row: current_row,
            change: changed_layers,
        }) = StreamingIterator::next(&mut rowscan)
        {
            let mut last_type = StorageTypeName::Id32;

            for ((layer, current_builder), current_value) in intervalcolumn_builders
                .iter_mut()
                .enumerate()
                .skip(*changed_layers)
                .zip(current_row)
            {
                let current_type = current_value.get_type();

                if layer != *changed_layers {
                    current_builder.finish_interval(last_type);
                }

                current_builder.add_value(*current_value);

                last_type = current_type;
            }

            for (tuple_buffer, reordering) in tuple_buffers.iter_mut().zip(reorderings.iter()) {
                for &column_index in reordering {
                    tuple_buffer.add_tuple_value(current_row[column_index]);
                }
            }
        }

        let result_trie = if keep_original {
            Some(Self {
                columns: intervalcolumn_builders
                    .into_iter()
                    .map(|builder| builder.finalize())
                    .collect(),
                empty_row: false,
            })
        } else {
            None
        };
        let result_reordered = tuple_buffers
            .into_iter()
            .map(|buffer| Some(Self::from_tuple_buffer(buffer.finalize())))
            .collect::<Vec<_>>();

        (result_trie, result_reordered)
    }
}

impl ByteSized for Trie {
    fn size_bytes(&self) -> bytesize::ByteSize {
        sum_bytes(self.columns.iter().map(|column| column.size_bytes()))
    }
}

/// Implementation of [PartialTrieScan] for a [Trie]
#[derive(Debug)]
pub(crate) struct TrieScanGeneric<'a> {
    /// Underlying [Trie] over which we are iterating
    trie: &'a Trie,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,

    /// [ColumnScan] for each layer in the [PartialTrieScan]
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanGeneric<'a> {
    /// Construct a new [TrieScanGeneric].
    pub(crate) fn new(trie: &'a Trie, column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>) -> Self {
        Self {
            trie,
            path_types: Vec::with_capacity(column_scans.len()),
            column_scans,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanGeneric<'a> {
    fn up(&mut self) {
        debug_assert!(
            !self.path_types.is_empty(),
            "Attempted to go up in the starting position"
        );

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        match self.path_types.last() {
            None => {
                self.column_scans[0].get_mut().reset(next_type);
            }
            Some(&previous_type) => {
                let next_layer = self.path_types.len();
                let previous_layer = next_layer - 1;

                let current_index = self.column_scans[previous_layer]
                    .get_mut()
                    .pos(previous_type)
                    .expect(
                        "Calling TrieScanGeneric::down is only allowed when currently pointing at an element.",
                    );

                let next_interval = self.trie.columns[next_layer]
                    .interval_bounds(previous_type, current_index, next_type)
                    .unwrap_or(0..0);

                self.column_scans[next_layer]
                    .get_mut()
                    .narrow(next_type, next_interval);
            }
        }

        self.path_types.push(next_type);
    }

    fn arity(&self) -> usize {
        self.trie.arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        &self.column_scans[layer]
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        let mut result = BitSet::default();

        for (index, storage_type) in STORAFE_TYPES.iter().enumerate() {
            if !self.trie.is_empty_layer(layer, *storage_type) {
                result.set(index, true);
            }
        }

        StorageTypeBitSet::from(result)
    }

    fn current_layer(&self) -> Option<usize> {
        self.path_types.len().checked_sub(1)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datatypes::{Float, StorageTypeName, StorageValueT},
        tabular::triescan::{PartialTrieScan, TrieScanEnum},
    };

    use super::{Trie, TrieScanGeneric};

    fn current_layer_next(
        scan: &mut TrieScanGeneric,
        storage_type: StorageTypeName,
    ) -> Option<StorageValueT> {
        let current_layer_scan = unsafe { &mut *scan.current_scan()?.get() };
        current_layer_scan.next(storage_type)
    }

    #[test]
    fn generic_trie_scan() {
        let table = vec![
            vec![
                StorageValueT::Id32(0),
                StorageValueT::Int64(-2),
                StorageValueT::Float(Float::new(1.2).unwrap()),
            ],
            vec![
                StorageValueT::Id32(0),
                StorageValueT::Int64(-1),
                StorageValueT::Id32(20),
            ],
            vec![
                StorageValueT::Id32(0),
                StorageValueT::Int64(-1),
                StorageValueT::Id32(32),
            ],
            vec![
                StorageValueT::Int64(6),
                StorageValueT::Id32(100),
                StorageValueT::Id32(101),
            ],
            vec![
                StorageValueT::Int64(6),
                StorageValueT::Id32(100),
                StorageValueT::Id32(102),
            ],
        ];

        let trie = Trie::from_rows(table);
        let mut scan = trie.partial_iterator();

        scan.down(StorageTypeName::Id32);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(0))
        );
        scan.down(StorageTypeName::Id32);
        assert_eq!(current_layer_next(&mut scan, StorageTypeName::Id32), None);
        scan.up();
        scan.down(StorageTypeName::Id64);
        assert_eq!(current_layer_next(&mut scan, StorageTypeName::Id32), None);
        scan.up();
        scan.down(StorageTypeName::Int64);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Int64),
            Some(StorageValueT::Int64(-2))
        );
        scan.down(StorageTypeName::Float);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Float),
            Some(StorageValueT::Float(Float::new(1.2).unwrap()))
        );
        scan.up();
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Int64),
            Some(StorageValueT::Int64(-1))
        );
        scan.down(StorageTypeName::Id32);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(20))
        );
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(32))
        );
        scan.up();
        scan.up();
        scan.up();
        scan.down(StorageTypeName::Int64);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Int64),
            Some(StorageValueT::Int64(6))
        );
        scan.down(StorageTypeName::Id32);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(100))
        );
        scan.down(StorageTypeName::Id32);
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(101))
        );
        assert_eq!(
            current_layer_next(&mut scan, StorageTypeName::Id32),
            Some(StorageValueT::Id32(102))
        );
    }

    #[test]
    fn trie_row_iterator() {
        let rows = vec![
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Id32(0),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-3),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Int64(-10),
                StorageValueT::Id32(2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Int64(-10),
                StorageValueT::Int64(-1),
            ],
            vec![
                StorageValueT::Int64(-7),
                StorageValueT::Id32(5),
                StorageValueT::Id32(6),
            ],
            vec![
                StorageValueT::Int64(-7),
                StorageValueT::Int64(-5),
                StorageValueT::Int64(-6),
            ],
        ];
        let trie = Trie::from_rows(rows.clone());

        let trie_rows = trie.row_iterator().collect::<Vec<_>>();

        assert_eq!(rows, trie_rows);
    }

    #[test]
    fn from_trie_scan() {
        let rows = vec![
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Id32(0),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-3),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Int64(-10),
                StorageValueT::Id32(2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Int64(-10),
                StorageValueT::Int64(-1),
            ],
            vec![
                StorageValueT::Int64(-7),
                StorageValueT::Id32(5),
                StorageValueT::Id32(6),
            ],
            vec![
                StorageValueT::Int64(-7),
                StorageValueT::Int64(-5),
                StorageValueT::Int64(-6),
            ],
        ];
        let trie = Trie::from_rows(rows.clone());

        let scan = trie.full_iterator();
        let trie_roundtrip = Trie::from_full_trie_scan(scan, 0);
        let trie_rows = trie_roundtrip.row_iterator().collect::<Vec<_>>();

        assert_eq!(rows, trie_rows);

        let scan = TrieScanEnum::Generic(trie.partial_iterator());
        let trie_roundtrip = Trie::from_partial_trie_scan(scan, 0);
        let trie_rows = trie_roundtrip.row_iterator().collect::<Vec<_>>();

        assert_eq!(rows, trie_rows);
    }

    #[test]
    fn trie_contains_row() {
        let rows = vec![
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Id32(0),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-3),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Id32(5),
                StorageValueT::Int64(-2),
            ],
            vec![
                StorageValueT::Id32(1),
                StorageValueT::Int64(-10),
                StorageValueT::Id32(2),
            ],
        ];
        let trie = Trie::from_rows(rows.clone());

        for row in rows.iter() {
            assert!(trie.contains_row(row));
        }

        assert!(!trie.contains_row(&[
            StorageValueT::Id32(1),
            StorageValueT::Int64(-12),
            StorageValueT::Id32(2),
        ]));
        assert!(!trie.contains_row(&[StorageValueT::Id32(1)]));
    }
}
