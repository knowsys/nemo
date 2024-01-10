//! This module defines a type of column,
//! whose data entries are divided into several intervals
//! or blocks of sorted values.
//!
//! Such columns represent a layer in a [Trie][crate::tabular::trie::Trie].

pub(crate) mod interval_lookup;

use std::ops::Range;

use bytesize::ByteSize;
use delegate::delegate;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnbuilder::{adaptive::ColumnBuilderAdaptive, ColumnBuilder},
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
    },
    datatypes::{ColumnDataType, Double, Float, StorageTypeName, StorageValueT},
    management::ByteSized,
};

use self::interval_lookup::{IntervalLookup, IntervalLookupBuilderT, IntervalLookupT};

#[derive(Debug, Clone)]
pub(crate) struct IntervalColumn<T, LookupMethod>
where
    T: ColumnDataType,
    LookupMethod: IntervalLookup,
{
    /// [Column][crate::columnar::column::Column]
    /// that stores the actual data
    data: ColumnEnum<T>,

    /// Associates each entry from the previous layer
    /// with a sorted interval of values in the `data` column of this layer
    interval_lookup: IntervalLookupT<LookupMethod>,
}

impl<T, LookupMethod> IntervalColumn<T, LookupMethod>
where
    T: ColumnDataType,
    LookupMethod: IntervalLookup,
{
    delegate! {
        to self.interval_lookup {
            pub fn interval_bounds(&self, storage_type: StorageTypeName, index: usize) -> Option<Range<usize>>;
        }
    }
}

impl<'a, T, LookupMethod> Column<'a, T> for IntervalColumn<T, LookupMethod>
where
    T: 'a + ColumnDataType,
    LookupMethod: IntervalLookup,
{
    type Scan = ColumnScanEnum<'a, T>;

    delegate! {
        to self.data {
            fn len(&self) -> usize;
            fn get(&self, index: usize) -> T ;
            fn iter(&'a self) -> Self::Scan;
        }
    }
}

impl<T, LookupMethod> ByteSized for IntervalColumn<T, LookupMethod>
where
    T: ColumnDataType,
    LookupMethod: IntervalLookup,
{
    fn size_bytes(&self) -> ByteSize {
        self.data.size_bytes() + self.interval_lookup.size_bytes()
    }
}

/// [IntervalColumn] for each [StorageTypeName]
#[derive(Debug, Clone)]
pub(crate) struct IntervalColumnT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Case [StorageTypeName::Id32]
    column_id32: IntervalColumn<u32, LookupMethod>,
    /// Case [StorageTypeName::Id64]
    column_id64: IntervalColumn<u64, LookupMethod>,
    /// Case [StorageTypeName::Int64]
    column_int64: IntervalColumn<i64, LookupMethod>,
    /// Case [StorageTypeName::Float]
    column_float: IntervalColumn<Float, LookupMethod>,
    /// Case [StorageTypeName::Double]
    column_double: IntervalColumn<Double, LookupMethod>,
}

impl<LookupMethod> IntervalColumnT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Create a new [IntervalColumnT].
    pub fn new(
        column_id32: IntervalColumn<u32, LookupMethod>,
        column_id64: IntervalColumn<u64, LookupMethod>,
        column_int64: IntervalColumn<i64, LookupMethod>,
        column_float: IntervalColumn<Float, LookupMethod>,
        column_double: IntervalColumn<Double, LookupMethod>,
    ) -> Self {
        Self {
            column_id32,
            column_id64,
            column_int64,
            column_float,
            column_double,
        }
    }

    /// Create a [ColumnScanRainbow] from iterators of the internal columns.
    pub fn iter(&self) -> ColumnScanRainbow {
        ColumnScanRainbow {
            scan_id32: ColumnScanCell::new(self.column_id32.iter()),
            scan_id64: ColumnScanCell::new(self.column_id64.iter()),
            scan_i64: ColumnScanCell::new(self.column_int64.iter()),
            scan_float: ColumnScanCell::new(self.column_float.iter()),
            scan_double: ColumnScanCell::new(self.column_double.iter()),
        }
    }

    pub fn interval_bounds(
        &self,
        previous_type: StorageTypeName,
        previous_index: usize,
        next_type: StorageTypeName,
    ) -> Option<Range<usize>> {
        match next_type {
            StorageTypeName::Id32 => self
                .column_id32
                .interval_bounds(previous_type, previous_index),
            StorageTypeName::Id64 => self
                .column_id64
                .interval_bounds(previous_type, previous_index),
            StorageTypeName::Int64 => self
                .column_int64
                .interval_bounds(previous_type, previous_index),
            StorageTypeName::Float => self
                .column_float
                .interval_bounds(previous_type, previous_index),
            StorageTypeName::Double => self
                .column_double
                .interval_bounds(previous_type, previous_index),
        }
    }

    /// Return the number of data entries in this column
    pub fn num_data(&self) -> usize {
        self.column_id32.data.len()
            + self.column_id64.data.len()
            + self.column_int64.data.len()
            + self.column_float.data.len()
            + self.column_double.data.len()
    }
}

impl<LookupMethod> ByteSized for IntervalColumnT<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    fn size_bytes(&self) -> ByteSize {
        self.column_id32.size_bytes()
            + self.column_id64.size_bytes()
            + self.column_int64.size_bytes()
            + self.column_float.size_bytes()
            + self.column_double.size_bytes()
    }
}

#[derive(Debug)]
struct IntervalColumnBuilder<T, LookupMethod>
where
    T: ColumnDataType,
    LookupMethod: IntervalLookup,
{
    data: ColumnBuilderAdaptive<T>,
    interval: IntervalLookupBuilderT<LookupMethod>,
}

impl<T, LookupMethod> Default for IntervalColumnBuilder<T, LookupMethod>
where
    T: ColumnDataType + Default,
    LookupMethod: IntervalLookup,
{
    fn default() -> Self {
        Self {
            data: Default::default(),
            interval: IntervalLookupBuilderT::<LookupMethod>::default(),
        }
    }
}

impl<T, LookupMethod> IntervalColumnBuilder<T, LookupMethod>
where
    T: ColumnDataType + Default,
    LookupMethod: IntervalLookup,
{
    pub fn add_data(&mut self, value: T) {
        self.data.add(value);
    }

    pub fn finish_interval(&mut self, storage_type: StorageTypeName) {
        self.interval.add(storage_type, self.data.count());
    }

    fn finalize(self) -> IntervalColumn<T, LookupMethod> {
        IntervalColumn {
            data: self.data.finalize(),
            interval_lookup: self.interval.finalize(),
        }
    }
}

/// Object for building an [IntervalColumnT]
/// based on receiving the table in matrix form
///
/// This is used for creating an [IntervalColumnT] from a [TupleBuffer][crate::datasources::TupleBuffer].
#[derive(Debug)]
pub(crate) struct IntervalColumnTBuilderMatrix<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Case [StorageTypeName::Id32]
    builder_id32: IntervalColumnBuilder<u32, LookupMethod>,
    /// Case [StorageTypeName::Id64]
    builder_id64: IntervalColumnBuilder<u64, LookupMethod>,
    /// Case [StorageTypeName::Int64]
    builder_int64: IntervalColumnBuilder<i64, LookupMethod>,
    /// Case [StorageTypeName::Float]
    builder_float: IntervalColumnBuilder<Float, LookupMethod>,
    /// Case [StorageTypeName::Double]
    builder_double: IntervalColumnBuilder<Double, LookupMethod>,

    /// Cached data item
    current_data_item: Option<StorageValueT>,
}

impl<LookupMethod> Default for IntervalColumnTBuilderMatrix<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    fn default() -> Self {
        Self {
            builder_id32: Default::default(),
            builder_id64: Default::default(),
            builder_int64: Default::default(),
            builder_float: Default::default(),
            builder_double: Default::default(),
            current_data_item: Default::default(),
        }
    }
}

impl<LookupMethod> IntervalColumnTBuilderMatrix<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Add currently cached value into the data column builder.
    pub fn commit_value(&mut self) {
        if let Some(value) = self.current_data_item {
            match value {
                StorageValueT::Id32(value) => self.builder_id32.add_data(value),
                StorageValueT::Id64(value) => self.builder_id64.add_data(value),
                StorageValueT::Int64(value) => self.builder_int64.add_data(value),
                StorageValueT::Float(value) => self.builder_float.add_data(value),
                StorageValueT::Double(value) => self.builder_double.add_data(value),
            }
        }
    }

    /// Add a new value into the builder.
    ///
    /// Returns `true` if the value is different from the last added value and `false` otherwise.
    pub fn add_value(&mut self, value: StorageValueT) -> bool {
        match self.current_data_item {
            None => {
                self.current_data_item = Some(value);
                true
            }
            Some(current_value) => {
                if current_value != value {
                    self.commit_value();
                    self.current_data_item = Some(value);

                    true
                } else {
                    false
                }
            }
        }
    }

    /// Signify to the builder that all values of the current interval have been added.
    pub fn finish_interval(&mut self, previous_type: StorageTypeName) {
        self.commit_value();

        self.builder_id32.finish_interval(previous_type);
        self.builder_id64.finish_interval(previous_type);
        self.builder_int64.finish_interval(previous_type);
        self.builder_float.finish_interval(previous_type);
        self.builder_double.finish_interval(previous_type);

        self.current_data_item = None;
    }

    /// Finish processing and return an [IntervalColumnT].
    pub fn finalize(self) -> IntervalColumnT<LookupMethod> {
        IntervalColumnT::new(
            self.builder_id32.finalize(),
            self.builder_id64.finalize(),
            self.builder_int64.finalize(),
            self.builder_float.finalize(),
            self.builder_double.finalize(),
        )
    }
}

/// Object for building an [IntervalColumnT]
/// based on receiving the table in form of a [TrieScan][crate::tabular::triescan]
#[derive(Debug)]
pub(crate) struct IntervalColumnTBuilderTriescan<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Case [StorageTypeName::Id32]
    builder_id32: IntervalColumnBuilder<u32, LookupMethod>,
    /// Case [StorageTypeName::Id64]
    builder_id64: IntervalColumnBuilder<u64, LookupMethod>,
    /// Case [StorageTypeName::Int64]
    builder_int64: IntervalColumnBuilder<i64, LookupMethod>,
    /// Case [StorageTypeName::Float]
    builder_float: IntervalColumnBuilder<Float, LookupMethod>,
    /// Case [StorageTypeName::Double]
    builder_double: IntervalColumnBuilder<Double, LookupMethod>,
}

impl<LookupMethod> Default for IntervalColumnTBuilderTriescan<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    fn default() -> Self {
        Self {
            builder_id32: Default::default(),
            builder_id64: Default::default(),
            builder_int64: Default::default(),
            builder_float: Default::default(),
            builder_double: Default::default(),
        }
    }
}

impl<LookupMethod> IntervalColumnTBuilderTriescan<LookupMethod>
where
    LookupMethod: IntervalLookup,
{
    /// Add a new value into the builder.
    pub fn add_value(&mut self, value: StorageValueT) {
        match value {
            StorageValueT::Id32(value) => self.builder_id32.add_data(value),
            StorageValueT::Id64(value) => self.builder_id64.add_data(value),
            StorageValueT::Int64(value) => self.builder_int64.add_data(value),
            StorageValueT::Float(value) => self.builder_float.add_data(value),
            StorageValueT::Double(value) => self.builder_double.add_data(value),
        }
    }

    /// Signify to the builder that all values of the current interval have been added.
    pub fn finish_interval(&mut self, previous_type: StorageTypeName) {
        self.builder_id32.finish_interval(previous_type);
        self.builder_id64.finish_interval(previous_type);
        self.builder_int64.finish_interval(previous_type);
        self.builder_float.finish_interval(previous_type);
        self.builder_double.finish_interval(previous_type);
    }

    /// Finish processing and return an [IntervalColumnT].
    pub fn finalize(self) -> IntervalColumnT<LookupMethod> {
        IntervalColumnT::new(
            self.builder_id32.finalize(),
            self.builder_id64.finalize(),
            self.builder_int64.finalize(),
            self.builder_float.finalize(),
            self.builder_double.finalize(),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        columnar::{column::Column, intervalcolumn::IntervalColumnTBuilderTriescan},
        datatypes::{Float, StorageTypeName, StorageValueT},
    };

    use super::{
        interval_lookup::{
            lookup_bitvector::IntervalLookupBitVector,
            lookup_column_dual::IntervalLookupColumnDual,
            lookup_column_single::IntervalLookupColumnSingle, IntervalLookup,
        },
        IntervalColumnTBuilderMatrix,
    };

    fn test_builder_matrix<LookupMethod: IntervalLookup>() {
        let mut builder = IntervalColumnTBuilderMatrix::<LookupMethod>::default();

        assert_eq!(builder.add_value(StorageValueT::Id32(12)), true);
        assert_eq!(builder.add_value(StorageValueT::Id32(16)), true);
        assert_eq!(builder.add_value(StorageValueT::Id32(16)), false);
        assert_eq!(builder.add_value(StorageValueT::Int64(-10)), true);
        assert_eq!(builder.add_value(StorageValueT::Int64(-4)), true);

        builder.finish_interval(StorageTypeName::Id64);

        assert_eq!(builder.add_value(StorageValueT::Int64(-4)), true);
        assert_eq!(builder.add_value(StorageValueT::Int64(-4)), false);
        assert_eq!(builder.add_value(StorageValueT::Int64(0)), true);
        assert_eq!(
            builder.add_value(StorageValueT::Float(Float::new(3.1).unwrap())),
            true
        );
        assert_eq!(
            builder.add_value(StorageValueT::Float(Float::new(3.1).unwrap())),
            false
        );

        builder.finish_interval(StorageTypeName::Double);

        assert_eq!(builder.add_value(StorageValueT::Id32(6)), true);
        assert_eq!(builder.add_value(StorageValueT::Id32(7)), true);

        builder.finish_interval(StorageTypeName::Double);

        let interval_column = builder.finalize();
        let column_id32 = interval_column
            .column_id32
            .data
            .iter()
            .collect::<Vec<u32>>();
        let column_int64 = interval_column
            .column_int64
            .data
            .iter()
            .collect::<Vec<i64>>();
        let column_float = interval_column
            .column_float
            .data
            .iter()
            .collect::<Vec<Float>>();

        assert_eq!(column_id32, vec![12, 16, 6, 7]);
        assert_eq!(column_int64, vec![-10, -4, -4, 0]);
        assert_eq!(column_float, vec![Float::new(3.1).unwrap()]);

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Id32),
            Some(0..2)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Int64),
            Some(0..2)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Float),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Double),
            None
        );

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Id32),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Int64),
            Some(2..4)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Float),
            Some(0..1)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Double),
            None
        );

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Id32),
            Some(2..4)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Int64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Float),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Double),
            None
        );
    }

    #[test]
    fn interval_builder_matrix() {
        test_builder_matrix::<IntervalLookupColumnSingle>();
        test_builder_matrix::<IntervalLookupColumnDual>();
        test_builder_matrix::<IntervalLookupBitVector>();
    }

    fn test_builder_trie<LookupMethod: IntervalLookup>() {
        let mut builder = IntervalColumnTBuilderTriescan::<LookupMethod>::default();

        builder.add_value(StorageValueT::Id32(12));
        builder.add_value(StorageValueT::Id32(16));
        builder.add_value(StorageValueT::Int64(-10));
        builder.add_value(StorageValueT::Int64(-4));

        builder.finish_interval(StorageTypeName::Id64);

        builder.add_value(StorageValueT::Int64(-4));
        builder.add_value(StorageValueT::Int64(0));
        builder.add_value(StorageValueT::Float(Float::new(3.1).unwrap()));

        builder.finish_interval(StorageTypeName::Double);

        builder.add_value(StorageValueT::Id32(6));
        builder.add_value(StorageValueT::Id32(7));

        builder.finish_interval(StorageTypeName::Double);

        let interval_column = builder.finalize();
        let column_id32 = interval_column
            .column_id32
            .data
            .iter()
            .collect::<Vec<u32>>();
        let column_int64 = interval_column
            .column_int64
            .data
            .iter()
            .collect::<Vec<i64>>();
        let column_float = interval_column
            .column_float
            .data
            .iter()
            .collect::<Vec<Float>>();

        assert_eq!(column_id32, vec![12, 16, 6, 7]);
        assert_eq!(column_int64, vec![-10, -4, -4, 0]);
        assert_eq!(column_float, vec![Float::new(3.1).unwrap()]);

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Id32),
            Some(0..2)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Int64),
            Some(0..2)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Float),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Id64, 0, StorageTypeName::Double),
            None
        );

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Id32),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Int64),
            Some(2..4)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Float),
            Some(0..1)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 0, StorageTypeName::Double),
            None
        );

        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Id32),
            Some(2..4)
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Id64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Int64),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Float),
            None
        );
        assert_eq!(
            interval_column.interval_bounds(StorageTypeName::Double, 1, StorageTypeName::Double),
            None
        );
    }

    #[test]
    fn interval_builder_trie() {
        test_builder_trie::<IntervalLookupColumnSingle>();
        test_builder_trie::<IntervalLookupColumnDual>();
        test_builder_trie::<IntervalLookupBitVector>();
    }
}
