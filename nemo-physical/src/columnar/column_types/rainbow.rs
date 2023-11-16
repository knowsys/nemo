use std::ops::Range;

use bytesize::ByteSize;

use crate::{
    columnar::{
        interval_lookup::one_column::IntervalLookupOneColumn,
        traits::{
            column::{Column, ColumnEnum},
            columnscan::ColumnScanEnum,
            interval::IntervalLookup,
        },
    },
    datatypes::{ColumnDataType, Double, StorageTypeName},
    management::ByteSized,
};

/// Note: This is a replacement of the ColumnWithInterval data structure
///
/// Subcolumn of a [`ColumnRainbow`] containing values of a particular type
#[derive(Debug, Clone)]
pub struct ColumnColor<T, Lookup>
where
    T: ColumnDataType,
    Lookup: IntervalLookup,
{
    /// Contains the actual data for this data type
    data: ColumnEnum<T>,

    /// Associates each value node in the previous layer
    /// with a sorted range of values in the `data` column of this layer
    interval_lookup: Lookup,
}

impl<T, Lookup> ColumnColor<T, Lookup>
where
    T: ColumnDataType,
    Lookup: IntervalLookup,
{
    /// Create a new [`ColumnColor`].
    pub fn new(data: ColumnEnum<T>, interval_lookup: Lookup) -> Self {
        Self {
            data,
            interval_lookup,
        }
    }

    /// Return the interval containing the successor of the node with the given index.
    ///
    /// Returns `None` if the node has no sucessor in this column.
    pub fn interval_bound(&self, index: usize) -> Option<Range<usize>> {
        self.interval_lookup.interval_bounds(index)
    }
}

impl<'a, T, Lookup> Column<'a, T> for ColumnColor<T, Lookup>
where
    T: 'a + ColumnDataType,
    Lookup: IntervalLookup,
{
    /// TODO: Reconsider this
    type Scan = ColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter(&'a self) -> Self::Scan {
        self.data.iter()
    }
}

impl<T, Lookup> ByteSized for ColumnColor<T, Lookup>
where
    T: ColumnDataType,
    Lookup: IntervalLookup,
{
    fn size_bytes(&self) -> ByteSize {
        self.data.size_bytes() + self.interval_lookup.size_bytes()
    }
}

/// Number of distinct data types withing a [`ColumnRainbow`]
const RAINBOX_NUM_COLORS: usize = 3;

/// TODO: Just use one of them or move to an dynamic approach
type LookupMethod = IntervalLookupOneColumn;

/// Column structure which can hold arbitrary data types.
///
/// It consists of one column for each storage type.
/// Hence, we distinguish between the local index of a value,
/// which is its index in the column that stores that value,
/// and its global index, which is the index of the value
/// if they were arranged sequentially given a certain ordering of types.
#[derive(Debug, Clone)]
pub struct ColumnRainbow {
    /// Column which contains keys which can be used within a dictionary to access the element
    column_keys: ColumnColor<u64, LookupMethod>,
    /// Column containing the integers
    column_integers: ColumnColor<i64, LookupMethod>,
    /// Column containing [`Double`]s
    column_doubles: ColumnColor<Double, LookupMethod>,

    /// Helper structure storing the starting global indices for values of each type.
    /// The last element in this array stores the number of data entries in this column.
    column_starts: [usize; RAINBOX_NUM_COLORS + 1],
}

///
#[derive(Debug)]
pub struct IntervalBounds {
    /// Bound for the keys column
    pub interval_keys: Option<Range<usize>>,
    /// Bound for the integer column
    pub interval_integers: Option<Range<usize>>,
    /// Bound for the [`Double`] column
    pub interval_doubles: Option<Range<usize>>,
}

impl ColumnRainbow {
    /// Internal ordering of types
    const ORDER: [StorageTypeName; RAINBOX_NUM_COLORS] = [
        StorageTypeName::U64,
        StorageTypeName::I64,
        StorageTypeName::Double,
    ];

    /// Construct a new [`ColumnRainbow`]
    pub fn new(
        column_keys: ColumnColor<u64, LookupMethod>,
        column_integers: ColumnColor<i64, LookupMethod>,
        column_doubles: ColumnColor<Double, LookupMethod>,
    ) -> Self {
        let column_starts = [
            0,
            column_keys.len(),
            column_keys.len() + column_integers.len(),
            column_keys.len() + column_integers.len() + column_doubles.len(),
        ];

        Self {
            column_keys,
            column_integers,
            column_doubles,
            column_starts,
        }
    }

    /// TODO: Description
    /// For a given [`StorageTypeName`] return ...
    ///
    /// # Panics
    /// Panics if the given [`StorageTypeName`] is not supported by this column.
    fn type_order(storage_type: StorageTypeName) -> usize {
        Self::ORDER
            .iter()
            .position(|&s| s == storage_type)
            .expect("Column does no store value of this type")
    }

    ///
    pub fn translate_index(&self, local_index: usize, storage_type: StorageTypeName) -> usize {
        self.column_starts[Self::type_order(storage_type)] + local_index
    }

    pub fn interval_bounds(&self, global_index: usize) -> IntervalBounds {
        IntervalBounds {
            interval_keys: self.column_keys.interval_bound(global_index),
            interval_integers: self.column_integers.interval_bound(global_index),
            interval_doubles: self.column_doubles.interval_bound(global_index),
        }
    }
}

/// Column functions
impl ColumnRainbow {
    /// Return the number of data elements contained in this column.
    pub fn len(&self) -> usize {
        self.column_starts[RAINBOX_NUM_COLORS]
    }

    /// Check whether column contains no data entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ByteSized for ColumnRainbow {
    fn size_bytes(&self) -> ByteSize {
        self.column_keys.size_bytes()
            + self.column_integers.size_bytes()
            + self.column_doubles.size_bytes()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        columnar::{column_types::vector::ColumnVector, traits::column::ColumnEnum},
        datatypes::{ColumnDataType, Double},
    };

    use super::{ColumnColor, ColumnRainbow};

    // fn create_empty_column_enum<T: ColumnDataType>() -> ColumnEnum<T> {
    //     ColumnEnum::ColumnVector(ColumnVector::new(vec![]))
    // }

    // fn create_simple_column_enum<T: ColumnDataType>(data: &[T]) -> ColumnEnum<T> {
    //     ColumnEnum::ColumnVector(ColumnVector::new(data.to_vec()))
    // }

    // fn create_example_column() -> ColumnRainbow {
    //     let column_keys = ColumnColor::new(
    //         create_simple_column_enum(&[2, 4, 5]),
    //         create_empty_column_enum(),
    //         create_empty_column_enum(),
    //     );
    //     let column_integers = ColumnColor::new(
    //         create_simple_column_enum(&[-3, 1, 3, 7]),
    //         create_empty_column_enum(),
    //         create_empty_column_enum(),
    //     );
    //     let column_doubles = ColumnColor::new(
    //         create_simple_column_enum(&[Double::new(-10.0).unwrap(), Double::new(10.0).unwrap()]),
    //         create_empty_column_enum(),
    //         create_empty_column_enum(),
    //     );

    //     ColumnRainbow::new(column_keys, column_integers, column_doubles)
    // }

    #[test]
    fn rainbow_column_access_elements() {}
}
