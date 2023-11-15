use std::ops::{Index, Range};

use bytesize::ByteSize;

use crate::{
    columnar::traits::{
        column::{Column, ColumnEnum},
        columnscan::ColumnScanEnum,
    },
    datatypes::{ColumnDataType, Double, StorageTypeName, StorageValueT},
    management::ByteSized,
};

/// Value encoding that a an entry in [`ColumnColor`] has no predecessor
pub const NO_PREDECESSOR: usize = usize::MAX;

/// Subcolumn of a [`ColumnRainbow`] containing values of a particular type
#[derive(Debug, Clone)]
pub struct ColumnColor<T>
where
    T: ColumnDataType,
{
    /// Contains the actual data for this data type
    data: ColumnEnum<T>,
    /// Marks the indices of sorted blocks of data within the data column
    ///
    /// The length of a block can be obtained
    /// by subtracting the start index of the next block from the start index of the current block
    blocks: ColumnEnum<usize>,
    /// Associates the global index form the previous column with a block index from this column
    predecessors: ColumnEnum<usize>,
}

impl<T> ColumnColor<T>
where
    T: ColumnDataType,
{
    /// Create a new [`ColumnColor`].
    pub fn new(
        data: ColumnEnum<T>,
        blocks: ColumnEnum<usize>,
        predecessors: ColumnEnum<usize>,
    ) -> Self {
        Self {
            data,
            blocks,
            predecessors,
        }
    }

    /// Given the (global) index of the predecessor node,
    /// return the range of indices of the successor nodes stored in the data column.
    ///
    /// Returns `None` if the node associated with the given index has no successor
    /// in this column.
    pub fn block_bounds(&self, global_index: usize) -> Option<Range<usize>> {
        let block_index = self.predecessors.get(global_index);

        if block_index == NO_PREDECESSOR {
            return None;
        }

        let block_start = self.blocks.get(block_index);
        let block_end = if block_index + 1 < self.blocks.len() {
            self.blocks.get(block_index + 1)
        } else {
            self.data.len()
        };

        Some(block_start..block_end)
    }
}

impl<'a, T> Column<'a, T> for ColumnColor<T>
where
    T: 'a + ColumnDataType,
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

impl<T> ByteSized for ColumnColor<T>
where
    T: ColumnDataType,
{
    fn size_bytes(&self) -> ByteSize {
        self.data.size_bytes() + self.blocks.size_bytes() + self.predecessors.size_bytes()
    }
}

/// Number of distinct data types withing a [`ColumnRainbow`]
const RAINBOX_NUM_COLORS: usize = 3;

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
    column_keys: ColumnColor<u64>,
    /// Column containing the integers
    column_integers: ColumnColor<i64>,
    /// Column containing [`Double`]s
    column_doubles: ColumnColor<Double>,

    /// Helper structure storing the starting global indices for values of each type.
    /// The last element in this array stores the number of data entries in this column.
    column_starts: [usize; RAINBOX_NUM_COLORS + 1],
}

///
#[derive(Debug)]
pub struct BlockBounds {
    /// Bound for the keys column
    pub bound_keys: Option<Range<usize>>,
    /// Bound for the integer column
    pub bound_integers: Option<Range<usize>>,
    /// Bound for the [`Double`] column
    pub bound_doubles: Option<Range<usize>>,
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
        column_keys: ColumnColor<u64>,
        column_integers: ColumnColor<i64>,
        column_doubles: ColumnColor<Double>,
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

    ///
    pub fn block_bounds(&self, global_index: usize) -> BlockBounds {
        BlockBounds {
            bound_keys: self.column_keys.block_bounds(global_index),
            bound_integers: self.column_integers.block_bounds(global_index),
            bound_doubles: self.column_doubles.block_bounds(global_index),
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

// impl<'a> Column<'a, StorageValueT> for ColumnRainbow {
//     type Scan = ColumnScanEnum<StorageValueT>;

//     fn len(&self) -> usize {
//         todo!()
//     }

//     fn get(&self, index: usize) -> StorageValueT {
//         todo!()
//     }

//     fn iter(&'a self) -> Self::Scan {
//         todo!()
//     }

//     fn is_empty(&self) -> bool {
//         self.len() == 0
//     }
// }

// impl<'a> Column<'a, u64> for ColumnRainbow {
//     type Scan = ColumnScanEnum<'a, u64>;

//     fn len(&self) -> usize {
//         self.column_starts[RAINBOX_NUM_COLORS]
//     }

//     fn get(&self, index: usize) -> u64 {
//         todo!()
//     }

//     fn iter(&'_ self) -> Self::Scan {
//         todo!()
//     }
// }

// impl<'a> Column<'a, i64> for ColumnRainbow {
//     type Scan = ColumnScanEnum<'a, i64>;

//     fn len(&self) -> usize {
//         todo!()
//     }

//     fn get(&self, index: usize) -> i64 {
//         todo!()
//     }

//     fn iter(&'_ self) -> Self::Scan {
//         todo!()
//     }
// }

#[cfg(test)]
mod test {
    use crate::{
        columnar::{column_types::vector::ColumnVector, traits::column::ColumnEnum},
        datatypes::{ColumnDataType, Double},
    };

    use super::{ColumnColor, ColumnRainbow};

    fn create_empty_column_enum<T: ColumnDataType>() -> ColumnEnum<T> {
        ColumnEnum::ColumnVector(ColumnVector::new(vec![]))
    }

    fn create_simple_column_enum<T: ColumnDataType>(data: &[T]) -> ColumnEnum<T> {
        ColumnEnum::ColumnVector(ColumnVector::new(data.to_vec()))
    }

    fn create_example_column() -> ColumnRainbow {
        let column_keys = ColumnColor::new(
            create_simple_column_enum(&[2, 4, 5]),
            create_empty_column_enum(),
            create_empty_column_enum(),
        );
        let column_integers = ColumnColor::new(
            create_simple_column_enum(&[-3, 1, 3, 7]),
            create_empty_column_enum(),
            create_empty_column_enum(),
        );
        let column_doubles = ColumnColor::new(
            create_simple_column_enum(&[Double::new(-10.0).unwrap(), Double::new(10.0).unwrap()]),
            create_empty_column_enum(),
            create_empty_column_enum(),
        );

        ColumnRainbow::new(column_keys, column_integers, column_doubles)
    }

    #[test]
    fn rainbow_column_access_elements() {}
}
