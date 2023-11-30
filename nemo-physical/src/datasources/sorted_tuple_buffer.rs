//! Module that allows callers to write data into columns of a database table.

use crate::{
    datasources::TupleBuffer, datatypes::StorageValueT
};

/// The [`SortedTableBuffer`] has three main functions (1) to receive data from a reader, (2) to
/// sort the data once loading is completed, and (3) to provide access to its columns. One data
/// value can be added at a time. Data values are expected to be added in a row wise. Once a row is
/// complete, it is commited to the table. Buffered uncompleted rows can be discarded if necessary.
#[derive(Debug)]
pub struct SortedTupleBuffer<'a> {
    table_buffer: TupleBuffer<'a>,
    tuple_order: Vec<usize>,
}

impl<'a> SortedTupleBuffer<'_> {
    /// Constructor for [`SortedTupleBuffer`]. It takes a tuple buffer and a row order to access the data latter on
    pub fn new<'b>(table_buffer: TupleBuffer<'b>, tuple_order: Vec<usize>) -> SortedTupleBuffer {
        SortedTupleBuffer {
            table_buffer,
            tuple_order,
        }
    }

    /// Returns the number of columns in the [`SortedTableBuffer`]
    pub fn column_number(&self) -> usize {
        self.table_buffer.column_number()
    }

    /// Returns the total number of rows in the [`SortedTableBuffer`]
    pub fn size(&self) -> usize {
        self.table_buffer.size()
    }

    /// Returns an iterator of the sorted [`StorageValueT`] values of the n-th column in the [`SortedTableBuffer`]
    pub fn get_column<'b>(&'b self, column_idx: &'b usize) -> impl Iterator<Item = StorageValueT> + 'b {
        self.tuple_order.iter().map(|row_idx| self.table_buffer.get_value(*row_idx, *column_idx).unwrap())
    }

}
