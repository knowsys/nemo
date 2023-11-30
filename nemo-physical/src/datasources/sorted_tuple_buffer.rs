//! Module that allows callers to write data into columns of a database table.

use crate::{datasources::TupleBuffer, datatypes::StorageValueT};

/// The [`SortedTableBuffer`] is a wrapper for [`TableBuffer`] that stores a tuple order.
#[derive(Debug)]
pub struct SortedTupleBuffer<'a> {
    tuple_buffer: TupleBuffer<'a>,
    tuple_order: Vec<usize>,
}

impl<'a> SortedTupleBuffer<'_> {
    /// Constructor for [`SortedTupleBuffer`]. It takes a [`TupleBuffer`] and a tuple_order as
    /// parameters. The tuple order indicates the indexes of the tuples ordered by increasing
    /// [`StorageValueT`]s.
    pub fn new<'b>(tuple_buffer: TupleBuffer<'b>, tuple_order: Vec<usize>) -> SortedTupleBuffer {
        SortedTupleBuffer {
            tuple_buffer,
            tuple_order,
        }
    }

    /// Returns the number of columns in the [`SortedTableBuffer`]
    pub fn column_number(&self) -> usize {
        self.tuple_buffer.column_number()
    }

    /// Returns the total number of tuples in the [`SortedTableBuffer`]
    pub fn size(&self) -> usize {
        self.tuple_buffer.size()
    }

    /// Returns an iterator of the sorted [`StorageValueT`] values of the n-th column in the [`SortedTableBuffer`]
    pub fn get_column<'b>(
        &'b self,
        column_idx: &'b usize,
    ) -> impl Iterator<Item = StorageValueT> + 'b {
        self.tuple_order.iter().map(|tuple_idx| {
            self.tuple_buffer
                .get_value(*tuple_idx, *column_idx)
                .unwrap()
        })
    }
}
