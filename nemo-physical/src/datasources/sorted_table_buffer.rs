//! Module that allows callers to write data into columns of a database table.

use crate::{
    datasources::TableWriter, datatypes::StorageValueT, datavalues::AnyDataValue,
    management::database::Dict,
};
use std::cell::RefCell;

/// The [`ColumnWriter`] is used to send the data of a single table column to the database. The interface
/// allows values to be added one by one, and also provides some functionality for rolling back the last value,
/// which is convenient when writing whole tuples
#[derive(Debug)]
pub struct SortedTableBuffer<'a> {
    table_buffer: TableWriter<'a>,
    is_finalized: bool,
}

impl SortedTableBuffer<'_> {
    /// Construct a new [`SortedTableBuffer`].
    pub fn new(dict: &RefCell<Dict>, column_count: usize) -> SortedTableBuffer {
        SortedTableBuffer {
            table_buffer: TableWriter::new(dict, column_count),
            is_finalized: false,
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

    /// Add a single new value to the table. Values are expected to be added in a row based fashion.
    /// When the number of buffered added values are equal to the number of columns, i.e., when a
    /// row is filled, then the row is commited to the table. Alternatively, a partially built row
    /// can be abandonded by calling [`drop_current_row`](SortedTableBuffer::drop_current_row).
    pub fn add_value(&mut self, value: AnyDataValue) {
        if !self.is_finalized {
            self.table_buffer.next_value(value);
        }
    }

    /// Forget current buffered added values.
    pub fn drop_current_row(&mut self) {
        self.table_buffer.drop_current_row();
    }

    /// Indicates that the data loading process has ended. Calling this function will sort the data.
    /// New value additions will be ignored.
    pub fn finalize(&mut self) {
        self.is_finalized = true;
        self.table_buffer.sort();
    }

    /// Returns the sorted values ---on the [`SortedTableBuffer`] level--- of the n-th column in
    /// the [`SortedTableBuffer`] represented by an iterator of [`StorageValueT`]s.
    pub fn get_column<'a>(
        &'a self,
        column_idx: &'a usize,
    ) -> impl Iterator<Item = StorageValueT> + 'a {
        self.table_buffer.get_column(column_idx)
    }
}
