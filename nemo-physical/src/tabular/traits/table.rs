use crate::datatypes::{storage_value::VecT, StorageTypeName, StorageValueT};
use std::fmt::Debug;

/// A row in a table
pub type TableRow = Vec<StorageValueT>;

/// Table that stores a relation.
pub trait Table: Debug {
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
