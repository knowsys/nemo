use crate::physical::datatypes::{data_value::VecT, DataValueT};
use std::fmt::Debug;

use super::table_schema::TableColumnTypes;

/// Table that stores a relation.
pub trait Table: Debug {
    /// Build table from a list of columns.
    fn from_cols(cols: Vec<VecT>) -> Self;

    /// Build table from a list of rows.
    fn from_rows(rows: &[Vec<DataValueT>]) -> Self;

    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize;

    /// Returns the schema of the table.
    fn get_types(&self) -> &TableColumnTypes;
}
