use super::TableSchema;
use crate::physical::datatypes::{data_value::VecT, DataValueT};
use std::fmt::Debug;

/// Table that stores a relation.
pub trait Table: Debug {
    /// Associated Table Schema
    type Schema: TableSchema;

    /// Build table from a list of columns.
    fn from_cols(schema: Self::Schema, cols: Vec<VecT>) -> Self;

    /// Build table from a list of rows.
    fn from_rows(schema: Self::Schema, rows: Vec<Vec<DataValueT>>) -> Self;

    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize;

    /// Returns the schema of the table.
    fn schema(&self) -> &Self::Schema;
}
