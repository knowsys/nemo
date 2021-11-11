use std::fmt::Debug;
use crate::physical::datatypes::DataTypeName;

/// Table that stores a relation.
pub trait Table: Debug {

    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize;

    /// Returns the number of colums in the table.
    fn col_num(&self) -> usize;

    /// Returns the datatype of the column at the given index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    fn get_col_type(&self, index: usize) -> DataTypeName;
}