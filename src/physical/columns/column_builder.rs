use std::fmt::Debug;
use super::Column;

/// Type for constructing columns.
pub trait ColumnBuilder<T>: Debug {
    
    /// Adds a new value to the constructed vector.
    fn add(&mut self, value: T);

    /// Returns column that was built.
    fn finalize<'a>(self) -> Box<dyn Column<T> +'a> where T: 'a;

}