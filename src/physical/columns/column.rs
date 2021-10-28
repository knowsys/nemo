use std::fmt::Debug;

/// Column of ordered values.
pub trait Column<T>: Debug {
   
    /// Returns the number of entries in the column.
    fn len(&self) -> usize;

    /// Returns the value at the given index.
    /// 
    /// # Panics
    /// Panics if `index` is out of bounds.
    fn get(&self, index: usize) -> T;

}
