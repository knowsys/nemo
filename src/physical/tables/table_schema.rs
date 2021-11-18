use std::fmt::Debug;
use crate::physical::datatypes::DataTypeName;


/// Schema for a particular relation (table). Columns each have a datatype
/// and a (numeric) label.
pub trait TableSchema: Debug {
        /// Returns the number of colums in the table.
        fn arity(&self) -> usize;

        /// Returns the datatype of the column at the given index.
        ///
        /// # Panics
        /// Panics if `index` is out of bounds.
        fn get_type(&self, index: usize) -> DataTypeName;

        /// Returns the numeric label of the column at the given index.
        ///
        /// # Panics
        /// Panics if `index` is out of bounds.
        fn get_label(&self, index: usize) -> usize;

        /// Returns the index of the given label or None if the label does
        /// not occur in this schema.
        fn find_index(&self, label: usize) -> Option<usize>;

}