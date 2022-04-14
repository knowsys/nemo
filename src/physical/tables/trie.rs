use super::{Table, TableSchema};
use crate::physical::columns::IntervalColumnT;
use crate::physical::datatypes::DataTypeName;
use std::fmt::Debug;

/// Represents one attribute in [`TrieSchema`].
#[derive(Debug, Copy, Clone)]
pub struct TrieSchemaEntry {
    /// Label of the column
    pub label: usize,

    /// Datatype used in the column
    pub datatype: DataTypeName,
}

/// Schema for [`Trie`].
#[derive(Debug)]
pub struct TrieSchema {
    attributes: Vec<TrieSchemaEntry>,
}

impl TrieSchema {
    /// Contruct new schema from vector of entries.
    pub fn new(attributes: Vec<TrieSchemaEntry>) -> Self {
        Self { attributes }
    }
}

impl TableSchema for TrieSchema {
    fn arity(&self) -> usize {
        self.attributes.len()
    }

    fn get_type(&self, index: usize) -> DataTypeName {
        self.attributes[index].datatype
    }

    fn get_label(&self, index: usize) -> usize {
        self.attributes[index].label
    }

    fn find_index(&self, label: usize) -> Option<usize> {
        for (index, elem) in self.attributes.iter().enumerate() {
            if elem.label == label {
                return Some(index);
            }
        }

        None
    }
}

/// Implementation of a trie data structure.
/// The underlying data is oragnized in IntervalColumns.
#[derive(Debug)]
pub struct Trie {
    schema: TrieSchema,
    columns: Vec<IntervalColumnT>,
}

impl Trie {
    /// Construct a new Trie from a given schema and a vector of IntervalColumns.
    pub fn new(schema: TrieSchema, columns: Vec<IntervalColumnT>) -> Self {
        Self { schema, columns }
    }

    /// Return reference to all columns.
    pub fn columns(&self) -> &Vec<IntervalColumnT> {
        &self.columns
    }

    /// Return reference to a column given an index.
    ///
    /// # Panics
    /// Panics if index is out of range
    pub fn get_column(&self, index: usize) -> &IntervalColumnT {
        &self.columns[index]
    }
}

impl Table for Trie {
    fn row_num(&self) -> usize {
        self.columns.last().map_or(0, |c| c.len())
    }

    fn schema(&self) -> &dyn TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::columns::{GenericIntervalColumn, IntervalColumnT, VectorColumn};
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tables::table::Table;
    use test_log::test;

    fn make_gic(values: &[u64], ints: &[usize]) -> GenericIntervalColumn<u64> {
        GenericIntervalColumn::new(
            Box::new(VectorColumn::new(values.to_vec())),
            Box::new(VectorColumn::new(ints.to_vec())),
        )
    }

    fn make_gict(values: &[u64], ints: &[usize]) -> IntervalColumnT {
        IntervalColumnT::IntervalColumnU64(Box::new(make_gic(values, ints)))
    }

    #[test]
    fn test_row_num() {
        let column_fst = make_gict(&[1, 2, 3], &[0]);
        let column_snd = make_gict(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_gict(&[3, 4, 5, 7, 2, 1], &[0, 2, 3, 4, 5]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, column_vec);
        assert_eq!(trie.row_num(), 6);

        let empty_trie = Trie::new(TrieSchema::new(vec![]), vec![]);
        assert_eq!(empty_trie.row_num(), 0);
    }
}
