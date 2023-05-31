use super::super::traits::{table::Table, table_schema::TableSchema};
use crate::columnar::column_types::interval::ColumnWithIntervalsT;
use crate::datatypes::StorageTypeName;
use std::fmt::Debug;

/// Representation of one attribute/tree node in an [`FTableSchema`]
#[derive(Debug)]
struct FTableSchemaEntry {
    label: usize,
    datatype: StorageTypeName,
    parent: usize,
}

/// Schema for a factorized relation (table), which is a [`TableSchema`] that organizes
/// attributes in a tree structure.
#[derive(Debug)]
pub struct FTableSchema {
    attributes: Vec<FTableSchemaEntry>,
}

impl Default for FTableSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl FTableSchema {
    /// Creates a new empty instance of this struct.
    pub fn new() -> FTableSchema {
        FTableSchema {
            attributes: Vec::new(),
        }
    }

    /// Adds a new entry with the given values to the current entries.
    /// The parent will be set if a node with the given label exists; otherwise the new node becomes a root.
    pub fn add_entry(&mut self, label: usize, datatype: StorageTypeName, parent_label: usize) {
        let pidx = self.find_index(parent_label);
        if let Some(idx) = pidx {
            self.attributes.push(FTableSchemaEntry {
                label,
                datatype,
                parent: idx,
            });
        } else {
            self.attributes.push(FTableSchemaEntry {
                label,
                datatype,
                parent: usize::MAX,
            });
        }
    }

    /// Returns the index of the parent node of the given node, if it exists.
    pub fn get_parent(&self, index: usize) -> Option<usize> {
        let pidx = self.attributes[index].parent;
        (pidx != usize::MAX).then(|| pidx)
    }

    /// Returns the label of the parent node of the given node, or [`usize::MAX`] if the node is a root.
    pub fn get_parent_label(&self, label: usize) -> Option<usize> {
        let idx = self.find_index(label);
        idx.and_then(|cidx| self.get_parent(cidx))
            .map(|pidx| self.attributes[pidx].label)
    }

    /// Returns true if the column with index `down_idx` is either equal to or a successor of the column
    /// with the index `up_idx`.
    pub fn is_below(&self, down_idx: usize, up_idx: usize) -> bool {
        let mut cur_idx = down_idx;
        while cur_idx != usize::MAX {
            if cur_idx == up_idx {
                return true;
            }
            cur_idx = self.attributes[cur_idx].parent;
        }
        false
    }
}

impl TableSchema for FTableSchema {
    fn arity(&self) -> usize {
        self.attributes.len()
    }

    fn get_type(&self, index: usize) -> StorageTypeName {
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

/// Implementation of a factorized trie, which might be a subtrie of a larger strcuture.
#[derive(Debug)]
pub struct Ftrie {
    schema: FTableSchema,
    columns: Vec<ColumnWithIntervalsT>,
}

impl Ftrie {
    /// Constructs a new fTrie without parent or children.
    pub fn new(schema: FTableSchema, columns: Vec<ColumnWithIntervalsT>) -> Ftrie {
        Ftrie { schema, columns }
    }

    /// Counts the total number of rows in a given interval of a certain column,
    /// as it would appear if we would serialise all child nodes recursively into
    /// a flat table. The column index ['usize::MAX'] can be used for starting with
    /// root columns.
    ///
    /// # Panics
    /// Panics if `col_idx` or `int_idx` are out of bounds.
    fn count_rows(&self, col_idx: usize, int_idx: usize) -> usize {
        let mut children: Vec<usize> = Vec::new();
        for idx in 0..self.schema.arity() {
            if self.schema.get_parent(idx).unwrap_or(usize::MAX) == col_idx {
                children.push(idx);
            }
        }

        let bounds = if col_idx == usize::MAX {
            0..1
        } else {
            self.columns[col_idx].int_bounds(int_idx)
        };

        if children.is_empty() {
            return bounds.end - bounds.start;
        }

        let mut sum = 0;
        for i in bounds {
            let mut product = 1;
            for cidx in children.iter() {
                product *= self.count_rows(*cidx, i);
            }
            sum += product;
        }
        sum
    }

    // TODO: more funcionality needed
}

impl Table for Ftrie {
    /// Returns the number of rows in the table.
    fn row_num(&self) -> usize {
        self.count_rows(usize::MAX, 0)
    }

    /// Returns the schema of the table.
    fn schema(&self) -> &dyn TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::super::traits::{table::Table, table_schema::TableSchema};
    use super::{FTableSchema, Ftrie};
    use crate::datatypes::StorageTypeName;
    use crate::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    #[test]
    fn test_count_rows_linear() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, StorageTypeName::U64, 0);
        fts.add_entry(11, StorageTypeName::U64, 1);
        fts.add_entry(111, StorageTypeName::U64, 11);

        let gic1 = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let gic11 = make_column_with_intervals_t(&[11, 21, 31], &[0, 1, 2]);
        let gic111 = make_column_with_intervals_t(&[11, 21, 31], &[0, 1, 2]);

        let columns = vec![gic1, gic11, gic111];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 3);
    }

    #[test]
    fn test_count_rows() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, StorageTypeName::U64, 0);
        fts.add_entry(11, StorageTypeName::U64, 1);
        fts.add_entry(12, StorageTypeName::U64, 1);
        fts.add_entry(121, StorageTypeName::U64, 12);

        let gic1 = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let gic11 = make_column_with_intervals_t(&[11, 21, 22, 31], &[0, 1, 3]);
        let gic12 = make_column_with_intervals_t(&[11, 21, 31], &[0, 1, 2]);
        let gic121 = make_column_with_intervals_t(&[11, 12, 21, 22, 23, 31], &[0, 2, 5]);

        let columns = vec![gic1, gic11, gic12, gic121];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 9);
    }

    #[test]
    fn test_count_rows_cartesian() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, StorageTypeName::U64, 0);
        fts.add_entry(2, StorageTypeName::U64, 0);
        fts.add_entry(11, StorageTypeName::U64, 1);
        fts.add_entry(21, StorageTypeName::U64, 2);

        let gic1 = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let gic2 = make_column_with_intervals_t(&[1, 2], &[0]);
        let gic11 = make_column_with_intervals_t(&[11, 21, 31], &[0, 1, 2]);
        let gic21 = make_column_with_intervals_t(&[11, 12, 21, 22], &[0, 2]);

        let columns = vec![gic1, gic2, gic11, gic21];

        let ftrie = Ftrie::new(fts, columns);

        assert_eq!(ftrie.row_num(), 12);
    }

    #[test]
    fn test() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, StorageTypeName::U64, 0);
        fts.add_entry(11, StorageTypeName::U64, 1);
        fts.add_entry(12, StorageTypeName::Double, 1);
        fts.add_entry(111, StorageTypeName::Float, 11);
        fts.add_entry(1111, StorageTypeName::U64, 111);
        fts.add_entry(1112, StorageTypeName::U64, 111);

        assert_eq!(fts.arity(), 6);
        assert_eq!(fts.get_type(0), StorageTypeName::U64);
        assert_eq!(fts.get_label(0), 1);
        assert_eq!(fts.get_parent(0), None);
        assert_eq!(fts.get_type(2), StorageTypeName::Double);
        assert_eq!(fts.get_label(2), 12);
        assert_eq!(fts.get_parent(2), Some(0));

        assert_eq!(fts.get_parent_label(111), Some(11));
        assert_eq!(fts.get_parent_label(1), None);

        assert!(fts.is_below(1, 1));
        assert!(fts.is_below(2, 0));
        assert!(!fts.is_below(2, 1));
        assert!(fts.is_below(5, 1));
    }
}
