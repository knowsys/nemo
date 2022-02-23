use super::TableSchema;
use crate::physical::datatypes::DataTypeName;
use std::fmt::Debug;

/// Representation of one attribute/tree node in an [`FTableSchema`]
#[derive(Debug)]
struct FTableSchemaEntry {
    label: usize,
    datatype: DataTypeName,
    parent: usize,
}

/// Schema for a factorized relation (table), which is a [`TableSchema`] that organizes
/// attributes in a tree structure.
#[derive(Debug)]
pub struct FTableSchema {
    attributes: Vec<FTableSchemaEntry>,
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
    pub fn add_entry(&mut self, label: usize, datatype: DataTypeName, parent_label: usize) {
        let pidx = self.find_index(parent_label);
        if pidx.is_some() {
            self.attributes.push(FTableSchemaEntry {
                label,
                datatype,
                parent: pidx.unwrap(),
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

#[cfg(test)]
mod test {
    use super::{FTableSchema, TableSchema};
    use crate::physical::datatypes::DataTypeName;

    #[test]
    fn test() {
        let mut fts = FTableSchema::new();
        fts.add_entry(1, DataTypeName::U64, 0);
        fts.add_entry(11, DataTypeName::U64, 1);
        fts.add_entry(12, DataTypeName::Double, 1);
        fts.add_entry(111, DataTypeName::Float, 11);
        fts.add_entry(1111, DataTypeName::U64, 111);
        fts.add_entry(1112, DataTypeName::U64, 111);

        assert_eq!(fts.arity(), 6);
        assert_eq!(fts.get_type(0), DataTypeName::U64);
        assert_eq!(fts.get_label(0), 1);
        assert_eq!(fts.get_parent(0), None);
        assert_eq!(fts.get_type(2), DataTypeName::Double);
        assert_eq!(fts.get_label(2), 12);
        assert_eq!(fts.get_parent(2), Some(0));

        assert_eq!(fts.get_parent_label(111), Some(11));
        assert_eq!(fts.get_parent_label(1), None);

        assert_eq!(fts.is_below(1, 1), true);
        assert_eq!(fts.is_below(2, 0), true);
        assert_eq!(fts.is_below(2, 1), false);
        assert_eq!(fts.is_below(5, 1), true);
    }
}
