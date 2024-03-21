use crate::datatypes::{storage_type_name::StorageTypeBitSet, StorageTypeName};

use super::{
    divided_column::DividedColumn,
    multi_column::{Multicolumn, MulticolumnGroup, MulticolumnGroupIterator},
};

#[derive(Debug)]
pub struct MultiIntervalcolumn {
    groups: Multicolumn,
    successors: DividedColumn,
    storage_types: Vec<u8>,
}

#[derive(Debug)]
pub struct IntervalBound {
    storage_type: StorageTypeName,
    global: usize,
    start: usize,
    length: usize,
}

impl MultiIntervalcolumn {
    pub fn interval_bounds(&self, node_index: usize) -> Vec<IntervalBound> {
        let supported_types =
            StorageTypeBitSet::from(usize::from(self.storage_types[node_index])).storage_types();
        let mut group_iterator = MulticolumnGroupIterator::new(&self.groups);

        supported_types
            .into_iter()
            .map(|storage_type| {
                let MulticolumnGroup {
                    global,
                    length,
                    start,
                } = group_iterator.next();

                IntervalBound {
                    storage_type,
                    global,
                    start,
                    length,
                }
            })
            .collect()
    }
}
