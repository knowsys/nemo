//! Implementation of the constant trie scan,
//! which has a singular column with a singular constant value.

use std::cell::UnsafeCell;

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanT},
        operations::constant::ColumnScanConstant,
    },
    datatypes::{storage_type_name::StorageTypeBitSet, StorageTypeName, StorageValueT},
    tabular::triescan::PartialTrieScan,
};

/// A trie scan which contains a singular column
/// with singular entry
#[derive(Debug)]
pub(crate) struct TrieScanConstant<'a> {
    column: UnsafeCell<ColumnScanT<'a>>,
    position: Option<StorageTypeName>,
    storage_type: StorageTypeName,
}

impl<'a> TrieScanConstant<'a> {
    pub fn new(value: StorageValueT) -> TrieScanConstant<'a> {
        macro_rules! no_scan {
            () => {
                ColumnScanEnum::Constant(ColumnScanConstant::new(None))
            };
        }

        let column = match value {
            StorageValueT::Id32(c) => {
                let scan = ColumnScanEnum::Constant(ColumnScanConstant::new(Some(c)));
                UnsafeCell::new(ColumnScanT::new(
                    scan,
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                ))
            }
            StorageValueT::Id64(c) => {
                let scan = ColumnScanEnum::Constant(ColumnScanConstant::new(Some(c)));
                UnsafeCell::new(ColumnScanT::new(
                    no_scan!(),
                    scan,
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                ))
            }
            StorageValueT::Int64(c) => {
                let scan = ColumnScanEnum::Constant(ColumnScanConstant::new(Some(c)));
                UnsafeCell::new(ColumnScanT::new(
                    no_scan!(),
                    no_scan!(),
                    scan,
                    no_scan!(),
                    no_scan!(),
                ))
            }

            StorageValueT::Float(c) => {
                let scan = ColumnScanEnum::Constant(ColumnScanConstant::new(Some(c)));
                UnsafeCell::new(ColumnScanT::new(
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                    scan,
                    no_scan!(),
                ))
            }

            StorageValueT::Double(c) => {
                let scan = ColumnScanEnum::Constant(ColumnScanConstant::new(Some(c)));
                UnsafeCell::new(ColumnScanT::new(
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                    no_scan!(),
                    scan,
                ))
            }
        };

        Self {
            position: None,
            column,
            storage_type: value.get_type(),
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanConstant<'a> {
    fn up(&mut self) {
        _ = self.position.take().expect("called up on first layer");
    }

    fn down(&mut self, storage_type: StorageTypeName) {
        assert_eq!(self.position, None, "called down on lowest layer");
        self.position = Some(storage_type);
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        assert_eq!(layer, 0);
        self.storage_type.bitset()
    }

    fn arity(&self) -> usize {
        1
    }

    fn current_layer(&self) -> Option<usize> {
        self.position.and(Some(0))
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        assert_eq!(layer, 0);
        &self.column
    }
}
