//! This module defines [GeneratorZero].

use std::cell::{RefCell, UnsafeCell};

use crate::{
    columnar::columnscan::ColumnScanT,
    datatypes::{StorageTypeName, storage_type_name::StorageTypeBitSet},
    management::database::Dict,
    tabular::{
        operations::OperationGenerator,
        triescan::{PartialTrieScan, TrieScanEnum},
    },
};

/// Used to create a trie iterator over a non-empty
/// table with zero arity
#[derive(Debug, Clone)]
pub(crate) struct GeneratorZero {}

impl GeneratorZero {
    /// Create a new [GeneratorZero].
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl OperationGenerator for GeneratorZero {
    fn generate<'a>(
        &'_ self,
        _input: Vec<Option<TrieScanEnum<'a>>>,
        _dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        Some(TrieScanEnum::Zero(TrieScanZero::new()))
    }
}

/// [PartialTrieScan] over a non-empty table with arity zero
#[derive(Debug, Clone)]
pub(crate) struct TrieScanZero {
    /// Whether scan is in the first layer
    down: bool,
}

impl TrieScanZero {
    /// Create a new [TrieScanZero].
    pub fn new() -> Self {
        Self { down: false }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanZero {
    fn up(&mut self) {
        self.down = false;
    }

    fn down(&mut self, _storage_type: StorageTypeName) {
        self.down = true;
    }

    fn possible_types(&self, _layer: usize) -> StorageTypeBitSet {
        StorageTypeBitSet::empty()
    }

    fn arity(&self) -> usize {
        0
    }

    fn current_layer(&self) -> Option<usize> {
        if self.down { Some(0) } else { None }
    }

    fn scan<'b>(&'b self, _layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        panic!("trie has arity zero")
    }
}
