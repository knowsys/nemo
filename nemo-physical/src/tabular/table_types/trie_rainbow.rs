use std::{cell::UnsafeCell, ops::Range};

use crate::{
    columnar::{
        column_types::rainbow::{BlockBounds, ColumnRainbow},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    },
    datatypes::{Double, StorageTypeName, StorageValueT},
    tabular::traits::{partial_trie_scan::PartialTrieScan, table::Table, trie_scan::TrieScan},
};

/// TODO: Adjust description once this replaces [`Trie`]
///
/// Experimental version of a trie which works with [`ColumnRainbow`]s
#[derive(Debug, Clone)]
pub struct TrieRainbow {
    columns: Vec<ColumnRainbow>,
}

impl TrieRainbow {
    /// Create new [`TrieRainbow`].
    pub fn new(columns: Vec<ColumnRainbow>) -> Self {
        Self { columns }
    }

    /// Return a reference to the [`ColumnRainbow`]s of this trie.
    pub fn columns(&self) -> &[ColumnRainbow] {
        &self.columns
    }

    /// Return a reference to the [`ColumnRainbow`] of a given index of this trie.
    ///
    /// # Panics
    /// Panics if no column exist for the given index.
    pub fn column(&self, index: usize) -> &ColumnRainbow {
        &self.columns[index]
    }
}

/// TODO: Adjust description once this replaces [`Trie`]
///
/// Implementation of [`PartialTrieScanRainbow`] for [`TrieScanRainbow`]
#[derive(Debug)]
pub struct TrieScanRainbow<'a> {
    /// Underlying [`TrieRainbow`] over which we are traversing
    trie: &'a TrieRainbow,
    ///
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
    /// Current layer within this scan
    current_layer: Option<usize>,
}

/// TODO: Adjust description once this replaces [`Trie`]
///
/// Alternate version of ColumnScanT which contains column scans for all types
#[derive(Debug)]
pub struct ColumnScanRainbow<'a> {
    /// Scan for keys
    pub scan_keys: ColumnScanCell<'a, u64>,
    /// Scan for integers
    pub scan_integers: ColumnScanCell<'a, i64>,
    /// Scan for [`Double`]s
    pub scan_doubles: ColumnScanCell<'a, Double>,
}

// TODO: Weird interface
impl<'a> ColumnScanRainbow<'a> {
    /// Create a new [`ColumnScanRainbow`]
    pub fn new(
        scan_keys: ColumnScanEnum<'a, u64>,
        scan_integers: ColumnScanEnum<'a, i64>,
        scan_doubles: ColumnScanEnum<'a, Double>,
    ) -> Self {
        Self {
            scan_keys: ColumnScanCell::new(scan_keys),
            scan_integers: ColumnScanCell::new(scan_integers),
            scan_doubles: ColumnScanCell::new(scan_doubles),
        }
    }

    pub fn reset_all(&mut self) {
        self.scan_keys.reset();
        self.scan_integers.reset();
        self.scan_doubles.reset();
    }

    pub fn reset(&mut self, storage_type: StorageTypeName) {
        match storage_type {
            StorageTypeName::U64 => self.scan_keys.reset(),
            StorageTypeName::I64 => self.scan_integers.reset(),
            StorageTypeName::Double => self.scan_doubles.reset(),
            StorageTypeName::U32 => todo!(),
            StorageTypeName::Float => todo!(),
        }
    }

    pub fn position(&self, storage_type: StorageTypeName) -> Option<usize> {
        match storage_type {
            StorageTypeName::U64 => self.scan_keys.pos(),
            StorageTypeName::I64 => self.scan_integers.pos(),
            StorageTypeName::Double => self.scan_doubles.pos(),
            StorageTypeName::U32 => todo!(),
            StorageTypeName::Float => todo!(),
        }
    }

    pub fn narrow(&mut self, bounds: BlockBounds) {
        if let Some(bound) = bounds.bound_keys {
            self.scan_keys.narrow(bound);
        }

        if let Some(bound) = bounds.bound_integers {
            self.scan_integers.narrow(bound);
        }

        if let Some(bound) = bounds.bound_doubles {
            self.scan_doubles.narrow(bound);
        }
    }

    pub fn get_smallest_scans(&mut self, storage_type: StorageTypeName) -> &Vec<bool> {
        match storage_type {
            StorageTypeName::U64 => self.scan_keys.get_smallest_scans(),
            StorageTypeName::I64 => self.scan_integers.get_smallest_scans(),
            StorageTypeName::Double => self.scan_doubles.get_smallest_scans(),
            StorageTypeName::U32 => todo!(),
            StorageTypeName::Float => todo!(),
        }
    }

    pub fn set_active_scans(&mut self, storage_type: StorageTypeName, active_scans: Vec<usize>) {
        match storage_type {
            StorageTypeName::U64 => self.scan_keys.set_active_scans(active_scans),
            StorageTypeName::I64 => self.scan_integers.set_active_scans(active_scans),
            StorageTypeName::Double => self.scan_doubles.set_active_scans(active_scans),
            StorageTypeName::U32 => todo!(),
            StorageTypeName::Float => todo!(),
        }
    }
}

/// TODO: Adjust description once this replaces [`Trie`]
///
/// Iterator for a trie whose column may contain multiple data types.
/// Allows for vertical traversal through the trie and
/// returns a column scan for horizontal traversal.
pub trait PartialTrieScanRainbow<'a>: std::fmt::Debug {
    /// Return to the upper layer.
    ///
    /// # Panics
    /// May panic if this method is called while being on the upper most layer.
    fn up(&mut self);

    /// From the current iterator position enter the next layer.
    ///
    /// # Panics
    /// May panic if this method is called while being on the lowest layer
    /// or if this method is called while the iterator of the current layer does not point to any element.
    fn down(&mut self, storage_type: StorageTypeName);

    /// Return the number of columns associated with this table
    fn arity(&self) -> usize;

    /// Return the index of the current layer for this scan.
    fn current_layer(&self) -> Option<usize>;

    /// Return the underlying [`ColumnScanRainbow`] object given an index.
    fn scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>>;

    /// Return the current position of the scan as a [`ColumnScanRainbow`].
    fn current_scan(&mut self) -> Option<&mut ColumnScanRainbow<'a>>;
}

impl<'a> PartialTrieScanRainbow<'a> for TrieScanRainbow<'a> {
    fn up(&mut self) {
        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));
    }

    fn down(&mut self, storage_type: StorageTypeName) {
        match self.current_layer {
            None => {
                self.current_layer = Some(0);

                // This `reset` is necessary because of the following scenario:
                // Calling `up` at the first layer while the first layer still points to some position.
                // Going `down` from there without the `reset` would lead to the first scan
                // still pointing to the previous position instead of starting at `None` as expected.
                // This is not needed for the Some(_) path as calling `narrow` has a similar effect.
                self.column_scans[0].get_mut().reset_all();
            }
            Some(current_layer) => {
                debug_assert!(
                    current_layer < self.column_scans.len(),
                    "Called down while on the last layer"
                );

                let current_local_index = self.column_scans[current_layer].get_mut().position(storage_type).expect(
                    "Calling down on a trie is only allowed when currently pointing at an element.",
                );
                let current_global_index = self
                    .trie
                    .column(current_layer)
                    .translate_index(current_local_index, storage_type);

                let next_layer = current_layer + 1;
                let next_layer_blocks = self
                    .trie
                    .column(next_layer)
                    .block_bounds(current_global_index);

                self.column_scans[next_layer]
                    .get_mut()
                    .narrow(next_layer_blocks);

                self.current_layer = Some(next_layer);
            }
        }
    }

    fn arity(&self) -> usize {
        self.column_scans.len()
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }

    fn scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>> {
        Some(&self.column_scans[index])
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanRainbow<'a>> {
        Some(self.column_scans[self.current_layer?].get_mut())
    }
}
