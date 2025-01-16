//! This module defines [TrieScanFilterHook] and [GeneratorFilterHook].

use std::{
    cell::{RefCell, UnsafeCell},
    rc::Rc,
};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanT},
        operations::{filter_hook::ColumnScanFilterHook, pass::ColumnScanPass},
    },
    datatypes::{
        into_datavalue::IntoDataValue, storage_type_name::StorageTypeBitSet, StorageTypeName,
    },
    datavalues::AnyDataValue,
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
    util::{hook::FilterHook, mapping::permutation::Permutation},
};

use super::OperationGenerator;

/// Used to create a [TrieScanFilterHook]
#[derive(Debug)]
pub(crate) struct GeneratorFilterHook {
    /// String-part which is passed to [FilterHook]
    string: String,
    /// [Permutation] that determines the order of the arguments to `hook`
    permutation: Permutation,
    /// Function that will be called on every table row
    filter: FilterHook,
}

impl GeneratorFilterHook {
    /// Create a new [GeneratorFilterHook].
    pub(crate) fn new(string: String, filter: FilterHook, permutation: Permutation) -> Self {
        Self {
            string,
            permutation,
            filter,
        }
    }
}

impl OperationGenerator for GeneratorFilterHook {
    fn generate<'a>(
        &'_ self,
        mut trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        debug_assert!(trie_scans.len() == 1);

        let trie_scan = trie_scans.remove(0)?;
        let arity = trie_scan.arity();

        let default_values = vec![AnyDataValue::new_boolean(false); arity];
        let reference_values = Rc::new(RefCell::new(default_values));
        let mut column_scans: Vec<UnsafeCell<ColumnScanT<'a>>> = Vec::with_capacity(arity);

        for column_index in 0..arity {
            macro_rules! output_scan {
                ($type:ty, $variant:ident, $scan:ident) => {{
                    let input_scan = &unsafe { &*trie_scan.scan(column_index).get() }.$scan;

                    if column_index < arity - 1 {
                        ColumnScanEnum::Pass(ColumnScanPass::new(input_scan))
                    } else {
                        let reference_index = self.permutation.get(column_index);

                        ColumnScanEnum::FilterHook(ColumnScanFilterHook::new(
                            input_scan,
                            self.filter.clone(),
                            self.string.clone(),
                            reference_index,
                            reference_values.clone(),
                            dictionary,
                        ))
                    }
                }};
            }

            let output_scan_id32 = output_scan!(u32, Id32, scan_id32);
            let output_scan_id64 = output_scan!(u64, Id64, scan_id64);
            let output_scan_i64 = output_scan!(i64, Int64, scan_i64);
            let output_scan_float = output_scan!(Float, Float, scan_float);
            let output_scan_double = output_scan!(Double, Double, scan_double);

            let new_scan = ColumnScanT::new(
                output_scan_id32,
                output_scan_id64,
                output_scan_i64,
                output_scan_float,
                output_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        Some(TrieScanEnum::FilterHook(TrieScanFilterHook {
            trie_scan: Box::new(trie_scan),
            dictionary,
            column_scans,
            path_types: Vec::with_capacity(arity),
            permutation: self.permutation.clone(),
            input_values: reference_values,
        }))
    }
}

#[derive(Debug)]
pub struct TrieScanFilterHook<'a> {
    /// Input trie scan which will be filtered
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    dictionary: &'a RefCell<Dict>,

    /// [Permutation] that determines the order of the arguments to `hook`
    permutation: Permutation,

    /// Values that will be used as input for evaluating
    input_values: Rc<RefCell<Vec<AnyDataValue>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
    /// For each layer in the resulting trie contains a [ColumnScanT]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanFilterHook<'a> {
    fn up(&mut self) {
        self.trie_scan.up();
        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types.last();

        let next_layer = previous_layer.map_or(0, |layer| layer + 1);

        if let Some((previous_layer, previous_type)) = previous_layer.zip(previous_type) {
            // This value will be used in some future layer as an input to a function,
            // so we translate it to an AnyDataValue and store it in `self.input_values`.
            let column_value = self.column_scans[previous_layer]
                .get_mut()
                .current(*previous_type)
                .expect(
                    "It is only allowed to call down while the previous scan points to some value.",
                )
                .into_datavalue(&self.dictionary.borrow())
                .expect("All ids occuring in a column must be known to the dictionary");

            self.input_values.borrow_mut()[self.permutation.get(previous_layer)] = column_value;
        }

        self.trie_scan.down(next_type);
        self.path_types.push(next_type);
        self.column_scans[next_layer].get_mut().reset(next_type);
    }

    fn arity(&self) -> usize {
        self.trie_scan.arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        &self.column_scans[layer]
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        self.trie_scan.possible_types(layer)
    }

    fn current_layer(&self) -> Option<usize> {
        self.path_types.len().checked_sub(1)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::{StorageTypeName, StorageValueT},
        datavalues::{AnyDataValue, DataValue},
        dictionary::meta_dv_dict::MetaDvDictionary,
        tabular::{operations::OperationGenerator, triescan::TrieScanEnum},
        util::{
            mapping::permutation::Permutation,
            test_util::test::{trie_dfs, trie_int64},
        },
    };

    use super::{FilterHook, GeneratorFilterHook};

    #[test]
    fn filter_hook() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![
            &[1, 2, 0],
            &[1, 3, 4],
            &[1, 4, 5],
            &[1, 5, 2],
            &[1, 6, 7],
            &[1, 7, 1],
        ]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let function = FilterHook::from(|_string: &str, values: &[AnyDataValue]| {
            values[0].to_i64_unchecked() + values[1].to_i64_unchecked()
                == values[2].to_i64_unchecked()
        });

        let filter_generator =
            GeneratorFilterHook::new(String::default(), function, Permutation::default());
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(2), // y = 3
                StorageValueT::Int64(3), // y = 3
                StorageValueT::Int64(4), // z = 4
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(5), // z = 5
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(6), // y = 6
                StorageValueT::Int64(7), // z = 7
                StorageValueT::Int64(7), // y = 7
            ],
        );
    }
}
