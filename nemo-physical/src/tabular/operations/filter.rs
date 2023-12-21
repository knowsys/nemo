//! This module defines [TrieScanFilter] and [GeneratorFilter].

use std::{
    cell::{RefCell, UnsafeCell},
    collections::{hash_map::Entry, HashMap, HashSet},
    rc::Rc,
};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanRainbow},
        operations::{filter::ColumnScanFilter, ColumnScanPass},
    },
    datatypes::{into_datavalue::IntoDataValue, StorageTypeName},
    datavalues::AnyDataValue,
    dictionary::meta_dv_dict::MetaDictionary,
    function::{evaluation::StackProgram, tree::FunctionTree},
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationColumnMarker, OperationGenerator, OperationTable};

/// Assigns an input column to a function which filters values from that column
type FilterAssignment = HashMap<OperationColumnMarker, FunctionTree<OperationColumnMarker>>;

/// Marks whether an output column results from an input column or has a filter applied to it via a [StackProgram]
#[derive(Debug, Clone)]
enum OutputColumn {
    /// Columns is just passed to the output
    Input,
    /// [StackProgram] for filtering values of the corresponding columns
    Filtered(StackProgram),
}

/// Used to create a [TrieScanFilter]
#[derive(Debug, Clone)]
pub struct GeneratorFilter {
    /// For each output column
    /// marks whether an output column results from an input column or has a filter applied to it via a [StackProgram]
    output_columns: Vec<OutputColumn>,
    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
}

impl GeneratorFilter {
    /// Create a new [GeneratorFilter].
    pub fn new(input: OperationTable, filters: FilterAssignment) -> Self {
        let mut output_columns = Vec::<OutputColumn>::new();
        let mut input_indices = Vec::<bool>::new();

        let referenced_columns: HashSet<OperationColumnMarker> = filters
            .iter()
            .flat_map(|(_, function)| function.references())
            .collect();
        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();

        for input_marker in input.into_iter() {
            let is_function_input = referenced_columns.contains(&input_marker);
            input_indices.push(is_function_input);

            if is_function_input {
                let num_function_input = reference_map.len();
                if let Entry::Vacant(entry) = reference_map.entry(input_marker) {
                    entry.insert(num_function_input);
                }
            }

            match filters.get(&input_marker) {
                Some(function) => {
                    output_columns.push(OutputColumn::Filtered(StackProgram::from_function_tree(
                        function,
                        &reference_map,
                        Some(input_marker),
                    )));
                }
                None => {
                    output_columns.push(OutputColumn::Input);
                }
            }
        }

        Self {
            output_columns,
            input_indices,
        }
    }
}

impl OperationGenerator for GeneratorFilter {
    fn generate<'a>(
        &'_ self,
        mut trie_scans: Vec<TrieScanEnum<'a>>,
        dictionary: &'a MetaDictionary,
    ) -> TrieScanEnum<'a> {
        let trie_scan = trie_scans.remove(0);

        let mut column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>> =
            Vec::with_capacity(self.output_columns.len());

        let input_values = Rc::new(RefCell::new(Vec::<AnyDataValue>::new()));

        for (index, output_column) in self.output_columns.iter().enumerate() {
            macro_rules! output_scan {
                ($type:ty, $scan:ident) => {{
                    match output_column {
                        OutputColumn::Filtered(program) => {
                            let input_scan = &unsafe { &*trie_scan.scan(index).get() }.$scan;
                            ColumnScanEnum::ColumnScanFilter(ColumnScanFilter::new(
                                input_scan,
                                program.clone(),
                                input_values.clone(),
                                dictionary,
                            ))
                        }
                        OutputColumn::Input => {
                            let input_scan = &unsafe { &*trie_scan.scan(index).get() }.$scan;
                            ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(input_scan))
                        }
                    }
                }};
            }

            let output_scan_id32 = output_scan!(u32, scan_id32);
            let output_scan_id64 = output_scan!(u64, scan_id64);
            let output_scan_i64 = output_scan!(i64, scan_i64);
            let output_scan_float = output_scan!(Float, scan_float);
            let output_scan_double = output_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                output_scan_id32,
                output_scan_id64,
                output_scan_i64,
                output_scan_float,
                output_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        TrieScanEnum::TrieScanFilter(TrieScanFilter {
            trie_scan: Box::new(trie_scan),
            dictionary,
            input_indices: self.input_indices.clone(),
            input_values,
            column_scans,
        })
    }
}

/// [PartialTrieScan] which filters values of an input [PartialTrieScan]
#[derive(Debug)]
pub struct TrieScanFilter<'a> {
    /// Input trie scan which will be filtered
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    /// TODO: Check lifetimes
    dictionary: &'a MetaDictionary,

    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
    /// Values that will be used as input for evaluating
    input_values: Rc<RefCell<Vec<AnyDataValue>>>,

    /// For each layer in the resulting trie contains a [`ColumnScanRainbow`]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanFilter<'a> {
    fn up(&mut self) {
        if let Some(current_layer) = self.current_layer() {
            if self.input_indices[current_layer] {
                // The input value is no longer valid
                self.input_values.borrow_mut().pop();
            }
        }

        self.trie_scan.up();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types().last();

        let next_layer = previous_layer.map_or(0, |layer| layer + 1);

        if let Some(previous_layer) = previous_layer {
            let previous_type =
                *previous_type.expect("If previous_layer is not None so is previuos_type.");

            if self.input_indices[previous_layer] {
                // This value will be used in some future layer as an input to a function,
                // so we translate it to an AnyDataValue and store it in `self.input_values`.
                let column_value = self.column_scans[previous_layer].get_mut().current(previous_type).expect("It is only allowed to call down while the previous scan points to some value.").into_datavalue(self.dictionary).expect("All ids occuring in a column must be known to the dictionary");
                self.input_values.borrow_mut().push(column_value);
            }
        }

        self.trie_scan.down(next_type);
        self.column_scans[next_layer].get_mut().reset(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        self.trie_scan.path_types()
    }

    fn arity(&self) -> usize {
        self.trie_scan.arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }
}
