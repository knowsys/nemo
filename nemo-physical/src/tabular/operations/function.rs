//! This module defines [TrieScanFunction] and [GeneratorFunction].

use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap, HashSet},
};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanRainbow},
        operations::{constant::ColumnScanConstant, ColumnScanPass},
    },
    datatypes::StorageTypeName,
    datavalues::{any_datavalue::IntoDataValue, AnyDataValue},
    dictionary::meta_dv_dict::MetaDictionary,
    function::{evaluation::StackProgram, tree::FunctionTree},
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationColumnMarker, OperationGenerator, OperationTable};

/// Assigns an output column to a function which determines how to compute the values of that column
type FunctionAssignment = HashMap<OperationColumnMarker, FunctionTree<OperationColumnMarker>>;

/// Marks whether an output column results from an input column or is computed via a [StackProgram]
#[derive(Debug, Clone)]
enum OutputColumn {
    /// Index of the input column
    Input(usize),
    /// [StackProgram] for computing the output
    Computed(StackProgram),
}

/// Used to create [TrieScanFunction]
#[derive(Debug, Clone)]
pub struct GeneratorFunction {
    /// For each output column
    /// either contains the index to the input columns it is derived from
    /// or the [StackProgram] used to compute it
    output_columns: Vec<OutputColumn>,
    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
}

impl GeneratorFunction {
    /// Create a new [GeneratorFunction]
    pub fn new(output: OperationTable, functions: FunctionAssignment) -> Self {
        let mut output_columns = Vec::<OutputColumn>::new();
        let mut input_indices = Vec::<bool>::new();

        let referenced_columns: HashSet<OperationColumnMarker> = functions
            .iter()
            .flat_map(|(_, function)| function.references())
            .collect();
        let mut input_index: usize = 0;
        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();

        for output_marker in output.into_iter() {
            let is_function_input = referenced_columns.contains(&output_marker);
            input_indices.push(is_function_input);

            if is_function_input {
                let num_function_input = reference_map.len();
                if let Entry::Vacant(entry) = reference_map.entry(output_marker.clone()) {
                    entry.insert(num_function_input);
                }
            }

            match functions.get(&output_marker) {
                Some(function) => {
                    output_columns.push(OutputColumn::Computed(StackProgram::from_function_tree(
                        function,
                        &reference_map,
                        None,
                    )));
                }
                None => {
                    output_columns.push(OutputColumn::Input(input_index));
                    input_index += 1;
                }
            }
        }

        Self {
            output_columns,
            input_indices,
        }
    }
}

impl OperationGenerator for GeneratorFunction {
    fn generate<'a>(
        &'_ self,
        mut input: Vec<TrieScanEnum<'a>>,
        dictionary: &'a MetaDictionary,
    ) -> TrieScanEnum<'a> {
        let trie_scan = input.remove(0);

        let mut column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>> =
            Vec::with_capacity(self.output_columns.len());

        for output_column in &self.output_columns {
            macro_rules! output_scan {
                ($type:ty, $scan:ident) => {{
                    match output_column {
                        OutputColumn::Computed(_) => {
                            ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None))
                        }
                        OutputColumn::Input(input_index) => {
                            let input_scan = &unsafe { &*trie_scan.scan(*input_index).get() }.$scan;
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

        let output_functions = self
            .output_columns
            .iter()
            .cloned()
            .map(|output| match output {
                OutputColumn::Input(_) => None,
                OutputColumn::Computed(program) => Some(program),
            })
            .collect();

        TrieScanEnum::TrieScanFunction(TrieScanFunction {
            trie_scan: Box::new(trie_scan),
            dictionary,
            input_indices: self.input_indices.clone(),
            output_functions,
            input_values: Vec::new(),
            column_scans,
            path_types: Vec::new(),
        })
    }
}

/// [PartialTrieScan] which introduced additional columns which contain the result
/// of evaluating provided functions over the columns of an input trie
#[derive(Debug)]
pub struct TrieScanFunction<'a> {
    /// Input trie scan the content of which is used for the definition of the new columns
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    /// TODO: Check lifetimes
    dictionary: &'a MetaDictionary,

    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
    /// For each output layer contains the stack program used to evaluate its content,
    /// or alternatively contains `None` if the input colums should passed instead
    output_functions: Vec<Option<StackProgram>>,
    /// Values that will be used as input for evaluating
    input_values: Vec<AnyDataValue>,

    /// For each layer in the resulting trie contains a [`ColumnScanRainbow`]
    /// evaluating the functions on columns of the input trie
    /// or simply passing the values from the input trie to the output.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
}

impl<'a> PartialTrieScan<'a> for TrieScanFunction<'a> {
    fn up(&mut self) {
        let current_layer = self.path_types.len();

        if self.output_functions[current_layer].is_none() {
            // If the current output layer corresponds to a layer in the input trie,
            // we need to call up on that
            self.trie_scan.up();
        }

        if self.input_indices[current_layer] {
            // The input value is no longer valid
            self.input_values.pop();
        }

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types.last();

        let next_layer = self.path_types.len();

        if let Some(previous_layer) = previous_layer {
            let previous_type =
                *previous_type.expect("If previous_layer is not None so is previuos_type.");

            if self.input_indices[previous_layer] {
                // This value will be used in some future layer as an input to a function,
                // so we translate it to an AnyDataValue and store it in `self.input_values`.

                let column_value = self.column_scans[previous_layer].get_mut().current(previous_type).expect("It is only allowed to call down while the previous scan points to some value.").into_datavalue(self.dictionary).expect("All ids occuring in a column must be known to the dictionary");
                self.input_values.push(column_value);
            }
        }

        // There are two cases.
        // Either we enter a layer which simply corresponds to an input layer.
        // In this case we need to call down in the input trie.
        // Otherwise we need to compute the new value and pass it into the column scan of this layer.
        if let Some(program) = &self.output_functions[next_layer] {
            let function_result = program.evaluate_data(&self.input_values);
            match function_result {
                Some(result) => {
                    let result_storage = result.to_storage_value_t(&self.dictionary);
                    self.column_scans[next_layer]
                        .get_mut()
                        .constant_set(result_storage);
                }
                None => {
                    self.column_scans[next_layer]
                        .get_mut()
                        .constant_set_none(next_type);
                }
            }
        } else {
            self.trie_scan.down(next_type);
        }

        self.column_scans[next_layer].get_mut().reset(next_type);
        self.path_types.push(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        &self.path_types
    }

    fn arity(&self) -> usize {
        self.column_scans.len()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }
}
