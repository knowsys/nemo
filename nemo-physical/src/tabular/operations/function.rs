//! This module defines [TrieScanFunction] and [GeneratorFunction].

use std::{
    cell::{RefCell, UnsafeCell},
    collections::{hash_map::Entry, HashMap, HashSet},
};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanRainbow},
        operations::{constant::ColumnScanConstant, pass::ColumnScanPass},
    },
    datatypes::{
        into_datavalue::IntoDataValue, storage_type_name::StorageTypeBitSet, StorageTypeName,
        StorageValueT,
    },
    datavalues::AnyDataValue,
    function::{evaluation::StackProgram, tree::FunctionTree},
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationColumnMarker, OperationGenerator, OperationTable};

/// Assigns an output column to a function which determines how to compute the values of that column
pub type FunctionAssignment = HashMap<OperationColumnMarker, FunctionTree<OperationColumnMarker>>;

/// Marks an output column as either being the same as an input column
/// or computing a new value
#[derive(Debug, PartialEq, Eq, Clone)]
enum ComputedMarker {
    /// Layer copies value from input trie
    Copy,
    /// Layer computes new value from input layers
    Computed,
}

/// Contains information about a layer in [TrieScanFunction]
#[derive(Debug, Clone)]
struct LayerInformation {
    /// Whether layer is the same as input or contains a computed value
    pub(self) computed: ComputedMarker,
    /// Whether output of this layer is used as an input value for some function.
    /// If this is the case, then this value will be `Some(list)`
    /// where `list` contains the indices of programs that can be computed in this layer.
    pub(self) input: Option<Vec<usize>>,
}

/// Used to create [TrieScanFunction]
#[derive(Debug)]
pub struct GeneratorFunction {
    /// Encodes for each layer information about what to do via [LayerInformation].
    layer_information: Vec<LayerInformation>,
    /// Contains all [StackProgram]s used to compute new values
    programs: Vec<StackProgram>,
}

impl GeneratorFunction {
    /// Create a new [GeneratorFunction]
    pub fn new(output: OperationTable, functions: &FunctionAssignment) -> Self {
        let mut layer_information = Vec::with_capacity(output.len());
        let mut programs = Vec::with_capacity(output.len());

        let referenced_columns: HashSet<OperationColumnMarker> = functions
            .iter()
            .flat_map(|(_, function)| function.references())
            .collect();

        let mut last_input_layers = vec![Vec::new(); output.len()];
        for (function_layer, function) in output
            .iter()
            .filter_map(|marker| functions.get(marker))
            .enumerate()
        {
            let last_input = Self::last_reference(&output, &function.references()).unwrap_or(0);
            last_input_layers[last_input].push(function_layer);
        }

        let mut reference_map = HashMap::<OperationColumnMarker, usize>::new();

        for (output_index, output_marker) in output.into_iter().enumerate() {
            let is_function_input = referenced_columns.contains(&output_marker);

            let input = if is_function_input {
                let num_function_input = reference_map.len();
                if let Entry::Vacant(entry) = reference_map.entry(output_marker) {
                    entry.insert(num_function_input);
                }

                Some(last_input_layers[output_index].clone())
            } else {
                None
            };

            let computed = match functions.get(&output_marker) {
                Some(function) => {
                    programs.push(StackProgram::from_function_tree(
                        function,
                        &reference_map,
                        None,
                    ));

                    ComputedMarker::Computed
                }
                None => ComputedMarker::Copy,
            };

            layer_information.push(LayerInformation { computed, input });
        }

        Self {
            layer_information,
            programs,
        }
    }

    fn last_reference(
        table: &OperationTable,
        references: &[OperationColumnMarker],
    ) -> Option<usize> {
        references
            .into_iter()
            .map(|marker| {
                table
                    .position(marker)
                    .expect("Every reference must occur in the table")
            })
            .max()
    }

    /// Returns whether this operation does not alter the input table.
    fn is_unchanging(&self) -> bool {
        // This operation behaves the same as the identity if
        // no new columns are computed
        self.layer_information
            .iter()
            .all(|info| info.computed == ComputedMarker::Copy)
    }
}

impl OperationGenerator for GeneratorFunction {
    fn generate<'a>(
        &'_ self,
        mut input: Vec<Option<TrieScanEnum<'a>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        debug_assert!(input.len() == 1);

        let trie_scan = input.remove(0)?;
        if self.is_unchanging() {
            return Some(trie_scan);
        }

        let mut column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>> =
            Vec::with_capacity(self.layer_information.len());

        let mut input_index: usize = 0;

        for information in &self.layer_information {
            macro_rules! output_scan {
                ($type:ty, $scan:ident) => {{
                    match information.computed {
                        ComputedMarker::Computed => {
                            ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None))
                        }
                        ComputedMarker::Copy => {
                            let input_scan = &unsafe { &*trie_scan.scan(input_index).get() }.$scan;
                            input_index += 1;

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

        Some(TrieScanEnum::TrieScanFunction(TrieScanFunction {
            trie_scan: Box::new(trie_scan),
            dictionary,
            input_values: Vec::new(),
            column_scans,
            path_types: Vec::new(),
            layer_information: self.layer_information.clone(),
            programs: self.programs.clone(),
            output_values: Vec::new(),
            output_index: 0,
        }))
    }
}

/// [PartialTrieScan] which introduced additional columns which contain the result
/// of evaluating provided functions over the columns of an input trie
#[derive(Debug)]
pub(crate) struct TrieScanFunction<'a> {
    /// Input trie scan the content of which is used for the definition of the new columns
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    dictionary: &'a RefCell<Dict>,

    /// Encodes for each layer information about what to do via [LayerInformation].
    layer_information: Vec<LayerInformation>,
    /// Contains all [StackProgram]s used to compute new values
    programs: Vec<StackProgram>,
    /// Values that will be used as input when evaluating a [StackProgram] from `programs`
    input_values: Vec<AnyDataValue>,
    /// The results of evaluating the [StackProgram] from `programs`
    output_values: Vec<Option<StorageValueT>>,
    /// Current value in `output_values`,
    output_index: usize,

    /// For each layer in the resulting trie contains a [`ColumnScanRainbow`]
    /// evaluating the functions on columns of the input trie
    /// or simply passing the values from the input trie to the output.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
}

impl<'a> PartialTrieScan<'a> for TrieScanFunction<'a> {
    fn up(&mut self) {
        let current_layer = self.path_types.len() - 1;
        let previous_layer = current_layer.checked_sub(1);

        if self.layer_information[current_layer].computed == ComputedMarker::Copy {
            // If the current output layer corresponds to a layer in the input trie,
            // we need to call up on that
            self.trie_scan.up();
        } else {
            self.output_index -= 1;
        }

        if let Some(previous_layer) = previous_layer {
            if let Some(programs) = &self.layer_information[previous_layer].input {
                // The input value is no longer valid
                self.input_values.pop();

                for _ in 0..programs.len() {
                    self.output_values.pop();
                }
            }
        }

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let next_layer = self.path_types.len();

        if let Some((previous_layer, previous_type)) =
            self.current_layer().zip(self.path_types.last().cloned())
        {
            if let Some(programs) = &self.layer_information[previous_layer].input {
                // The current value will be used as input for some program
                let column_value = self.column_scans[previous_layer].get_mut().current(previous_type).expect("It is only allowed to call down while the previous scan points to some value.").into_datavalue(&self.dictionary.borrow()).expect("All ids occuring in a column must be known to the dictionary");
                self.input_values.push(column_value);

                // All inputs for every program in `programs` is now defined
                for program in programs.into_iter().map(|&index| &self.programs[index]) {
                    let dictionary = &mut self.dictionary.borrow_mut();
                    let program_result = program.evaluate_data(&self.input_values);

                    self.output_values.push(
                        program_result.map(|result| result.to_storage_value_t_mut(dictionary)),
                    );
                }
            }

            if self.layer_information[previous_layer].computed == ComputedMarker::Computed {
                self.output_index += 1;
            }
        }

        match self.layer_information[next_layer].computed {
            ComputedMarker::Copy => self.trie_scan.down(next_type),
            ComputedMarker::Computed => {
                if let Some(output_value) = &self.output_values[self.output_index] {
                    if output_value.get_type() == next_type {
                        self.column_scans[next_layer]
                            .get_mut()
                            .constant_set(output_value.clone());
                    } else {
                        self.column_scans[next_layer]
                            .get_mut()
                            .constant_set_none(next_type);
                    }
                }
            }
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

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        match self.layer_information[layer].computed {
            ComputedMarker::Copy => {
                let input_layer = self
                    .layer_information
                    .iter()
                    .take(layer)
                    .filter(|info| info.computed == ComputedMarker::Copy)
                    .count();

                self.trie_scan.possible_types(input_layer)
            }
            ComputedMarker::Computed => StorageTypeBitSet::full(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::{into_datavalue::IntoDataValue, StorageTypeName, StorageValueT},
        datavalues::AnyDataValue,
        dictionary::DvDict,
        function::tree::FunctionTree,
        management::database::Dict,
        tabular::{
            operations::{OperationGenerator, OperationTableGenerator},
            rowscan::RowScan,
            trie::Trie,
            triescan::TrieScanEnum,
        },
        util::test_util::test::{trie_dfs, trie_int64},
    };

    use super::{FunctionAssignment, GeneratorFunction};

    #[test]
    fn function_constant() {
        let dictionary = RefCell::new(Dict::default());

        let trie = trie_int64(vec![
            &[1, 2, 5],
            &[1, 4, 1],
            &[1, 4, 7],
            &[1, 4, 9],
            &[2, 3, 1],
            &[3, 5, 2],
            &[3, 5, 4],
            &[3, 9, 8],
        ]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");
        marker_generator.add_marker("a");
        marker_generator.add_marker("b");
        marker_generator.add_marker("c");
        marker_generator.add_marker("d");

        let markers = marker_generator.operation_table(["a", "x", "y", "b", "c", "z", "d"].iter());

        let mut assigment = FunctionAssignment::new();
        assigment.insert(
            *marker_generator.get(&"a").unwrap(),
            FunctionTree::constant(AnyDataValue::new_integer_from_i64(2)),
        );
        assigment.insert(
            *marker_generator.get(&"b").unwrap(),
            FunctionTree::constant(AnyDataValue::new_integer_from_i64(3)),
        );
        assigment.insert(
            *marker_generator.get(&"c").unwrap(),
            FunctionTree::constant(AnyDataValue::new_integer_from_i64(4)),
        );
        assigment.insert(
            *marker_generator.get(&"d").unwrap(),
            FunctionTree::constant(AnyDataValue::new_integer_from_i64(1)),
        );

        let function_generator = GeneratorFunction::new(markers, &assigment);
        let mut function_scan = function_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut function_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(2), // a = 2
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(2), // y = 2
                StorageValueT::Int64(3), // b = 3
                StorageValueT::Int64(4), // c = 4
                StorageValueT::Int64(5), // z = 5
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(3), // b = 3
                StorageValueT::Int64(4), // c = 4
                StorageValueT::Int64(1), // z = 1
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(7), // z = 7
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(9), // z = 9
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(2), // x = 2
                StorageValueT::Int64(3), // y = 3
                StorageValueT::Int64(3), // b = 3
                StorageValueT::Int64(4), // c = 4
                StorageValueT::Int64(1), // z = 1
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(3), // x = 3
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(3), // b = 3
                StorageValueT::Int64(4), // c = 4
                StorageValueT::Int64(2), // z = 2
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(4), // z = 4
                StorageValueT::Int64(1), // d = 1
                StorageValueT::Int64(9), // y = 9
                StorageValueT::Int64(3), // b = 3
                StorageValueT::Int64(4), // c = 4
                StorageValueT::Int64(8), // z = 8
                StorageValueT::Int64(1), // d = 1
            ],
        );
    }

    #[test]
    fn function_duplicate() {
        let dictionary = RefCell::new(Dict::default());

        let trie = trie_int64(vec![
            &[1, 3, 7, 10, 4],
            &[1, 3, 7, 10, 5],
            &[1, 3, 7, 11, 6],
            &[1, 3, 7, 11, 7],
            &[1, 3, 8, 12, 8],
            &[1, 3, 8, 13, 9],
            &[1, 4, 9, 10, 10],
            &[2, 5, 6, 10, 11],
        ]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");
        marker_generator.add_marker("v");
        marker_generator.add_marker("w");
        marker_generator.add_marker("a");
        marker_generator.add_marker("b");

        let markers = marker_generator.operation_table(["x", "a", "y", "z", "v", "b", "w"].iter());

        let mut assigment = FunctionAssignment::new();
        assigment.insert(
            *marker_generator.get(&"a").unwrap(),
            FunctionTree::reference(*marker_generator.get(&"x").unwrap()),
        );
        assigment.insert(
            *marker_generator.get(&"b").unwrap(),
            FunctionTree::reference(*marker_generator.get(&"y").unwrap()),
        );

        let function_generator = GeneratorFunction::new(markers, &assigment);
        let mut function_scan = function_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut function_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1),  // x = 1
                StorageValueT::Int64(1),  // a = 1
                StorageValueT::Int64(3),  // y = 3
                StorageValueT::Int64(7),  // z = 7
                StorageValueT::Int64(10), // v = 10
                StorageValueT::Int64(3),  // b = 3
                StorageValueT::Int64(4),  // w = 4
                StorageValueT::Int64(5),  // w = 5
                StorageValueT::Int64(11), // v = 11
                StorageValueT::Int64(3),  // b = 3
                StorageValueT::Int64(6),  // w = 6
                StorageValueT::Int64(7),  // w = 7
                StorageValueT::Int64(8),  // z = 8
                StorageValueT::Int64(12), // v = 12
                StorageValueT::Int64(3),  // b = 3
                StorageValueT::Int64(8),  // w = 8
                StorageValueT::Int64(13), // v = 13
                StorageValueT::Int64(3),  // b = 3
                StorageValueT::Int64(9),  // w = 9
                StorageValueT::Int64(4),  // y = 4
                StorageValueT::Int64(9),  // z = 9
                StorageValueT::Int64(10), // v = 10
                StorageValueT::Int64(4),  // b = 4
                StorageValueT::Int64(10), // w = 10
                StorageValueT::Int64(2),  // x = 2
                StorageValueT::Int64(2),  // a = 2
                StorageValueT::Int64(5),  // y = 5
                StorageValueT::Int64(6),  // z = 6
                StorageValueT::Int64(10), // v = 10
                StorageValueT::Int64(5),  // b = 5
                StorageValueT::Int64(11), // w = 11
            ],
        );
    }

    #[test]
    fn function_arithmetic() {
        let dictionary = RefCell::new(Dict::default());

        let trie = trie_int64(vec![&[1, 3], &[1, 4], &[2, 5]]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("r");

        let markers = marker_generator.operation_table(["x", "y", "r"].iter());
        let marker_x = *marker_generator.get(&"x").unwrap();
        let marker_y = *marker_generator.get(&"y").unwrap();

        let function = FunctionTree::numeric_division(
            FunctionTree::numeric_multiplication(
                FunctionTree::numeric_addition(
                    FunctionTree::reference(marker_x),
                    FunctionTree::constant(AnyDataValue::new_integer_from_i64(3)),
                ),
                FunctionTree::reference(marker_y),
            ),
            FunctionTree::reference(marker_x),
        );

        let mut assigment = FunctionAssignment::new();
        assigment.insert(*marker_generator.get(&"r").unwrap(), function);

        let function_generator = GeneratorFunction::new(markers, &assigment);
        let mut function_scan = function_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut function_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1),  // x = 1
                StorageValueT::Int64(3),  // y = 3
                StorageValueT::Int64(12), // r = 12
                StorageValueT::Int64(4),  // y = 4
                StorageValueT::Int64(16), // r = 16
                StorageValueT::Int64(2),  // x = 2
                StorageValueT::Int64(5),  // y = 5
                StorageValueT::Int64(12), // r = 12
            ],
        );
    }

    #[test]
    fn function_repeat_multiple_types() {
        let mut dictionary = Dict::default();
        let a = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("a")))
            .value() as u32;
        let b = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("b")))
            .value() as u32;
        let foo1 = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("foo1")))
            .value() as u32;
        let foo2 = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("foo2")))
            .value() as u32;
        let bar = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("bar")))
            .value() as u32;
        let dictionary = RefCell::new(dictionary);

        let trie = Trie::from_rows(vec![
            vec![StorageValueT::Id32(a), StorageValueT::Id32(foo1)],
            vec![StorageValueT::Id32(a), StorageValueT::Id32(foo2)],
            vec![StorageValueT::Id32(b), StorageValueT::Int64(42)],
            vec![StorageValueT::Id32(b), StorageValueT::Id32(bar)],
        ]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("rx");
        marker_generator.add_marker("ry");

        let markers = marker_generator.operation_table(["x", "y", "rx", "ry"].iter());
        let marker_x = *marker_generator.get(&"x").unwrap();
        let marker_y = *marker_generator.get(&"y").unwrap();
        let marker_rx = *marker_generator.get(&"rx").unwrap();
        let marker_ry = *marker_generator.get(&"ry").unwrap();

        let mut assigment = FunctionAssignment::new();
        assigment.insert(marker_rx, FunctionTree::reference(marker_x));
        assigment.insert(marker_ry, FunctionTree::reference(marker_y));

        let function_generator = GeneratorFunction::new(markers, &assigment);
        let mut function_scan = function_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut function_scan,
            &[StorageTypeName::Id32, StorageTypeName::Int64],
            &[
                StorageValueT::Id32(a),    // x = "a"
                StorageValueT::Id32(foo1), // y = "foo1"
                StorageValueT::Id32(a),    // rx = "a"
                StorageValueT::Id32(foo1), // ry = "foo1"
                StorageValueT::Id32(foo2), // y = "foo2"
                StorageValueT::Id32(a),    // rx = "a"
                StorageValueT::Id32(foo2), // ry = "foo2"
                StorageValueT::Id32(b),    // x = "b"
                StorageValueT::Id32(bar),  // y = "bar"
                StorageValueT::Id32(b),    // rx = "b"
                StorageValueT::Id32(bar),  // ry = "bar"
                StorageValueT::Int64(42),  // y = 42
                StorageValueT::Id32(b),    // rx = "b"
                StorageValueT::Int64(42),  // ry = 42
            ],
        );
    }

    #[test]
    fn function_new_value() {
        let mut dictionary = Dict::default();
        let hello = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("hello: ")))
            .value() as u32;
        let world = dictionary
            .add_datavalue(AnyDataValue::new_plain_string(String::from("world: ")))
            .value() as u32;
        let dictionary = RefCell::new(dictionary);

        let trie = Trie::from_rows(vec![
            vec![StorageValueT::Int64(10), StorageValueT::Id32(hello)],
            vec![StorageValueT::Int64(20), StorageValueT::Id32(world)],
        ]);

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("int");
        marker_generator.add_marker("str");
        marker_generator.add_marker("con");

        let markers = marker_generator.operation_table(["int", "str", "con"].iter());
        let marker_int = *marker_generator.get(&"int").unwrap();
        let marker_str = *marker_generator.get(&"str").unwrap();
        let marker_con = *marker_generator.get(&"con").unwrap();

        let mut assigment = FunctionAssignment::new();
        assigment.insert(
            marker_con,
            FunctionTree::string_concatenation(
                FunctionTree::reference(marker_str),
                FunctionTree::canonical_string(FunctionTree::reference(marker_int)),
            ),
        );

        let function_generator = GeneratorFunction::new(markers, &assigment);
        let function_scan = function_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        let result = RowScan::new(function_scan, 0)
            .map(|row| {
                row.into_iter()
                    .map(|value| value.into_datavalue(&dictionary.borrow()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let expected = vec![
            vec![
                AnyDataValue::new_integer_from_i64(10),
                AnyDataValue::new_plain_string(String::from("hello: ")),
                AnyDataValue::new_plain_string(String::from(
                    "hello: \"10\"^^<http://www.w3.org/2001/XMLSchema#int>",
                )),
            ],
            vec![
                AnyDataValue::new_integer_from_i64(20),
                AnyDataValue::new_plain_string(String::from("world: ")),
                AnyDataValue::new_plain_string(String::from(
                    "world: \"20\"^^<http://www.w3.org/2001/XMLSchema#int>",
                )),
            ],
        ];

        assert_eq!(result, expected);
    }
}
