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
    },
    datavalues::AnyDataValue,
    function::{evaluation::StackProgram, tree::FunctionTree},
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationColumnMarker, OperationGenerator, OperationTable};

/// Assigns an output column to a function which determines how to compute the values of that column
pub type FunctionAssignment = HashMap<OperationColumnMarker, FunctionTree<OperationColumnMarker>>;

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
    pub fn new(output: OperationTable, functions: &FunctionAssignment) -> Self {
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
                if let Entry::Vacant(entry) = reference_map.entry(output_marker) {
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

    /// Returns whether this operation does not alter the input table.
    fn is_unchanging(&self) -> bool {
        // This operation behaves the same as the identity if
        // no new columns are computed
        self.output_columns
            .iter()
            .all(|c| matches!(c, OutputColumn::Input(_)))
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

        Some(TrieScanEnum::TrieScanFunction(TrieScanFunction {
            trie_scan: Box::new(trie_scan),
            dictionary,
            input_indices: self.input_indices.clone(),
            output_functions,
            input_values: Vec::new(),
            column_scans,
            path_types: Vec::new(),
        }))
    }
}

/// [PartialTrieScan] which introduced additional columns which contain the result
/// of evaluating provided functions over the columns of an input trie
#[derive(Debug)]
pub struct TrieScanFunction<'a> {
    /// Input trie scan the content of which is used for the definition of the new columns
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    dictionary: &'a RefCell<Dict>,

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
        let current_layer = self.path_types.len() - 1;
        let previous_layer = current_layer.checked_sub(1);

        if self.output_functions[current_layer].is_none() {
            // If the current output layer corresponds to a layer in the input trie,
            // we need to call up on that
            self.trie_scan.up();
        }

        if let Some(previous_layer) = previous_layer {
            if self.input_indices[previous_layer] {
                // The input value is no longer valid
                self.input_values.pop();
            }
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

                let column_value = self.column_scans[previous_layer].get_mut().current(previous_type).expect("It is only allowed to call down while the previous scan points to some value.").into_datavalue(&self.dictionary.borrow()).expect("All ids occuring in a column must be known to the dictionary");
                self.input_values.push(column_value);
            }
        }

        // There are two cases.
        // Either we enter a layer which simply corresponds to an input layer.
        // In this case we need to call down in the input trie.
        // Otherwise we need to compute the new value and pass it into the column scan of this layer.
        if let Some(program) = &self.output_functions[next_layer] {
            let function_result = program.evaluate_data(&self.input_values);
            match function_result
                .map(|result| result.to_storage_value_t_mut(&mut self.dictionary.borrow_mut()))
                .filter(|result| result.get_type() == next_type)
            {
                Some(result) => {
                    self.column_scans[next_layer].get_mut().constant_set(result);
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

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        if self.output_functions[layer].is_some() {
            StorageTypeBitSet::full()
        } else {
            let input_layer = self
                .output_functions
                .iter()
                .take(layer)
                .filter(|out| out.is_none())
                .count();

            self.trie_scan.possible_types(input_layer)
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
            .add_datavalue(AnyDataValue::new_string(String::from("a")))
            .value() as u32;
        let b = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("b")))
            .value() as u32;
        let foo1 = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("foo1")))
            .value() as u32;
        let foo2 = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("foo2")))
            .value() as u32;
        let bar = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("bar")))
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
            .add_datavalue(AnyDataValue::new_string(String::from("hello: ")))
            .value() as u32;
        let world = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("world: ")))
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
                AnyDataValue::new_string(String::from("hello: ")),
                AnyDataValue::new_string(String::from(
                    "hello: \"10\"^^<http://www.w3.org/2001/XMLSchema#int>",
                )),
            ],
            vec![
                AnyDataValue::new_integer_from_i64(20),
                AnyDataValue::new_string(String::from("world: ")),
                AnyDataValue::new_string(String::from(
                    "world: \"20\"^^<http://www.w3.org/2001/XMLSchema#int>",
                )),
            ],
        ];

        assert_eq!(result, expected);
    }
}
