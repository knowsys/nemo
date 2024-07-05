//! This module defines [TrieScanFilter] and [GeneratorFilter].

use std::{
    cell::{RefCell, UnsafeCell},
    collections::{hash_map::Entry, HashMap, HashSet},
    rc::Rc,
};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanT},
        operations::{
            constant::ColumnScanConstant, filter::ColumnScanFilter,
            filter_constant::ColumnScanFilterConstant, pass::ColumnScanPass,
        },
    },
    datatypes::{
        into_datavalue::IntoDataValue, storage_type_name::StorageTypeBitSet, StorageTypeName,
        StorageValueT,
    },
    datavalues::AnyDataValue,
    function::{
        evaluation::StackProgram,
        tree::{FunctionTree, SpecialCaseFilter},
    },
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationColumnMarker, OperationGenerator, OperationTable};

/// Representation of a filter expression that can be used
/// to eliminate certain values from a table.
pub type Filter = FunctionTree<OperationColumnMarker>;
/// Collection of filters to be applied to a table
pub type Filters = Vec<Filter>;
/// Assigns an input column to a function which filters values from that column
type FilterAssignment = HashMap<OperationColumnMarker, FunctionTree<OperationColumnMarker>>;

/// Encodes how one particular output column is computed
#[derive(Debug, Clone)]
enum OutputColumn {
    /// Columns is just passed to the output
    Input,
    /// Column is filtered by a constant
    Constant(AnyDataValue),
    /// [StackProgram] for filtering values of the corresponding columns
    Filtered(StackProgram),
}

/// Used to create a [TrieScanFilter]
#[derive(Debug, Clone)]
pub(crate) struct GeneratorFilter {
    /// For each output column,
    /// marks whether it results from an input column or has a filter applied to it via a [StackProgram]
    output_columns: Vec<OutputColumn>,
    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
}

impl GeneratorFilter {
    /// Create a new [GeneratorFilter].
    pub(crate) fn new(input: OperationTable, filters: &Filters) -> Self {
        let filter_assignment = Self::compute_assignments(&input, filters);

        let mut output_columns = Vec::<OutputColumn>::new();
        let mut input_indices = Vec::<bool>::new();

        let referenced_columns: HashSet<OperationColumnMarker> = filter_assignment
            .iter()
            .flat_map(|(this, function)| {
                let mut references = function.references();
                references.retain(|marker| marker != this);
                references
            })
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

            match filter_assignment.get(&input_marker) {
                Some(function) => match function.special_filter() {
                    SpecialCaseFilter::Normal => {
                        output_columns.push(OutputColumn::Filtered(
                            StackProgram::from_function_tree(
                                function,
                                &reference_map,
                                Some(input_marker),
                            ),
                        ));
                    }
                    SpecialCaseFilter::Constant(_, constant) => {
                        output_columns.push(OutputColumn::Constant(constant.clone()));
                    }
                },
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

    /// Helper function that finds the [OperationColumnMarker] used in the given [Filter]
    /// that appears closest to the end of the given [OperationTable].
    ///
    /// Returns `None` if the given filter does not use any values as input.
    fn find_last_reference(
        table: &OperationTable,
        filter: &Filter,
    ) -> Option<OperationColumnMarker> {
        let references = filter.references();
        for marker in table.iter().rev() {
            if references.contains(marker) {
                return Some(*marker);
            }
        }

        None
    }

    /// Helper function that takes a list of boolean [Filter]s
    /// and constructs a [Filter] that represent its conjuction.
    fn fold_filters(filters: Vec<&Filter>) -> Filter {
        Filter::boolean_conjunction(filters.into_iter().cloned().collect::<Vec<_>>())
    }

    /// Compute the [FilterAssignment] from a list of [Filters].
    fn compute_assignments(input: &OperationTable, filters: &Filters) -> FilterAssignment {
        let mut grouped_filters = HashMap::<OperationColumnMarker, Vec<&Filter>>::new();

        for filter in filters {
            let marker = Self::find_last_reference(input, filter).unwrap_or(input[0]);
            grouped_filters.entry(marker).or_default().push(filter);
        }

        let mut result = FilterAssignment::new();
        for (marker, group_filters) in grouped_filters {
            result.insert(marker, Self::fold_filters(group_filters));
        }

        result
    }

    /// Returns whether this operation does not alter the input table.
    fn is_unchanging(&self) -> bool {
        // This operation behaves the same as the identity if
        // no new columns are computed
        self.output_columns
            .iter()
            .all(|output| matches!(output, OutputColumn::Input))
    }
}

impl OperationGenerator for GeneratorFilter {
    fn generate<'a>(
        &'_ self,
        mut trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        debug_assert!(trie_scans.len() == 1);

        let trie_scan = trie_scans.remove(0)?;
        let arity = trie_scan.arity();

        if self.is_unchanging() {
            return Some(trie_scan);
        }

        let mut column_scans: Vec<UnsafeCell<ColumnScanT<'a>>> =
            Vec::with_capacity(self.output_columns.len());

        let input_values = Rc::new(RefCell::new(Vec::<AnyDataValue>::new()));

        for (index, output_column) in self.output_columns.iter().enumerate() {
            macro_rules! output_scan {
                ($type:ty, $variant:ident, $scan:ident) => {{
                    match output_column {
                        OutputColumn::Filtered(program) => {
                            let input_scan = &unsafe { &*trie_scan.scan(index).get() }.$scan;
                            ColumnScanEnum::Filter(ColumnScanFilter::new(
                                input_scan,
                                program.clone(),
                                input_values.clone(),
                                dictionary,
                            ))
                        }
                        OutputColumn::Constant(constant) => {
                            let input_scan = &unsafe { &*trie_scan.scan(index).get() }.$scan;
                            if let StorageValueT::$variant(value) =
                                constant.to_storage_value_t(&dictionary.borrow())
                            {
                                ColumnScanEnum::FilterConstant(ColumnScanFilterConstant::new(
                                    input_scan, value,
                                ))
                            } else {
                                ColumnScanEnum::Constant(ColumnScanConstant::new(None))
                            }
                        }
                        OutputColumn::Input => {
                            let input_scan = &unsafe { &*trie_scan.scan(index).get() }.$scan;
                            ColumnScanEnum::Pass(ColumnScanPass::new(input_scan))
                        }
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

        Some(TrieScanEnum::Filter(TrieScanFilter {
            trie_scan: Box::new(trie_scan),
            dictionary,
            input_indices: self.input_indices.clone(),
            input_values,
            column_scans,
            path_types: Vec::with_capacity(arity),
        }))
    }
}

/// [PartialTrieScan] which filters values of an input [PartialTrieScan]
#[derive(Debug)]
pub(crate) struct TrieScanFilter<'a> {
    /// Input trie scan which will be filtered
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to translate column values in [AnyDataValue] for evaluation
    dictionary: &'a RefCell<Dict>,

    /// Marks for each output index,
    /// whether the value of the corresponding layer is used as input to some function.
    input_indices: Vec<bool>,
    /// Values that will be used as input for evaluating
    input_values: Rc<RefCell<Vec<AnyDataValue>>>,
    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,

    /// For each layer in the resulting trie contains a [ColumnScanT]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanFilter<'a> {
    fn up(&mut self) {
        let previous_layer = self.path_types.len() - 1;
        let next_layer = previous_layer.checked_sub(1);

        if let Some(layer) = next_layer {
            if self.input_indices[layer] {
                // The input value is no longer valid
                self.input_values.borrow_mut().pop();
            }
        }

        self.trie_scan.up();
        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let previous_layer = self.current_layer();
        let previous_type = self.path_types.last();

        let next_layer = previous_layer.map_or(0, |layer| layer + 1);

        if let Some((previous_layer, previous_type)) = previous_layer.zip(previous_type) {
            if self.input_indices[previous_layer] {
                // This value will be used in some future layer as an input to a function,
                // so we translate it to an AnyDataValue and store it in `self.input_values`.
                let column_value = self.column_scans[previous_layer].get_mut().current(*previous_type).expect("It is only allowed to call down while the previous scan points to some value.").into_datavalue(&self.dictionary.borrow()).expect("All ids occuring in a column must be known to the dictionary");
                self.input_values.borrow_mut().push(column_value);
            }
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
        datavalues::AnyDataValue,
        dictionary::meta_dv_dict::MetaDvDictionary,
        tabular::{
            operations::{OperationGenerator, OperationTableGenerator},
            triescan::TrieScanEnum,
        },
        util::test_util::test::{trie_dfs, trie_int64},
    };

    use super::{Filter, GeneratorFilter};

    #[test]
    fn filter_equal() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![
            &[1, 4, 0, 0],
            &[1, 4, 1, 4],
            &[1, 4, 1, 5],
            &[1, 4, 2, 3],
            &[1, 4, 2, 4],
            &[1, 5, 1, 6],
        ]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");
        marker_generator.add_marker("v");

        let markers = marker_generator.operation_table(["x", "y", "z", "v"].iter());

        let filters = vec![
            Filter::equals(
                Filter::reference(*marker_generator.get(&"x").unwrap()),
                Filter::reference(*marker_generator.get(&"z").unwrap()),
            ),
            Filter::equals(
                Filter::reference(*marker_generator.get(&"y").unwrap()),
                Filter::reference(*marker_generator.get(&"v").unwrap()),
            ),
        ];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(1), // z = 1
                StorageValueT::Int64(4), // v = 4
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(1), // z = 1
            ],
        );
    }

    #[test]
    fn restrict_constant() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![
            &[1, 4, 0, 7],
            &[1, 4, 1, 5],
            &[1, 4, 1, 7],
            &[1, 4, 2, 3],
            &[1, 4, 2, 4],
            &[1, 5, 1, 6],
        ]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");
        marker_generator.add_marker("v");

        let markers = marker_generator.operation_table(["x", "y", "z", "v"].iter());

        let filters = vec![
            Filter::equals(
                Filter::reference(*marker_generator.get(&"y").unwrap()),
                Filter::constant(AnyDataValue::new_integer_from_i64(4)),
            ),
            Filter::equals(
                Filter::reference(*marker_generator.get(&"v").unwrap()),
                Filter::constant(AnyDataValue::new_integer_from_i64(7)),
            ),
        ];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(0), // z = 1
                StorageValueT::Int64(7), // v = 7
                StorageValueT::Int64(1), // z = 1
                StorageValueT::Int64(7), // v = 7
                StorageValueT::Int64(2), // z = 2
            ],
        );
    }

    #[test]
    fn filter_less_than() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![&[1, 5], &[5, 2], &[5, 4], &[5, 7], &[8, 5]]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");

        let markers = marker_generator.operation_table(["x", "y"].iter());

        let filters = vec![Filter::numeric_lessthan(
            Filter::reference(*marker_generator.get(&"y").unwrap()),
            Filter::reference(*marker_generator.get(&"x").unwrap()),
        )];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(5), // x = 5
                StorageValueT::Int64(2), // y = 2
                StorageValueT::Int64(4), // y = 4
                StorageValueT::Int64(8), // x = 8
                StorageValueT::Int64(5), // y = 5
            ],
        );
    }

    #[test]
    fn filter_unequal() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![&[1, 5], &[5, 2], &[5, 5], &[5, 7], &[8, 5], &[8, 8]]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");

        let markers = marker_generator.operation_table(["x", "y"].iter());

        let filters = vec![Filter::unequals(
            Filter::reference(*marker_generator.get(&"x").unwrap()),
            Filter::reference(*marker_generator.get(&"y").unwrap()),
        )];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(1), // x = 1
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(5), // x = 5
                StorageValueT::Int64(2), // y = 2
                StorageValueT::Int64(7), // y = 7
                StorageValueT::Int64(8), // x = 8
                StorageValueT::Int64(5), // y = 5
            ],
        );
    }

    #[test]
    fn filter_top() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![
            &[1, 2],
            &[1, 4],
            &[1, 5],
            &[2, 7],
            &[2, 9],
            &[3, 1],
            &[3, 4],
            &[7, 8],
            &[7, 10],
            &[12, 9],
            &[12, 18],
        ]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");

        let markers = marker_generator.operation_table(["x", "y"].iter());

        let filters = vec![Filter::numeric_greaterthan(
            Filter::reference(*marker_generator.get(&"x").unwrap()),
            Filter::constant(AnyDataValue::new_integer_from_i64(5)),
        )];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(7),  // x = 7
                StorageValueT::Int64(8),  // y = 8
                StorageValueT::Int64(10), // y = 10
                StorageValueT::Int64(12), // y = 12
                StorageValueT::Int64(9),  // y = 9
                StorageValueT::Int64(18), // y = 18
            ],
        );
    }

    #[test]
    fn filter_constant_repeat() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie = trie_int64(vec![
            &[2, 0, 0],
            &[2, 0, 1],
            &[2, 5, 2],
            &[2, 2, 9],
            &[3, 3, 7],
            &[8, 5, 8],
        ]);

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");

        let markers = marker_generator.operation_table(["x", "y", "z"].iter());

        let filters = vec![
            Filter::equals(
                Filter::reference(*marker_generator.get(&"y").unwrap()),
                Filter::constant(AnyDataValue::new_integer_from_i64(5)),
            ),
            Filter::equals(
                Filter::reference(*marker_generator.get(&"x").unwrap()),
                Filter::reference(*marker_generator.get(&"z").unwrap()),
            ),
        ];

        let filter_generator = GeneratorFilter::new(markers, &filters);
        let mut filter_scan = filter_generator
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut filter_scan,
            &[StorageTypeName::Int64],
            &[
                StorageValueT::Int64(2), // x = 7
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(2), // z = 10
                StorageValueT::Int64(3), // x = 3
                StorageValueT::Int64(8), // x = 8
                StorageValueT::Int64(5), // y = 5
                StorageValueT::Int64(8), // z = 5
            ],
        );
    }
}
