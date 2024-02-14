//! This module defines [TrieScanNull] and [GeneratorNull].

use std::cell::{RefCell, UnsafeCell};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanRainbow},
        operations::{constant::ColumnScanConstant, pass::ColumnScanPass},
    },
    datatypes::{storage_type_name::StorageTypeBitSet, StorageTypeName, StorageValueT},
    dictionary::DvDict,
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::{OperationGenerator, OperationTable};

/// Going down [TrieScanNull], in each layer,
/// one can either create a new null or repeat a previously created null.
#[derive(Debug, Clone)]
enum NullInstruction {
    /// Create a new fresh nulls
    FreshNull,
    /// Repeat previously created null.
    ///
    /// The value references
    RepeatNull(usize),
}

/// Used to create a [TrieScanNull]
#[derive(Debug, Clone)]
pub struct GeneratorNull {
    /// What to do on each layer that outputs a null
    instructions: Vec<NullInstruction>,
}

impl GeneratorNull {
    /// Create a new [GeneratorNull].
    ///
    /// Assumes that `output` is equal to `input`,
    /// except for potentially added columns at the end.
    /// When evaluating the resulting [TrieScanNull],
    /// those columns will result in fresh nulls.
    /// Markers associated with fresh nulls must be different from
    /// any markers in `input`.
    ///
    /// Additional markers in `output` may repeat.
    /// In this case the fresh null will appear multiple times.
    pub fn new(output: OperationTable, input: OperationTable) -> Self {
        debug_assert!(output.arity() >= input.arity());
        debug_assert!(input
            .iter()
            .zip(output.iter())
            .all(|(input_marker, output_marker)| input_marker == output_marker));
        debug_assert!(output
            .iter()
            .skip(input.arity())
            .all(|marker| input.position(marker).is_none()));

        let mut instructions = Vec::new();

        for (index, marker) in output.iter().enumerate().skip(input.arity()) {
            let first_index = output
                .position(marker)
                .expect("Values of iterators must appear somewhere");

            if first_index == index {
                instructions.push(NullInstruction::FreshNull);
            } else {
                instructions.push(NullInstruction::RepeatNull(first_index));
            }
        }

        Self { instructions }
    }
}

impl OperationGenerator for GeneratorNull {
    fn generate<'a>(
        &'_ self,
        mut trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        debug_assert!(trie_scans.len() == 1);

        // We return `None` if we do not add anything
        if self.instructions.is_empty() {
            return None;
        }

        let trie_scan = trie_scans.remove(0)?;

        let arity = trie_scan.arity() + self.instructions.len();
        let mut column_scans = Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(arity);

        for layer in 0..trie_scan.arity() {
            macro_rules! pass_scan {
                ($type:ty, $scan:ident) => {{
                    let input_scan = &unsafe { &*trie_scan.scan(layer).get() }.$scan;

                    ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(input_scan))
                }};
            }

            let pass_scan_id32 = pass_scan!(u32, scan_id32);
            let pass_scan_id64 = pass_scan!(u64, scan_id64);
            let pass_scan_i64 = pass_scan!(i64, scan_i64);
            let pass_scan_float = pass_scan!(Float, scan_float);
            let pass_scan_double = pass_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                pass_scan_id32,
                pass_scan_id64,
                pass_scan_i64,
                pass_scan_float,
                pass_scan_double,
            );

            column_scans.push(UnsafeCell::new(new_scan));
        }

        for _ in &self.instructions {
            let scan_id32 = ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None));
            let scan_id64 = ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None));
            let scan_i64 = ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None));
            let scan_float = ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None));
            let scan_double = ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(None));

            let new_scan =
                ColumnScanRainbow::new(scan_id32, scan_id64, scan_i64, scan_float, scan_double);

            column_scans.push(UnsafeCell::new(new_scan));
        }

        Some(TrieScanEnum::TrieScanNull(TrieScanNull {
            trie_scan: Box::new(trie_scan),
            dictionary,
            column_scans,
            instructions: self.instructions.clone(),
            path_types: Vec::new(),
        }))
    }
}

/// [PartialTrieScan] that appends columns containing fresh nulls to an in input [PartialTrieScan]
#[derive(Debug)]
pub struct TrieScanNull<'a> {
    /// Input trie scan
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to create the fresh nulls
    dictionary: &'a RefCell<Dict>,

    /// What to do on each layer that outputs a null
    instructions: Vec<NullInstruction>,

    /// For each layer in the resulting trie contains a [ColumnScanRainbow]
    /// which either just pass the values from the input `trie_scan`
    /// or contain fresh nulls.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,

    /// Path of [StorageTypeName] indicating the the types of the current (partial) row
    path_types: Vec<StorageTypeName>,
}

impl<'a> PartialTrieScan<'a> for TrieScanNull<'a> {
    fn up(&mut self) {
        if let Some(current_layer) = self.current_layer() {
            if current_layer < self.trie_scan.arity() {
                self.trie_scan.up();
            }
        }

        self.path_types.pop();
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let next_layer = self.path_types.len();

        debug_assert!(next_layer < self.arity());

        if next_layer < self.trie_scan.arity() {
            self.trie_scan.down(next_type);
        } else {
            // We store null in Id64 section
            if next_type != StorageTypeName::Id64 {
                self.column_scans[next_layer]
                    .get_mut()
                    .constant_set_none(next_type);
            } else {
                let instruction_index = next_layer - self.trie_scan.arity();
                let next_null = match self.instructions[instruction_index] {
                    NullInstruction::FreshNull => {
                        self.dictionary.borrow_mut().fresh_null_id() as u64
                    }
                    NullInstruction::RepeatNull(null_index) => {
                        if let Some(StorageValueT::Id64(null)) = self.column_scans[null_index]
                            .get_mut()
                            .current(StorageTypeName::Id64)
                        {
                            null
                        } else {
                            unreachable!("Null must have been set during the previous layer");
                        }
                    }
                };

                self.column_scans[next_layer]
                    .get_mut()
                    .constant_set(StorageValueT::Id64(next_null));
            }
        }

        self.column_scans[next_layer].get_mut().reset(next_type);
        self.path_types.push(next_type);
    }

    fn path_types(&self) -> &[StorageTypeName] {
        &self.path_types
    }

    fn arity(&self) -> usize {
        self.trie_scan.arity() + self.instructions.len()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }

    fn possible_types(&self, _layer: usize) -> StorageTypeBitSet {
        StorageTypeName::Id64.bitset()
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::{into_datavalue::IntoDataValue, StorageValueT},
        datavalues::{AnyDataValue, DataValue, ValueDomain},
        dictionary::DvDict,
        management::database::Dict,
        tabular::{
            operations::{prune::TrieScanPrune, OperationGenerator, OperationTableGenerator},
            trie::Trie,
            triescan::TrieScanEnum,
        },
    };

    use super::GeneratorNull;

    #[test]
    fn triescan_null_basic() {
        let mut dictionary = Dict::default();
        let a = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("a")))
            .value() as u32;
        let b = dictionary
            .add_datavalue(AnyDataValue::new_string(String::from("b")))
            .value() as u32;
        let dictionary = RefCell::new(dictionary);

        let trie = Trie::from_rows(vec![
            vec![StorageValueT::Id32(a), StorageValueT::Int64(12)],
            vec![StorageValueT::Int64(-12), StorageValueT::Id32(b)],
        ]);

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("v");
        marker_generator.add_marker("w");

        let markers_input = marker_generator.operation_table(["x", "y"].iter());
        let markers_output = marker_generator.operation_table(["x", "y", "v", "w", "v"].iter());

        let trie_scan = TrieScanEnum::TrieScanGeneric(trie.partial_iterator());

        let generator_null = GeneratorNull::new(markers_output, markers_input);
        let null_scan = generator_null
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        let result = Trie::from_trie_scan(TrieScanPrune::new(null_scan), 0)
            .row_iterator()
            .map(|row| {
                row.into_iter()
                    .map(|value| value.into_datavalue(&dictionary.borrow()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], AnyDataValue::new_string(String::from("a")));
        assert_eq!(result[0][1], AnyDataValue::new_integer_from_i64(12));
        assert_eq!(result[1][0], AnyDataValue::new_integer_from_i64(-12));
        assert_eq!(result[1][1], AnyDataValue::new_string(String::from("b")));

        for row in 0..1 {
            for column in 2..5 {
                assert_eq!(result[row][column].value_domain(), ValueDomain::Null);
            }
        }

        assert_eq!(result[0][2], result[0][4]);
        assert_eq!(result[1][2], result[1][4]);

        assert_ne!(result[0][2], result[0][3]);
        assert_ne!(result[1][2], result[1][3]);
    }
}
