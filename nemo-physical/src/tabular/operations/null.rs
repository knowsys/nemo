//! This module defines [TrieScanNull] and [GeneratorNull].

use std::cell::{RefCell, UnsafeCell};

use crate::{
    columnar::{
        columnscan::{ColumnScanEnum, ColumnScanT},
        operations::{constant::ColumnScanConstant, pass::ColumnScanPass},
    },
    datavalues::ValueDomain,
    dictionary::DvDict,
    management::database::Dict,
    storagevalues::{storage_type_name::StorageTypeBitSet, StorageTypeName, StorageValueT},
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
pub(crate) struct GeneratorNull {
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
    pub(crate) fn new(output: OperationTable, input: OperationTable) -> Self {
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
                instructions.push(NullInstruction::RepeatNull(first_index - input.arity()));
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

        if self.instructions.is_empty() {
            return trie_scans.remove(0);
        }

        let trie_scan = trie_scans.remove(0)?;

        let arity = trie_scan.arity() + self.instructions.len();
        let mut column_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

        for layer in 0..trie_scan.arity() {
            macro_rules! pass_scan {
                ($type:ty, $scan:ident) => {{
                    let input_scan = &unsafe { &*trie_scan.scan(layer).get() }.$scan;

                    ColumnScanEnum::Pass(ColumnScanPass::new(input_scan))
                }};
            }

            let pass_scan_id32 = pass_scan!(u32, scan_id32);
            let pass_scan_id64 = pass_scan!(u64, scan_id64);
            let pass_scan_i64 = pass_scan!(i64, scan_i64);
            let pass_scan_float = pass_scan!(Float, scan_float);
            let pass_scan_double = pass_scan!(Double, scan_double);

            let new_scan = ColumnScanT::new(
                pass_scan_id32,
                pass_scan_id64,
                pass_scan_i64,
                pass_scan_float,
                pass_scan_double,
            );

            column_scans.push(UnsafeCell::new(new_scan));
        }

        for _ in &self.instructions {
            let scan_id32 = ColumnScanEnum::Constant(ColumnScanConstant::new(None));
            let scan_id64 = ColumnScanEnum::Constant(ColumnScanConstant::new(None));
            let scan_i64 = ColumnScanEnum::Constant(ColumnScanConstant::new(None));
            let scan_float = ColumnScanEnum::Constant(ColumnScanConstant::new(None));
            let scan_double = ColumnScanEnum::Constant(ColumnScanConstant::new(None));

            let new_scan =
                ColumnScanT::new(scan_id32, scan_id64, scan_i64, scan_float, scan_double);

            column_scans.push(UnsafeCell::new(new_scan));
        }

        Some(TrieScanEnum::Null(TrieScanNull {
            trie_scan: Box::new(trie_scan),
            dictionary,
            column_scans,
            instructions: self.instructions.clone(),
            current_layer: None,
            null_cache: Vec::with_capacity(self.instructions.len()),
        }))
    }
}

/// [PartialTrieScan] that appends columns containing fresh nulls to an in input [PartialTrieScan]
#[derive(Debug)]
pub(crate) struct TrieScanNull<'a> {
    /// Input trie scan
    trie_scan: Box<TrieScanEnum<'a>>,
    /// Dictionary used to create the fresh nulls
    dictionary: &'a RefCell<Dict>,

    /// What to do on each layer that outputs a null
    instructions: Vec<NullInstruction>,

    /// Stores all the created nulls in case a column wants to repeat the same value
    null_cache: Vec<StorageValueT>,

    /// Current layer of this [PartialTrieScan]
    current_layer: Option<usize>,

    /// For each layer in the resulting trie contains a [ColumnScanT]
    /// which either just pass the values from the input `trie_scan`
    /// or contain fresh nulls.
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanNull<'a> {
    fn up(&mut self) {
        if let Some(previous_layer) = self.current_layer() {
            if previous_layer < self.trie_scan.arity() {
                self.trie_scan.up();
            } else {
                self.null_cache.pop();
            }
        }

        self.current_layer = self
            .current_layer
            .expect("Cannot call PartialTrieScan::up when in starting position")
            .checked_sub(1);
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let next_layer = self.current_layer.map_or(0, |layer| layer + 1);

        debug_assert!(next_layer < self.arity());

        if next_layer < self.trie_scan.arity() {
            self.trie_scan.down(next_type);
        } else if next_type == StorageTypeName::Id32 {
            // Generation of the new null always happens when entering the Id32 type.
            // When the resulting null-id doesn't fit in 32bit, we write it in the Id64 column

            let instruction_index = next_layer - self.trie_scan.arity();
            let next_null = match self.instructions[instruction_index] {
                NullInstruction::FreshNull => {
                    let id = self.dictionary.borrow_mut().fresh_null_id() as u64;
                    if let Ok(id_u32) = u32::try_from(id) {
                        StorageValueT::Id32(id_u32)
                    } else {
                        StorageValueT::Id64(id)
                    }
                }
                NullInstruction::RepeatNull(null_index) => self.null_cache[null_index],
            };

            self.null_cache.push(next_null);

            self.column_scans[next_layer]
                .get_mut()
                .constant_set_none(StorageTypeName::Id32);
            self.column_scans[next_layer]
                .get_mut()
                .constant_set_none(StorageTypeName::Id64);

            self.column_scans[next_layer]
                .get_mut()
                .constant_set(next_null);
        }

        self.column_scans[next_layer].get_mut().reset(next_type);
        self.current_layer = Some(next_layer);
    }

    fn arity(&self) -> usize {
        self.trie_scan.arity() + self.instructions.len()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>> {
        &self.column_scans[layer]
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        if layer < self.trie_scan.arity() {
            self.trie_scan.possible_types(layer)
        } else {
            ValueDomain::Null.storage_type()
        }
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datavalues::{AnyDataValue, DataValue, ValueDomain},
        dictionary::DvDict,
        management::database::Dict,
        storagevalues::{into_datavalue::IntoDataValue, StorageValueT},
        tabular::{
            operations::{OperationGenerator, OperationTableGenerator},
            rowscan::RowScan,
            trie::Trie,
            triescan::TrieScanEnum,
        },
    };

    use super::GeneratorNull;

    #[test]
    fn triescan_null_basic() {
        let mut dictionary = Dict::default();
        let a = u32::try_from(
            dictionary
                .add_datavalue(AnyDataValue::new_plain_string(String::from("a")))
                .value(),
        )
        .expect("The dictionary should not immediately return large ids");
        let b = u32::try_from(
            dictionary
                .add_datavalue(AnyDataValue::new_plain_string(String::from("b")))
                .value(),
        )
        .expect("The dictionary should not immediately return large ids");
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

        let trie_scan = TrieScanEnum::Generic(trie.partial_iterator());

        let generator_null = GeneratorNull::new(markers_output, markers_input);
        let null_scan = generator_null
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        let result = RowScan::new(null_scan, 0)
            .map(|row| {
                row.into_iter()
                    .map(|value| value.into_datavalue(&dictionary.borrow()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0][0],
            AnyDataValue::new_plain_string(String::from("a"))
        );
        assert_eq!(result[0][1], AnyDataValue::new_integer_from_i64(12));
        assert_eq!(result[1][0], AnyDataValue::new_integer_from_i64(-12));
        assert_eq!(
            result[1][1],
            AnyDataValue::new_plain_string(String::from("b"))
        );

        for row in &result {
            for value in &row[2..5] {
                assert_eq!(value.value_domain(), ValueDomain::Null);
            }
        }

        assert_eq!(result[0][2], result[0][4]);
        assert_eq!(result[1][2], result[1][4]);

        assert_ne!(result[0][2], result[0][3]);
        assert_ne!(result[1][2], result[1][3]);
    }

    #[test]
    fn null_empty() {
        let dictionary = RefCell::new(Dict::default());

        let trie_zero = Trie::zero_arity(true);
        let trie_scan = TrieScanEnum::Generic(trie_zero.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("v");
        marker_generator.add_marker("w");

        let markers_input = marker_generator.operation_table([].iter());
        let markers_output = marker_generator.operation_table(["v", "w"].iter());

        let generator_null = GeneratorNull::new(markers_output, markers_input);
        let null_scan = generator_null
            .generate(vec![Some(trie_scan)], &dictionary)
            .unwrap();

        let result = RowScan::new(null_scan, 0)
            .map(|row| {
                row.into_iter()
                    .map(|value| value.into_datavalue(&dictionary.borrow()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 1);

        for value in &result[0] {
            assert_eq!(value.value_domain(), ValueDomain::Null);
        }

        let null_scan = generator_null.generate(vec![None], &dictionary);
        assert!(null_scan.is_none());
    }
}
