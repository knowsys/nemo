use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{
    arithmetic::expression::{StackOperation, StackProgram, StackValue},
    columnar::{
        operations::{
            columnscan_empty::ColumnScanEmpty, ColumnScanArithmetic, ColumnScanConstant,
            ColumnScanCopy, ColumnScanNulls, ColumnScanPass,
        },
        traits::columnscan::ColumnScanEnum,
    },
    datatypes::{DataValueT, StorageTypeName, StorageValueT},
    management::database::Dict,
    tabular::{
        table_types::trie_rainbow::{ColumnScanRainbow, PartialTrieScanRainbow},
        traits::partial_trie_scan::TrieScanRainbowEnum,
    },
};

/// Enum which represents an instruction to modify a trie by appending certain columns.
#[derive(Debug, Clone)]
pub enum AppendInstruction {
    /// Add column which has the same entries as another existing column.
    /// The parameter is the index of the column that is to be repeated.
    RepeatColumn(usize),
    /// Add a column which only contains the constant given as a parameter.
    Constant(DataValueT),
    /// Add a column which results from performing a given mathematical operation
    /// based on existing columns.
    /// The parameter contains a [`StackProgram`] encoding the operation.
    Arithmetic(StackProgram<DataValueT>),
    /// Add columns evaluating to fresh nulls.
    Nulls,
}

/// TODO: Description
///
/// [`PartialTrieScan`] which appends columns to an existing [`PartialTrieScan`].
#[derive(Debug)]
pub struct TrieScanAppendRainbow<'a> {
    /// Trie scans to which new columns will be appended.
    trie_scan: Box<TrieScanRainbowEnum<'a>>,

    /// Layer we are currently at in the resulting trie.
    current_layer: Option<usize>,

    /// Layers in the current trie that point to layers in the base trie.
    /// I.e. which `column_scans` are pass scans.
    base_indices: Vec<usize>,
    /// Pointer into the `base_indices` member.
    base_pointer: usize,

    /// [`ColumnScanT`]s which represent each new layer in the resulting trie.
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`].
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> TrieScanAppendRainbow<'a> {
    /// The parameter `instructions` is a slice of [`AppendInstruction`] vectors.
    /// It is interpreted as follows:
    /// The ith entry in the slice contains what type of column is added into the ith column gap in the original trie.
    /// Example: Given a trie with columns labeled xyz and instructions
    /// [[Constant(2)], [RepeatColumn(0)], [Constant(3), Constant(4)], [RepeatColumn(1), Constant(1), RepeatColumn(2)]]
    /// results in a trie with "schema" 2xxy34zy1z
    pub fn new(
        dict: &mut Dict,
        trie_scan: TrieScanRainbowEnum<'a>,
        instructions: &[Vec<AppendInstruction>],
        nulls_start: u64,
    ) -> Self {
        let arity_source = trie_scan.arity();

        debug_assert_eq!(instructions.len(), arity_source + 1);

        let valid = |instructions: &[AppendInstruction], position: usize| -> bool {
            instructions.iter().all(|instruction| match instruction {
                AppendInstruction::RepeatColumn(index_repeat) => *index_repeat < position,
                AppendInstruction::Nulls => position == arity_source,
                AppendInstruction::Constant(_) => true,
                AppendInstruction::Arithmetic(_) => true,
            })
        };

        debug_assert!(instructions.iter().enumerate().all(|(i, v)| valid(v, i)));

        let mut result = Self {
            trie_scan: Box::new(trie_scan),
            current_layer: None,
            base_indices: Vec::new(),
            base_pointer: 0,
            column_scans: Vec::new(),
        };

        for (source_index, insert_instructions) in instructions.iter().enumerate() {
            let num_nulls = if source_index == arity_source {
                insert_instructions
                    .iter()
                    .filter(|i| matches!(i, AppendInstruction::Nulls))
                    .count()
            } else {
                0
            };

            let mut null_index = 0usize;
            for instruction in insert_instructions.iter() {
                match instruction {
                    AppendInstruction::RepeatColumn(repeat_index) => {
                        result.add_repeat_column(*repeat_index)
                    }
                    AppendInstruction::Constant(value) => result.add_constant_column(dict, value),
                    AppendInstruction::Arithmetic(arithmetic_tree) => {
                        result.add_arithmetic_column(arithmetic_tree.clone(), dict)
                    }
                    AppendInstruction::Nulls => {
                        result.add_null_column(nulls_start, null_index, num_nulls);
                        null_index += 1;
                    }
                }
            }

            if source_index < arity_source {
                result.add_transparent_column(source_index);
            }
        }

        result
    }

    fn add_null_column(&mut self, nulls_start: u64, null_index: usize, num_nulls: usize) {
        let null_scan = ColumnScanEnum::ColumnScanNulls(ColumnScanNulls::new(
            nulls_start + null_index as u64,
            num_nulls as u64,
        ));
        let scan_empty_integers = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());
        let scan_empty_doubles = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());

        let new_scan = ColumnScanRainbow::new(null_scan, scan_empty_integers, scan_empty_doubles);

        self.column_scans.push(UnsafeCell::new(new_scan));
    }

    fn add_arithmetic_column(
        &mut self,
        arithmetic_expression: StackProgram<DataValueT>,
        dict: &Dict,
    ) {
        let constant = arithmetic_expression
            .iter()
            .flat_map(|e| {
                if let StackOperation::Push(StackValue::Constant(value)) = e {
                    Some(value)
                } else {
                    None
                }
            })
            .last();

        macro_rules! arithmetic_for_datatype {
            ($variant:ident, $type:ty, $scan:ident) => {{
                let mut column_map = HashMap::<usize, usize>::new();
                let mut input_scans = Vec::new();

                let mut arithmetic_expression = arithmetic_expression.clone();

                for source_index in arithmetic_expression.references_mut() {
                    let base_scan =
                        &unsafe { &*self.trie_scan.scan(*source_index).unwrap().get() }.$scan;
                    let column_map_len = column_map.len();

                    match column_map.entry(*source_index) {
                        Entry::Occupied(entry) => {
                            *source_index = *entry.get();
                            continue;
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(column_map_len);
                            *source_index = column_map_len;
                        }
                    }

                    input_scans.push(base_scan);
                }

                let translate_type = |l: StackOperation<DataValueT>| match l {
                    StackOperation::Push(StackValue::Constant(t)) => {
                        if let StorageValueT::$variant(value) = t
                            .to_storage_value(dict)
                            .expect("We don't have string operations so this cannot fail.")
                        {
                            StackOperation::Push(StackValue::Constant(value))
                        } else {
                            unreachable!(
                                "Expected a operation tree value of type {}",
                                stringify!($src_name)
                            );
                        }
                    }
                    StackOperation::Push(StackValue::Reference(r)) => {
                        StackOperation::Push(StackValue::Reference(r))
                    }
                    StackOperation::UnaryOperation(op) => StackOperation::UnaryOperation(op),
                    StackOperation::BinaryOperation(op) => StackOperation::BinaryOperation(op),
                };

                let arithmetic_tree_type =
                    StackProgram::new(arithmetic_expression.into_iter().map(&translate_type))
                        .expect("Program was valid before");

                ColumnScanEnum::ColumnScanArithmetic(ColumnScanArithmetic::new(
                    input_scans,
                    arithmetic_tree_type,
                ))
            }};
        }

        let new_scan = if let Some(constant) = constant {
            let empty_scan_keys = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());
            let empty_scan_integers = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());
            let empty_scan_doubles = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());

            let (scan_keys, scan_integers, scan_doubles) = match constant {
                DataValueT::U64(_) => {
                    let scan_arithmetic_keys = arithmetic_for_datatype!(U64, u64, scan_keys);

                    (
                        scan_arithmetic_keys,
                        empty_scan_integers,
                        empty_scan_doubles,
                    )
                }
                DataValueT::I64(_) => {
                    let scan_arithmetic_integers =
                        arithmetic_for_datatype!(I64, i64, scan_integers);

                    (
                        empty_scan_keys,
                        scan_arithmetic_integers,
                        empty_scan_doubles,
                    )
                }
                DataValueT::Double(_) => {
                    let scan_arithmetic_doubles =
                        arithmetic_for_datatype!(Double, Double, scan_doubles);

                    (
                        empty_scan_keys,
                        empty_scan_integers,
                        scan_arithmetic_doubles,
                    )
                }
                DataValueT::String(_) => todo!(),
                DataValueT::U32(_) => todo!(),
                DataValueT::Float(_) => todo!(),
            };

            ColumnScanRainbow::new(scan_keys, scan_integers, scan_doubles)
        } else {
            let scan_keys = arithmetic_for_datatype!(U64, u64, scan_keys);
            let scan_integers = arithmetic_for_datatype!(I64, i64, scan_integers);
            let scan_doubles = arithmetic_for_datatype!(Double, Double, scan_doubles);

            ColumnScanRainbow::new(scan_keys, scan_integers, scan_doubles)
        };

        self.column_scans.push(UnsafeCell::new(new_scan));
    }

    fn add_transparent_column(&mut self, source_index: usize) {
        self.base_indices.push(self.column_scans.len());

        let base_scan_keys =
            &unsafe { &*self.trie_scan.scan(source_index).unwrap().get() }.scan_keys;
        let base_scan_integers =
            &unsafe { &*self.trie_scan.scan(source_index).unwrap().get() }.scan_integers;
        let base_scan_doubles =
            &unsafe { &*self.trie_scan.scan(source_index).unwrap().get() }.scan_doubles;

        // TODO: Think about whether pass scan is really needed
        let pass_scan_keys = ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(base_scan_keys));
        let pass_scan_integers =
            ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(base_scan_integers));
        let pass_scan_doubles =
            ColumnScanEnum::ColumnScanPass(ColumnScanPass::new(base_scan_doubles));

        let new_scan =
            ColumnScanRainbow::new(pass_scan_keys, pass_scan_integers, pass_scan_doubles);
        self.column_scans.push(UnsafeCell::new(new_scan));
    }

    fn add_repeat_column(&mut self, repeat_index: usize) {
        let referenced_scan_keys =
            &unsafe { &*self.trie_scan.scan(repeat_index).unwrap().get() }.scan_keys;
        let referenced_scan_integers =
            &unsafe { &*self.trie_scan.scan(repeat_index).unwrap().get() }.scan_integers;
        let referenced_scan_doubles =
            &unsafe { &*self.trie_scan.scan(repeat_index).unwrap().get() }.scan_doubles;

        let copy_scan_keys =
            ColumnScanEnum::ColumnScanCopy(ColumnScanCopy::new(referenced_scan_keys));
        let copy_scan_integers =
            ColumnScanEnum::ColumnScanCopy(ColumnScanCopy::new(referenced_scan_integers));
        let copy_scan_doubles =
            ColumnScanEnum::ColumnScanCopy(ColumnScanCopy::new(referenced_scan_doubles));

        let new_scan =
            ColumnScanRainbow::new(copy_scan_keys, copy_scan_integers, copy_scan_doubles);
        self.column_scans.push(UnsafeCell::new(new_scan));
    }

    fn add_constant_column(&mut self, dict: &mut Dict, value: &DataValueT) {
        let empty_scan_keys = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());
        let empty_scan_integers = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());
        let empty_scan_doubles = ColumnScanEnum::ColumnScanEmpty(ColumnScanEmpty::new());

        let (scan_keys, scan_integers, scan_doubles) = match value.to_storage_value_mut(dict) {
            StorageValueT::U64(value) => {
                let constant_scan_keys =
                    ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(value));

                (constant_scan_keys, empty_scan_integers, empty_scan_doubles)
            }
            StorageValueT::I64(value) => {
                let constant_scan_integers =
                    ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(value));

                (empty_scan_keys, constant_scan_integers, empty_scan_doubles)
            }
            StorageValueT::Double(value) => {
                let constant_scan_doubles =
                    ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new(value));

                (empty_scan_keys, empty_scan_integers, constant_scan_doubles)
            }
            StorageValueT::Float(_) => todo!(),
            StorageValueT::U32(_) => todo!(),
        };

        let new_scan = ColumnScanRainbow::new(scan_keys, scan_integers, scan_doubles);

        self.column_scans.push(UnsafeCell::new(new_scan));
    }
}

impl<'a> PartialTrieScanRainbow<'a> for TrieScanAppendRainbow<'a> {
    fn up(&mut self) {
        if self.current_layer.is_none() {
            return;
        }

        if self.base_pointer < self.base_indices.len()
            && self.base_indices[self.base_pointer] == self.current_layer.unwrap()
        {
            self.trie_scan.up();

            if self.trie_scan.current_scan().is_some() {
                self.base_pointer -= 1;
            }
        }

        self.current_layer = self.current_layer.and_then(|l| l.checked_sub(1));
    }

    fn down(&mut self, storage_type: StorageTypeName) {
        if !self.base_indices.is_empty() && self.current_layer.is_none() {
            self.trie_scan.down(storage_type);
        }

        if !self.base_indices.is_empty()
            && self.base_pointer < self.base_indices.len() - 1
            && self.current_layer.is_some()
            && self.base_indices[self.base_pointer] == self.current_layer.unwrap()
        {
            self.trie_scan.down(storage_type);
            self.base_pointer += 1;
        }

        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));
        debug_assert!(self.current_layer.unwrap() < self.arity());

        self.column_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset(storage_type);
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanRainbow<'a>> {
        Some(self.column_scans[self.current_layer?].get_mut())
    }

    fn scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>> {
        Some(&self.column_scans[index])
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }

    fn arity(&self) -> usize {
        self.column_scans.len()
    }
}

// #[cfg(test)]
// mod test {
//     use crate::{
//         arithmetic::expression::{self, StackProgram, StackValue},
//         columnar::traits::columnscan::ColumnScanT,
//         datatypes::{DataValueT, StorageTypeName},
//         management::database::Dict,
//         tabular::{
//             operations::triescan_append::{AppendInstruction, TrieScanAppend},
//             table_types::trie::{Trie, TrieScanGeneric},
//             traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum},
//         },
//         util::{make_column_with_intervals_t, test_util::make_column_with_intervals_int_t},
//     };

//     fn scan_next(int_scan: &mut TrieScanAppend) -> Option<u64> {
//         if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
//             rcs.next()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     fn scan_current(int_scan: &mut TrieScanAppend) -> Option<u64> {
//         if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
//             rcs.current()
//         } else {
//             panic!("type should be u64");
//         }
//     }

//     #[test]
//     fn test_constant_types() {
//         let columns_x = make_column_with_intervals_int_t(&[0, 3, 43], &[0]);
//         let trie = Trie::new(vec![columns_x]);

//         let trie_generic_scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

//         let mut dict = Dict::default();

//         let mut trie_append_scan = TrieScanAppend::new(
//             &mut dict,
//             trie_generic_scan,
//             &[
//                 vec![AppendInstruction::Constant(DataValueT::String(
//                     "Hello".to_string().into(),
//                 ))],
//                 vec![],
//             ],
//             vec![StorageTypeName::U64, StorageTypeName::I64],
//         );

//         trie_append_scan.down();
//         let column_scan = trie_append_scan.current_scan().unwrap();
//         let ColumnScanT::U64(_) = column_scan else {
//             panic!("wrong column type");
//         };
//         column_scan.next().unwrap();

//         trie_append_scan.down();
//         let column_scan = trie_append_scan.current_scan().unwrap();
//         let ColumnScanT::I64(_) = column_scan else {
//             panic!("wrong column type");
//         };
//     }

//     #[test]
//     fn test_constant() {
//         let column_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
//         let column_y = make_column_with_intervals_t(&[2, 4, 1, 5, 9], &[0, 2, 3]);
//         let column_z = make_column_with_intervals_t(&[5, 1, 7, 9, 3, 2, 4, 8], &[0, 1, 4, 5, 7]);

//         let trie = Trie::new(vec![column_x, column_y, column_z]);
//         let trie_generic = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

//         let mut dict = Dict::default();
//         let mut trie_iter = TrieScanAppend::new(
//             &mut dict,
//             trie_generic,
//             &[
//                 vec![AppendInstruction::Constant(DataValueT::U64(2))],
//                 vec![],
//                 vec![
//                     AppendInstruction::Constant(DataValueT::U64(3)),
//                     AppendInstruction::Constant(DataValueT::U64(4)),
//                 ],
//                 vec![AppendInstruction::Constant(DataValueT::U64(1))],
//             ],
//             vec![
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//             ],
//         );

//         assert!(scan_current(&mut trie_iter).is_none());

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(7));
//         assert_eq!(scan_current(&mut trie_iter), Some(7));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(9));
//         assert_eq!(scan_current(&mut trie_iter), Some(9));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(9));
//         assert_eq!(scan_current(&mut trie_iter), Some(9));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(8));
//         assert_eq!(scan_current(&mut trie_iter), Some(8));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));
//     }

//     #[test]
//     fn test_duplicates() {
//         let column_a = make_column_with_intervals_t(&[1, 2], &[0]);
//         let column_b = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);
//         let column_c = make_column_with_intervals_t(&[7, 8, 9, 6], &[0, 2, 3]);
//         let column_d = make_column_with_intervals_t(&[10, 11, 12, 13, 10, 10], &[0, 2, 4, 5]);
//         let column_e =
//             make_column_with_intervals_t(&[4, 5, 6, 7, 8, 9, 10, 11], &[0, 2, 4, 5, 6, 7]);

//         let trie = Trie::new(vec![column_a, column_b, column_c, column_d, column_e]);
//         let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

//         let mut dict = Dict::default();
//         let mut trie_iter = TrieScanAppend::new(
//             &mut dict,
//             trie_iter,
//             &[
//                 vec![],
//                 vec![AppendInstruction::RepeatColumn(0)],
//                 vec![],
//                 vec![],
//                 vec![AppendInstruction::RepeatColumn(1)],
//                 vec![],
//             ],
//             vec![
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//             ],
//         );

//         assert!(scan_current(&mut trie_iter).is_none());

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(7));
//         assert_eq!(scan_current(&mut trie_iter), Some(7));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(10));
//         assert_eq!(scan_current(&mut trie_iter), Some(10));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(11));
//         assert_eq!(scan_current(&mut trie_iter), Some(11));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(6));
//         assert_eq!(scan_current(&mut trie_iter), Some(6));
//         assert_eq!(scan_next(&mut trie_iter), Some(7));
//         assert_eq!(scan_current(&mut trie_iter), Some(7));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(8));
//         assert_eq!(scan_current(&mut trie_iter), Some(8));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(12));
//         assert_eq!(scan_current(&mut trie_iter), Some(12));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(8));
//         assert_eq!(scan_current(&mut trie_iter), Some(8));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(13));
//         assert_eq!(scan_current(&mut trie_iter), Some(13));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(9));
//         assert_eq!(scan_current(&mut trie_iter), Some(9));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(9));
//         assert_eq!(scan_current(&mut trie_iter), Some(9));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(10));
//         assert_eq!(scan_current(&mut trie_iter), Some(10));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(10));
//         assert_eq!(scan_current(&mut trie_iter), Some(10));

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(6));
//         assert_eq!(scan_current(&mut trie_iter), Some(6));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(10));
//         assert_eq!(scan_current(&mut trie_iter), Some(10));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(11));
//         assert_eq!(scan_current(&mut trie_iter), Some(11));
//     }

//     #[test]
//     fn triescan_arithmetic() {
//         let column_x = make_column_with_intervals_t(&[1, 2], &[0]);
//         let column_y = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);

//         let trie = Trie::new(vec![column_x, column_y]);
//         let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

//         // ((x + 3) * y) / x
//         // in stack notation: ref(0) 3 + ref(1) * ref(0) /
//         use expression::BinaryOperation::*;
//         use expression::StackOperation::*;
//         let expression = StackProgram::new([
//             Push(StackValue::Reference(0)),
//             Push(StackValue::Constant(DataValueT::U64(3))),
//             BinaryOperation(Addition),
//             Push(StackValue::Reference(1)),
//             BinaryOperation(Multiplication),
//             Push(StackValue::Reference(0)),
//             BinaryOperation(Division),
//         ])
//         .unwrap();

//         let mut dict = Dict::default();
//         let mut trie_iter = TrieScanAppend::new(
//             &mut dict,
//             trie_iter,
//             &[
//                 vec![],
//                 vec![],
//                 vec![AppendInstruction::Arithmetic(expression)],
//             ],
//             vec![
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//                 StorageTypeName::U64,
//             ],
//         );

//         assert!(scan_current(&mut trie_iter).is_none());

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(1));
//         assert_eq!(scan_current(&mut trie_iter), Some(1));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(3));
//         assert_eq!(scan_current(&mut trie_iter), Some(3));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(12));
//         assert_eq!(scan_current(&mut trie_iter), Some(12));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(4));
//         assert_eq!(scan_current(&mut trie_iter), Some(4));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(16));
//         assert_eq!(scan_current(&mut trie_iter), Some(16));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);

//         trie_iter.up();
//         assert_eq!(scan_next(&mut trie_iter), Some(2));
//         assert_eq!(scan_current(&mut trie_iter), Some(2));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(5));
//         assert_eq!(scan_current(&mut trie_iter), Some(5));

//         trie_iter.down();
//         assert!(scan_current(&mut trie_iter).is_none());
//         assert_eq!(scan_next(&mut trie_iter), Some(12));
//         assert_eq!(scan_current(&mut trie_iter), Some(12));
//         assert_eq!(scan_next(&mut trie_iter), None);
//         assert_eq!(scan_current(&mut trie_iter), None);
//     }
// }
