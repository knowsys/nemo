use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap, VecDeque},
    ops::Range,
};

use crate::{
    arithmetic::expression::{StackOperation, StackProgram, StackValue},
    columnar::{
        column_builders::{columnbuilder::ColumnBuilder, columnbuilder_rle::ColumnBuilderRle},
        column_operations::{
            ColumnScanArithmetic, ColumnScanCast, ColumnScanCastEnum, ColumnScanConstant,
            ColumnScanCopy, ColumnScanPass,
        },
        column_storage::{
            column::{Column, ColumnEnum},
            column_rle::ColumnRle,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
            interval::{ColumnWithIntervals, ColumnWithIntervalsT},
        },
    },
    datatypes::{DataValueT, StorageTypeName, StorageValueT},
    generate_cast_statements,
    management::database::Dict,
    tabular::{
        table_types::trie::Trie,
        traits::{
            partial_trie_scan::{PartialTrieScan, TrieScanEnum},
            table::Table,
        },
    },
};
use std::num::NonZeroUsize;

/// Helper function which, given a continuous range, expands it in such a way
/// that all of the child nodes are covered as well.
/// This process is repeated the
fn expand_range(columns: &[ColumnWithIntervalsT], range: Range<usize>) -> Range<usize> {
    let mut current_range = range;
    for column in columns {
        let start = column.int_bounds(current_range.start).start;
        let end = if current_range.end >= column.int_len() {
            column.len()
        } else {
            column.int_bounds(current_range.end).start
        };

        current_range = start..end
    }

    current_range
}

/// Enum which represents an instruction to modify a trie by appending certain columns.
#[derive(Debug, Clone)]
pub enum AppendInstruction {
    /// Add column which has the same entries as another existing column.
    RepeatColumn(usize),
    /// Add a column which only contains a constant.
    /// Must contain schema information.
    /// In this case whether the constant is associated with a dict
    Constant(DataValueT),
    /// Add a column which results from performing a given mathematical operation
    /// based on existing columns.
    Arithmetic(StackProgram<DataValueT>),
}

/// Appends columns to an existing trie and returns the modified trie.
/// The parameter `instructions` is a slice of `AppendInstruction` vectors.
/// It is interpreted as follows:
/// The ith entry in the slice contains what type of column is added into the ith column gap in the original trie.
/// Example: Given a trie with columns labeled xyz and instructions
/// [[Constant(2)], [RepeatColumn(0)], [Constant(3), Constant(4)], [RepeatColumn(1), Constant(1), RepeatColumn(2)]]
/// results in a trie with "schema" 2xxy34zy1z
/// TODO: Maybe this version of append is not needed.
pub fn trie_append(
    dict: &mut Dict,
    mut trie: Trie,
    instructions: impl IntoIterator<Item = Vec<AppendInstruction>>,
) -> Trie {
    let mut new_columns = VecDeque::<ColumnWithIntervalsT>::new();

    for (gap_index, instructions) in instructions.into_iter().enumerate() {
        for instruction in instructions.into_iter().rev() {
            match instruction {
                AppendInstruction::RepeatColumn(repeat_index) => {
                    let referenced_column = trie.get_column(repeat_index);
                    let prev_column = trie.get_column(gap_index - 1);

                    macro_rules! append_column_for_datatype {
                        ($variant:ident, $type:ty) => {{
                            if let ColumnWithIntervalsT::Id64(reference_column_typed) =
                                referenced_column
                            {
                                let mut new_data_column = ColumnBuilderRle::<u64>::new();

                                for (value_index, value) in
                                    reference_column_typed.get_data_column().iter().enumerate()
                                {
                                    let expanded_range = expand_range(
                                        &trie.columns()[(repeat_index + 1)..gap_index],
                                        value_index..(value_index + 1),
                                    );

                                    new_data_column.add_repeated_value(value, expanded_range.len());
                                }

                                let new_interval_column = ColumnRle::range(
                                    0usize,
                                    1.into(),
                                    NonZeroUsize::new(prev_column.len())
                                        .expect("Tried to construct empty rle column."),
                                );

                                new_columns.push_front(ColumnWithIntervalsT::Id64(
                                    ColumnWithIntervals::new(
                                        ColumnEnum::ColumnRle(new_data_column.finalize()),
                                        ColumnEnum::ColumnRle(new_interval_column),
                                    ),
                                ));
                            } else {
                                panic!("Expected a column of type {}", stringify!($type));
                            }
                        }};
                    }

                    match trie.get_types()[repeat_index] {
                        StorageTypeName::Id32 => append_column_for_datatype!(Id32, u32),
                        StorageTypeName::Id64 => append_column_for_datatype!(Id64, u64),
                        StorageTypeName::Int64 => append_column_for_datatype!(Int64, i64),
                        StorageTypeName::Float => {
                            append_column_for_datatype!(Float, Float)
                        }
                        StorageTypeName::Double => {
                            append_column_for_datatype!(Double, Double)
                        }
                    };
                }
                AppendInstruction::Constant(value) => {
                    macro_rules! append_columns_for_datatype {
                        ($value:ident, $variant:ident, $type:ty) => {{
                            let target_length = gap_index
                                .checked_sub(1)
                                .map(|i| trie.get_column(i).len())
                                .unwrap_or(1);
                            let target_length = NonZeroUsize::new(target_length)
                                .expect("Tried to construct empty rle column.");

                            let new_data_column = ColumnRle::constant($value, target_length);
                            let new_interval_column =
                                ColumnRle::range(0usize, 1.into(), target_length);

                            new_columns.push_front(ColumnWithIntervalsT::$variant(
                                ColumnWithIntervals::new(
                                    ColumnEnum::ColumnRle(new_data_column),
                                    ColumnEnum::ColumnRle(new_interval_column),
                                ),
                            ));
                        }};
                    }

                    match value.to_storage_value_mut(dict) {
                        StorageValueT::Id32(value) => {
                            append_columns_for_datatype!(value, Id32, u32)
                        }
                        StorageValueT::Id64(value) => {
                            append_columns_for_datatype!(value, Id64, u64)
                        }
                        StorageValueT::Int64(value) => {
                            append_columns_for_datatype!(value, Int64, i64)
                        }
                        StorageValueT::Float(value) => {
                            append_columns_for_datatype!(value, Float, Float)
                        }
                        StorageValueT::Double(value) => {
                            append_columns_for_datatype!(value, Double, Double)
                        }
                    };
                }
                AppendInstruction::Arithmetic(_) => todo!(),
            }
        }

        if gap_index > 0 {
            new_columns.push_front(trie.columns_mut().pop().expect(
                "We only pop as many elements as there are columns, so the vector will never be empty",
            ));
        }
    }

    Trie::new(Vec::<ColumnWithIntervalsT>::from(new_columns))
}

/// [`PartialTrieScan`] which appends columns to an existing [`PartialTrieScan`].
#[derive(Debug)]
pub struct TrieScanAppend<'a> {
    /// Trie scans to which new columns will be appended.
    trie_scan: Box<TrieScanEnum<'a>>,

    /// Layer we are currently at in the resulting trie.
    current_layer: Option<usize>,

    /// Types of the resulting trie.
    target_types: Vec<StorageTypeName>,

    /// Layers in the current trie that point to layers in the base trie.
    /// I.e. which `column_scans` are pass scans.
    base_indices: Vec<usize>,
    /// Pointer into the `base_indices` member.
    base_pointer: usize,

    /// [`ColumnScanT`]s which represent each new layer in the resulting trie.
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`].
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanAppend<'a> {
    /// The parameter `instructions` is a slice of `AppendInstruction` vectors.
    /// It is interpreted as follows:
    /// The ith entry in the slice contains what type of column is added into the ith column gap in the original trie.
    /// Example: Given a trie with columns labeled xyz and instructions
    /// [[Constant(2)], [RepeatColumn(0)], [Constant(3), Constant(4)], [RepeatColumn(1), Constant(1), RepeatColumn(2)]]
    /// results in a trie with "schema" 2xxy34zy1z
    /// Note: This also receives type information. This is important because copied columns might require different types.
    pub fn new(
        dict: &mut Dict,
        trie_scan: TrieScanEnum<'a>,
        instructions: &[Vec<AppendInstruction>],
        target_types: Vec<StorageTypeName>,
    ) -> Self {
        let src_arity = trie_scan.get_types().len();

        debug_assert_eq!(instructions.len(), src_arity + 1);

        fn valid(instr: &[AppendInstruction], pos: usize) -> bool {
            instr.iter().all(|a| {
                let AppendInstruction::RepeatColumn(e) = a else {
                    return true;
                };
                *e <= pos
            })
        }

        debug_assert!(instructions.iter().enumerate().all(|(i, v)| valid(v, i)));
        debug_assert_eq!(
            target_types.len(),
            instructions.iter().map(|i| i.len()).sum::<usize>() + src_arity
        );

        let mut result = Self {
            trie_scan: Box::new(trie_scan),
            current_layer: None,
            target_types,
            base_indices: Vec::new(),
            base_pointer: 0,
            column_scans: Vec::new(),
        };

        let mut target_index = 0;
        for (src_index, insert_instructions) in instructions.iter().enumerate() {
            for instruction in insert_instructions.iter() {
                match instruction {
                    AppendInstruction::RepeatColumn(repeat_index) => {
                        result.add_repeat_column(*repeat_index)
                    }
                    AppendInstruction::Constant(value) => result.add_constant_column(dict, value),
                    AppendInstruction::Arithmetic(arithmetic_tree) => result.add_arithmetic_column(
                        arithmetic_tree.clone(),
                        result.target_types[target_index],
                        dict,
                    ),
                }

                target_index += 1;
            }

            if src_index < src_arity {
                result.add_backed_column(src_index);
                target_index += 1;
            }
        }

        result
    }

    fn add_arithmetic_column(
        &mut self,
        mut arithmetic_expression: StackProgram<DataValueT>,
        output_type: StorageTypeName,
        dict: &Dict,
    ) {
        macro_rules! input_for_datatype {
            ($variant:ident, $type:ty) => {{
                let mut column_map = HashMap::<usize, usize>::new();
                let mut input_scans = Vec::new();

                for src_index in arithmetic_expression.references_mut() {
                    let base_scan = unsafe { &*self.trie_scan.get_scan(*src_index).unwrap().get() };
                    let column_map_len = column_map.len();

                    match column_map.entry(*src_index) {
                        Entry::Occupied(entry) => {
                            *src_index = *entry.get();
                            continue;
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(column_map_len);
                            *src_index = column_map_len;
                        }
                    }

                    if let ColumnScanT::$variant(base_scan_cell) = base_scan {
                        input_scans.push(base_scan_cell);
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($src_name));
                    }
                }

                let translate_type = |l: StackOperation<DataValueT>| match l {
                    StackOperation::Push(StackValue::Constant(t)) => {
                        if let StorageValueT::$variant(value) = t
                            .to_storage_value(dict)
                            .expect("We don't have string operations so this cannot fail.")
                        {
                            StackOperation::Push(StackValue::Constant(value))
                        } else {
                            panic!(
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

                let new_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanArithmetic(
                    ColumnScanArithmetic::new(input_scans, arithmetic_tree_type),
                ));

                self.column_scans
                    .push(UnsafeCell::new(ColumnScanT::$variant(new_scan)));
            }};
        }

        match output_type {
            StorageTypeName::Id32 => input_for_datatype!(Id32, u32),
            StorageTypeName::Id64 => input_for_datatype!(Id64, u64),
            StorageTypeName::Int64 => input_for_datatype!(Int64, i64),
            StorageTypeName::Float => input_for_datatype!(Float, f32),
            StorageTypeName::Double => input_for_datatype!(Double, f64),
        }
    }

    fn add_backed_column(&mut self, src_index: usize) {
        self.base_indices.push(self.column_scans.len());
        let src_type = self.trie_scan.get_types()[src_index];
        let dst_type = self.target_types[self.column_scans.len()];

        let base_scan = unsafe { &*self.trie_scan.get_scan(src_index).unwrap().get() };

        macro_rules! append_pass_for_datatype {
            ($variant:ident) => {{
                if let ColumnScanT::$variant(base_scan_cell) = base_scan {
                    ColumnScanT::$variant(ColumnScanCell::new(ColumnScanEnum::ColumnScanPass(
                        ColumnScanPass::new(base_scan_cell),
                    )))
                } else {
                    panic!("Expected a column scan of type {}", stringify!($variant));
                }
            }};
        }

        let reference_scan = match self.trie_scan.get_types()[src_index] {
            StorageTypeName::Id32 => append_pass_for_datatype!(Id32),
            StorageTypeName::Id64 => append_pass_for_datatype!(Id64),
            StorageTypeName::Int64 => append_pass_for_datatype!(Int64),
            StorageTypeName::Float => append_pass_for_datatype!(Float),
            StorageTypeName::Double => append_pass_for_datatype!(Double),
        };

        if src_type == dst_type {
            self.column_scans.push(UnsafeCell::new(reference_scan));
        } else {
            macro_rules! cast_reference_scan {
                ($src_name:ident, $dst_name:ident, $src_type:ty, $dst_type:ty) => {{
                    let reference_scan_typed = if let ColumnScanT::$src_name(scan) = reference_scan
                    {
                        scan
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($type));
                    };

                    let new_scan = ColumnScanT::$dst_name(ColumnScanCell::new(
                        ColumnScanEnum::ColumnScanCast(ColumnScanCastEnum::$src_name(
                            ColumnScanCast::<$src_type, $dst_type>::new(reference_scan_typed),
                        )),
                    ));

                    self.column_scans.push(UnsafeCell::new(new_scan));
                }};
            }

            generate_cast_statements!(cast_reference_scan; src_type, dst_type);
        }
    }

    fn add_repeat_column(&mut self, repeat_index: usize) {
        let referenced_scan = unsafe { &*self.trie_scan.get_scan(repeat_index).unwrap().get() };

        macro_rules! append_repeat_for_datatype {
            ($variant:ident) => {{
                if let ColumnScanT::$variant(referenced_scan_cell) = referenced_scan {
                    self.column_scans
                        .push(UnsafeCell::new(ColumnScanT::$variant(ColumnScanCell::new(
                            ColumnScanEnum::ColumnScanCopy(ColumnScanCopy::new(
                                referenced_scan_cell,
                            )),
                        ))));
                }
            }};
        }

        match self.trie_scan.get_types()[repeat_index] {
            StorageTypeName::Id32 => append_repeat_for_datatype!(Id32),
            StorageTypeName::Id64 => append_repeat_for_datatype!(Id64),
            StorageTypeName::Int64 => append_repeat_for_datatype!(Int64),
            StorageTypeName::Float => append_repeat_for_datatype!(Float),
            StorageTypeName::Double => append_repeat_for_datatype!(Double),
        }
    }

    fn add_constant_column(&mut self, dict: &mut Dict, value: &DataValueT) {
        macro_rules! append_constant_for_datatype {
            ($variant:ident, $value: expr) => {{
                self.column_scans
                    .push(UnsafeCell::new(ColumnScanT::$variant(ColumnScanCell::new(
                        ColumnScanEnum::ColumnScanConstant(ColumnScanConstant::new($value)),
                    ))));
            }};
        }

        match value.to_storage_value_mut(dict) {
            StorageValueT::Id32(value) => append_constant_for_datatype!(Id32, value),
            StorageValueT::Id64(value) => append_constant_for_datatype!(Id64, value),
            StorageValueT::Int64(value) => append_constant_for_datatype!(Int64, value),
            StorageValueT::Float(value) => {
                append_constant_for_datatype!(Float, value)
            }
            StorageValueT::Double(value) => {
                append_constant_for_datatype!(Double, value)
            }
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanAppend<'a> {
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

    fn down(&mut self) {
        if !self.base_indices.is_empty() && self.current_layer.is_none() {
            self.trie_scan.down();
        }

        if !self.base_indices.is_empty()
            && self.base_pointer < self.base_indices.len() - 1
            && self.current_layer.is_some()
            && self.base_indices[self.base_pointer] == self.current_layer.unwrap()
        {
            self.trie_scan.down();
            self.base_pointer += 1;
        }

        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));
        debug_assert!(self.current_layer.unwrap() < self.get_types().len());

        self.column_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.column_scans[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.column_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.target_types
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

#[cfg(test)]
mod test {
    use crate::{
        arithmetic::expression::{self, StackProgram, StackValue},
        columnar::column_storage::columnscan::ColumnScanT,
        datatypes::{DataValueT, StorageTypeName},
        management::database::Dict,
        tabular::{
            operations::triescan_append::{AppendInstruction, TrieScanAppend},
            table_types::trie::{Trie, TrieScanGeneric},
            traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum},
        },
        util::{make_column_with_intervals_t, test_util::make_column_with_intervals_int_t},
    };

    fn scan_next(int_scan: &mut TrieScanAppend) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = int_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut TrieScanAppend) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = int_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_constant_types() {
        let columns_x = make_column_with_intervals_int_t(&[0, 3, 43], &[0]);
        let trie = Trie::new(vec![columns_x]);

        let trie_generic_scan = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut dict = Dict::default();

        let mut trie_append_scan = TrieScanAppend::new(
            &mut dict,
            trie_generic_scan,
            &[
                vec![AppendInstruction::Constant(DataValueT::String(
                    "Hello".to_string().into(),
                ))],
                vec![],
            ],
            vec![StorageTypeName::Id64, StorageTypeName::Int64],
        );

        trie_append_scan.down();
        let column_scan = trie_append_scan.current_scan().unwrap();
        let ColumnScanT::Id64(_) = column_scan else {
            panic!("wrong column type");
        };
        column_scan.next().unwrap();

        trie_append_scan.down();
        let column_scan = trie_append_scan.current_scan().unwrap();
        let ColumnScanT::Int64(_) = column_scan else {
            panic!("wrong column type");
        };
    }

    #[test]
    fn test_constant() {
        let column_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_y = make_column_with_intervals_t(&[2, 4, 1, 5, 9], &[0, 2, 3]);
        let column_z = make_column_with_intervals_t(&[5, 1, 7, 9, 3, 2, 4, 8], &[0, 1, 4, 5, 7]);

        let trie = Trie::new(vec![column_x, column_y, column_z]);
        let trie_generic = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut dict = Dict::default();
        let mut trie_iter = TrieScanAppend::new(
            &mut dict,
            trie_generic,
            &[
                vec![AppendInstruction::Constant(DataValueT::U64(2))],
                vec![],
                vec![
                    AppendInstruction::Constant(DataValueT::U64(3)),
                    AppendInstruction::Constant(DataValueT::U64(4)),
                ],
                vec![AppendInstruction::Constant(DataValueT::U64(1))],
            ],
            vec![
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
            ],
        );

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(8));
        assert_eq!(scan_current(&mut trie_iter), Some(8));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));
    }

    #[test]
    fn test_duplicates() {
        let column_a = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_b = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);
        let column_c = make_column_with_intervals_t(&[7, 8, 9, 6], &[0, 2, 3]);
        let column_d = make_column_with_intervals_t(&[10, 11, 12, 13, 10, 10], &[0, 2, 4, 5]);
        let column_e =
            make_column_with_intervals_t(&[4, 5, 6, 7, 8, 9, 10, 11], &[0, 2, 4, 5, 6, 7]);

        let trie = Trie::new(vec![column_a, column_b, column_c, column_d, column_e]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut dict = Dict::default();
        let mut trie_iter = TrieScanAppend::new(
            &mut dict,
            trie_iter,
            &[
                vec![],
                vec![AppendInstruction::RepeatColumn(0)],
                vec![],
                vec![],
                vec![AppendInstruction::RepeatColumn(1)],
                vec![],
            ],
            vec![
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
            ],
        );

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(10));
        assert_eq!(scan_current(&mut trie_iter), Some(10));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(11));
        assert_eq!(scan_current(&mut trie_iter), Some(11));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(6));
        assert_eq!(scan_current(&mut trie_iter), Some(6));
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(8));
        assert_eq!(scan_current(&mut trie_iter), Some(8));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(12));
        assert_eq!(scan_current(&mut trie_iter), Some(12));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(8));
        assert_eq!(scan_current(&mut trie_iter), Some(8));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(13));
        assert_eq!(scan_current(&mut trie_iter), Some(13));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(10));
        assert_eq!(scan_current(&mut trie_iter), Some(10));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(10));
        assert_eq!(scan_current(&mut trie_iter), Some(10));

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(6));
        assert_eq!(scan_current(&mut trie_iter), Some(6));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(10));
        assert_eq!(scan_current(&mut trie_iter), Some(10));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(11));
        assert_eq!(scan_current(&mut trie_iter), Some(11));
    }

    #[test]
    fn triescan_arithmetic() {
        let column_x = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_y = make_column_with_intervals_t(&[3, 4, 5], &[0, 2]);

        let trie = Trie::new(vec![column_x, column_y]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        // ((x + 3) * y) / x
        // in stack notation: ref(0) 3 + ref(1) * ref(0) /
        use expression::BinaryOperation::*;
        use expression::StackOperation::*;
        let expression = StackProgram::new([
            Push(StackValue::Reference(0)),
            Push(StackValue::Constant(DataValueT::U64(3))),
            BinaryOperation(Addition),
            Push(StackValue::Reference(1)),
            BinaryOperation(Multiplication),
            Push(StackValue::Reference(0)),
            BinaryOperation(Division),
        ])
        .unwrap();

        let mut dict = Dict::default();
        let mut trie_iter = TrieScanAppend::new(
            &mut dict,
            trie_iter,
            &[
                vec![],
                vec![],
                vec![AppendInstruction::Arithmetic(expression)],
            ],
            vec![
                StorageTypeName::Id64,
                StorageTypeName::Id64,
                StorageTypeName::Id64,
            ],
        );

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(12));
        assert_eq!(scan_current(&mut trie_iter), Some(12));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(16));
        assert_eq!(scan_current(&mut trie_iter), Some(16));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(12));
        assert_eq!(scan_current(&mut trie_iter), Some(12));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);
    }
}
