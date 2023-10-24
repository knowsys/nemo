use crate::{
    columnar::{
        operations::{
            arithmetic::expression::StackValue, columnscan_restrict_values::VALUE_SCAN_INDEX,
            condition::statement::ConditionStatement, ColumnScanEqualColumn, ColumnScanPass,
            ColumnScanRestrictValues,
        },
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{DataValueT, StorageTypeName, StorageValueT},
    management::database::Dict,
    tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum},
};
use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};
use std::{collections::HashSet, fmt::Debug};

/// [`SelectEqualClasses`] contains a vectors that indicate which column indices should be forced to the same value
/// E.g. for R(a, a, b, c, d, c) eq_classes = [[0, 1], [3, 5]]
/// We don't put single elements in a class and assume that entries in each class are sorted
pub type SelectEqualClasses = Vec<Vec<usize>>;

/// Trie iterator enforcing conditions which state that some columns should have the same value
#[derive(Debug)]
pub struct TrieScanSelectEqual<'a> {
    /// Base trie on which the filter is applied
    base_trie: Box<TrieScanEnum<'a>>,

    /// For each layer in the resulting trie, contains a [`ColumnScanEqualColumn`]
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`]
    select_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,
}

impl<'a> TrieScanSelectEqual<'a> {
    /// Construct new [`TrieScanSelectEqual`] object.
    /// Assumes that the members in each class are sorted and contains at least two elements
    pub fn new(base_trie: TrieScanEnum<'a>, eq_classes: &SelectEqualClasses) -> Self {
        debug_assert!(eq_classes.iter().all(|class| class.is_sorted()));
        debug_assert!(eq_classes.iter().all(|class| class.len() > 1));

        let column_types = base_trie.get_types();
        let arity = column_types.len();
        let mut select_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

        for (col_index, col_type) in column_types.iter().enumerate() {
            // As a default fill everything with [`ColScanPass`]
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        let base_scan_cell = if let ColumnScanT::$variant(base_scan) =
                            &*base_trie.get_scan(col_index).unwrap().get()
                        {
                            base_scan
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        };
                        let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanPass(
                            ColumnScanPass::new(base_scan_cell),
                        ));

                        select_scans.push(UnsafeCell::new(ColumnScanT::$variant(next_scan)));
                    }
                };
            }

            match col_type {
                StorageTypeName::U32 => init_scans_for_datatype!(U32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64),
                StorageTypeName::I64 => init_scans_for_datatype!(I64),
                StorageTypeName::Float => init_scans_for_datatype!(Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double),
            };
        }

        for class in eq_classes {
            // Each member in a equivalency class exept the first one will be represented
            // by a [`ColScanEqualColumn`] which references the previous column in that class
            for member_index in 1..class.len() {
                let prev_member_idx = class[member_index - 1];
                let current_member_idx = class[member_index];

                macro_rules! init_scans_for_datatype {
                    ($variant:ident) => {
                        unsafe {
                            let reference_scan_enum = if let ColumnScanT::$variant(ref_scan) =
                                &*base_trie.get_scan(prev_member_idx).unwrap().get()
                            {
                                ref_scan
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($variant));
                            };
                            let value_scan_enum = if let ColumnScanT::$variant(value_scan) =
                                &*base_trie.get_scan(current_member_idx).unwrap().get()
                            {
                                value_scan
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($variant));
                            };

                            let next_scan = ColumnScanCell::new(
                                ColumnScanEnum::ColumnScanEqualColumn(ColumnScanEqualColumn::new(
                                    reference_scan_enum,
                                    value_scan_enum,
                                )),
                            );

                            select_scans[current_member_idx] =
                                UnsafeCell::new(ColumnScanT::$variant(next_scan));
                        }
                    };
                }

                match column_types[current_member_idx] {
                    StorageTypeName::U32 => init_scans_for_datatype!(U32),
                    StorageTypeName::U64 => init_scans_for_datatype!(U64),
                    StorageTypeName::I64 => init_scans_for_datatype!(I64),
                    StorageTypeName::Float => init_scans_for_datatype!(Float),
                    StorageTypeName::Double => init_scans_for_datatype!(Double),
                }
            }
        }

        Self {
            base_trie: Box::new(base_trie),
            select_scans,
            current_layer: None,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanSelectEqual<'a> {
    fn up(&mut self) {
        self.current_layer = self
            .current_layer
            .expect("calling up only allowed after calling down")
            .checked_sub(1);

        self.base_trie.up();
    }

    fn down(&mut self) {
        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));
        debug_assert!(self.current_layer.unwrap() < self.get_types().len());

        self.base_trie.down();

        self.select_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.select_scans[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.select_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        self.base_trie.get_types()
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

/// Trie iterator enforcing conditions on the input trie expressed as lower and upper bounds
#[derive(Debug)]
pub struct TrieScanRestrictValues<'a> {
    /// Base trie on which the filter is applied
    base_trie: Box<TrieScanEnum<'a>>,

    /// For each layer in the resulting trie, contains a [`ColumnScanRestrictValues`]
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`]
    select_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,
}

impl<'a> TrieScanRestrictValues<'a> {
    /// Construct new TrieScanRestrictValues object.
    pub fn new(
        dict: &Dict,
        base_trie: TrieScanEnum<'a>,
        conditions: &[ConditionStatement<DataValueT>],
    ) -> Self {
        let column_types = base_trie.get_types();
        let arity = column_types.len();
        let mut select_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

        // Initialize `select_scans` with column scans
        // that just pass through values of the previous layer
        for (col_index, col_type) in column_types.iter().enumerate() {
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        let base_scan_enum = if let ColumnScanT::$variant(base_scan) =
                            &*base_trie.get_scan(col_index).unwrap().get()
                        {
                            base_scan
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        };
                        let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanPass(
                            ColumnScanPass::new(base_scan_enum),
                        ));

                        select_scans.push(UnsafeCell::new(ColumnScanT::$variant(next_scan)));
                    }
                };
            }

            match col_type {
                StorageTypeName::U32 => init_scans_for_datatype!(U32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64),
                StorageTypeName::I64 => init_scans_for_datatype!(I64),
                StorageTypeName::Float => init_scans_for_datatype!(Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double),
            }
        }

        // For each column in the input trie contains the set of column indices,
        // which are needed to evaluate the condition for this column
        let mut input_column_indices = vec![HashSet::<usize>::new(); arity];
        // For each column the conditions that need to be evaluated on that column
        let mut input_column_conditions =
            vec![Vec::<&ConditionStatement<DataValueT>>::new(); arity];

        // A condition is always applied in the layer that corresponds to
        // the maximum index referenced by the condition
        for condition in conditions {
            let maximum_reference = condition.maximum_reference().unwrap_or(0);

            for reference_index in condition.references() {
                input_column_indices[maximum_reference].insert(reference_index);
            }

            input_column_conditions[maximum_reference].push(condition);
        }

        for (column_index, (input_indices, input_conditions)) in input_column_indices
            .into_iter()
            .zip(input_column_conditions.iter())
            .enumerate()
        {
            if input_conditions.is_empty() {
                // No conditions apply to this column
                // So we just leave the Pass scans built above
                continue;
            }

            macro_rules! build_scans_for_datatype {
                ($variant:ident) => {{
                    // Scan whose values are being restricted
                    let scan_value = if let ColumnScanT::$variant(scan) =
                        unsafe { &*base_trie.get_scan(column_index).unwrap().get() }
                    {
                        scan
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($variant));
                    };

                    // Mapping from the indices used in the input_conditions
                    // that reference layers in the base trie
                    // to indices that reference the column scans that will
                    // be the input for the new scan
                    let mut map_index = HashMap::<usize, usize>::new();

                    // The index of the scan which will be restricted will have
                    // the special value `VALUE_SCAN_INDEX`
                    map_index.insert(column_index, VALUE_SCAN_INDEX);

                    // References to scans which serve as input for the new column scan
                    let mut scans_restriction = Vec::new();

                    for input_index in input_indices {
                        let map_index_len = map_index.len();

                        match map_index.entry(input_index) {
                            Entry::Occupied(_) => {}
                            Entry::Vacant(entry) => {
                                entry.insert(map_index_len);

                                let scan_restriction = if let ColumnScanT::$variant(scan) =
                                    unsafe { &*base_trie.get_scan(input_index).unwrap().get() }
                                {
                                    scan
                                } else {
                                    panic!("Expected a column scan of type {}", stringify!($variant));
                                };

                                scans_restriction.push(scan_restriction);
                            }
                        }
                    }

                    // Translates DataValueT into $type and the references according to map_index
                    let translate_type_index = |l: &StackValue<DataValueT>| match l {
                        StackValue::Constant(t) => {
                            if let StorageValueT::$variant(value) = t
                                .to_storage_value(dict)
                                .expect("We don't have string operations so this cannot fail.")
                            {
                                StackValue::Constant(value)
                            } else {
                                panic!(
                                    "Expected a operation tree value of type {}",
                                    stringify!($src_name)
                                );
                            }
                        }
                        StackValue::Reference(index) => {
                            StackValue::Reference(*map_index.get(&index).expect(
                                "Every input index should have been recorded while building the hash set.",
                            ))
                        }
                    };

                    let conditions = input_conditions
                        .into_iter()
                        .map(|c| ConditionStatement {
                            operation: c.operation,
                            lhs: c.lhs.map_values(&translate_type_index),
                            rhs: c.rhs.map_values(&translate_type_index),
                        })
                        .collect();

                    let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanRestrictValues(
                        ColumnScanRestrictValues::new(scan_value, scans_restriction, conditions),
                    ));

                    select_scans[column_index] = UnsafeCell::new(ColumnScanT::$variant(next_scan));
                }};
            }

            match column_types[column_index] {
                StorageTypeName::U32 => build_scans_for_datatype!(U32),
                StorageTypeName::U64 => build_scans_for_datatype!(U64),
                StorageTypeName::I64 => build_scans_for_datatype!(I64),
                StorageTypeName::Float => build_scans_for_datatype!(Float),
                StorageTypeName::Double => build_scans_for_datatype!(Double),
            }
        }

        Self {
            base_trie: Box::new(base_trie),
            select_scans,
            current_layer: None,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanRestrictValues<'a> {
    fn up(&mut self) {
        self.current_layer = self
            .current_layer
            .expect("calling up only allowed after calling down")
            .checked_sub(1);
        self.base_trie.up();
    }

    fn down(&mut self) {
        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));
        debug_assert!(self.current_layer.unwrap() < self.get_types().len());

        self.base_trie.down();

        self.select_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.select_scans[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.select_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        self.base_trie.get_types()
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

#[cfg(test)]
mod test {
    use super::{TrieScanRestrictValues, TrieScanSelectEqual};
    use crate::columnar::operations::arithmetic::expression::StackValue;
    use crate::columnar::operations::condition::statement::ConditionStatement;
    use crate::columnar::traits::columnscan::ColumnScanT;
    use crate::datatypes::DataValueT;
    use crate::management::database::Dict;
    use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum};
    use crate::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn select_eq_next(scan: &mut TrieScanSelectEqual) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.next()
        } else {
            panic!("Type should be u64");
        }
    }

    fn select_eq_current(scan: &mut TrieScanSelectEqual) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.current()
        } else {
            panic!("Type should be u64");
        }
    }

    fn restrict_val_next(scan: &mut TrieScanRestrictValues) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.next()
        } else {
            panic!("Type should be u64");
        }
    }

    fn restrict_val_current(scan: &mut TrieScanRestrictValues) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.current()
        } else {
            panic!("Type should be u64");
        }
    }

    #[test]
    fn test_select_equal() {
        let column_fst = make_column_with_intervals_t(&[1], &[0]);
        let column_snd = make_column_with_intervals_t(&[4, 5], &[0]);
        let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1], &[0, 3]);
        let column_fth = make_column_with_intervals_t(&[0, 4, 5, 3, 4, 6], &[0, 1, 3, 5]);

        let trie = Trie::new(vec![column_fst, column_snd, column_trd, column_fth]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut select_iter = TrieScanSelectEqual::new(trie_iter, &vec![vec![0, 2], vec![1, 3]]);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(4));
        assert_eq!(select_eq_current(&mut select_iter), Some(4));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(4));
        assert_eq!(select_eq_current(&mut select_iter), Some(4));
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), Some(5));
        assert_eq!(select_eq_current(&mut select_iter), Some(5));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), Some(1));
        assert_eq!(select_eq_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_eq_current(&mut select_iter), None);
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_eq_next(&mut select_iter), None);
        assert_eq!(select_eq_current(&mut select_iter), None);
    }

    #[test]
    fn trie_restrict_value_constant() {
        let column_fst = make_column_with_intervals_t(&[1], &[0]);
        let column_snd = make_column_with_intervals_t(&[4, 5], &[0]);
        let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1], &[0, 3]);
        let column_fth = make_column_with_intervals_t(&[7, 5, 7, 3, 4, 6], &[0, 1, 3, 5]);

        let trie = Trie::new(vec![column_fst, column_snd, column_trd, column_fth]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &dict,
            trie_iter,
            &[
                ConditionStatement::equal(
                    StackValue::Reference(1),
                    StackValue::Constant(DataValueT::U64(4)),
                ),
                ConditionStatement::equal(
                    StackValue::Reference(3),
                    StackValue::Constant(DataValueT::U64(7)),
                ),
            ],
        );

        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(1));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(1));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(4));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(4));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(0));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(0));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(1));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(1));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(2));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(2));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
    }

    #[test]
    fn trie_restrict_value_column() {
        let column_fst = make_column_with_intervals_t(&[1, 5, 8], &[0]);
        let column_snd = make_column_with_intervals_t(&[5, 2, 4, 7, 5], &[0, 1, 4]);

        let trie = Trie::new(vec![column_fst, column_snd]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &dict,
            trie_iter,
            &[ConditionStatement::less_than(
                StackValue::Reference(1),
                StackValue::Reference(0),
            )],
        );

        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(1));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(1));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(5));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(2));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(2));
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(4));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(4));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(8));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(8));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
    }

    #[test]
    fn trie_restrict_unequals() {
        let column_fst = make_column_with_intervals_t(&[1, 5, 8], &[0]);
        let column_snd = make_column_with_intervals_t(&[5, 2, 5, 7, 5, 8], &[0, 1, 4]);

        let trie = Trie::new(vec![column_fst, column_snd]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &dict,
            trie_iter,
            &[ConditionStatement::unequal(
                StackValue::Reference(1),
                StackValue::Reference(0),
            )],
        );

        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(1));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(1));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(5));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(2));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(2));
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(7));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        restrict_iter.up();
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(8));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(8));
        restrict_iter.down();
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
        assert_eq!(restrict_val_next(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_current(&mut restrict_iter), Some(5));
        assert_eq!(restrict_val_next(&mut restrict_iter), None);
        assert_eq!(restrict_val_current(&mut restrict_iter), None);
    }
}
