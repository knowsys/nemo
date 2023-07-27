use crate::{
    columnar::{
        operations::{
            columnscan_restrict_values::{FilterBound, FilterValue},
            ColumnScanEqualColumn, ColumnScanPass, ColumnScanRestrictValues,
        },
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{DataValueT, Double, Float, StorageTypeName, StorageValueT},
    management::database::Dict,
    tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum},
};
use std::fmt::Debug;
use std::{cell::UnsafeCell, collections::HashMap};

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

/// Struct representing the restriction of a column to a certain values
/// bounded by the given lower and upper bounds.
#[derive(Debug, Default, Clone)]
pub struct ValueAssignment {
    /// List of lower bounds that the column must satisfy.
    pub lower_bounds: Vec<FilterBound<DataValueT>>,
    /// List of upper bounds that the column must satisfy.
    pub upper_bounds: Vec<FilterBound<DataValueT>>,
    /// List of values that the column must avoid.
    pub avoid_values: Vec<FilterValue<DataValueT>>,
}

impl<'a> TrieScanRestrictValues<'a> {
    /// Construct new TrieScanRestrictValues object.
    pub fn new(
        dict: &mut Dict,
        base_trie: TrieScanEnum<'a>,
        assignments: &HashMap<usize, ValueAssignment>,
    ) -> Self {
        let column_types = base_trie.get_types();
        let arity = column_types.len();
        let mut select_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

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

        macro_rules! translate_filter_value {
            ($variant:ident, $filter_value:expr, $dict:expr) => {
                match $filter_value {
                    FilterValue::Column(index) => FilterValue::Column(*index),
                    FilterValue::Constant(constant) => {
                        if let StorageValueT::$variant(constant_typed) =
                            constant.to_storage_value_mut($dict)
                        {
                            FilterValue::Constant(constant_typed)
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    }
                }
            };
        }

        macro_rules! translate_filter_bound {
            ($variant:ident, $filter_bound:expr, $dict:expr) => {
                match $filter_bound {
                    FilterBound::Inclusive(value) => {
                        FilterBound::Inclusive(translate_filter_value!($variant, value, $dict))
                    }
                    FilterBound::Exclusive(value) => {
                        FilterBound::Exclusive(translate_filter_value!($variant, value, $dict))
                    }
                }
            };
        }

        for (column_idx_value, assignment) in assignments {
            macro_rules! init_scans_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let scan_value = if let ColumnScanT::$variant(scan) =
                        unsafe { &*base_trie.get_scan(*column_idx_value).unwrap().get() }
                    {
                        scan
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($variant));
                    };

                    let mut column_map = HashMap::<usize, usize>::new();
                    let mut scans_restriction = Vec::new();
                    let mut lower_bounds: Vec<FilterBound<$type>> = assignment
                        .lower_bounds
                        .iter()
                        .map(|b| translate_filter_bound!($variant, b, dict))
                        .collect();
                    let mut upper_bounds: Vec<FilterBound<$type>> = assignment
                        .upper_bounds
                        .iter()
                        .map(|b| translate_filter_bound!($variant, b, dict))
                        .collect();
                    let mut avoid_values: Vec<FilterValue<$type>> = assignment
                        .avoid_values
                        .iter()
                        .map(|v| translate_filter_value!($variant, v, dict))
                        .collect();

                    for value in lower_bounds
                        .iter_mut()
                        .map(|b| b.value_mut())
                        .chain(upper_bounds.iter_mut().map(|b| b.value_mut()))
                        .chain(avoid_values.iter_mut())
                    {
                        if let Some(column_index) = value.column_index_mut() {
                            let map_len = column_map.len();
                            let mapped_index =
                                *column_map.entry(*column_index).or_insert_with(|| {
                                    let referenced_scan = if let ColumnScanT::$variant(scan) =
                                        unsafe { &*base_trie.get_scan(*column_index).unwrap().get() }
                                    {
                                        scan
                                    } else {
                                        panic!(
                                            "Expected a column scan of type {}",
                                            stringify!($variant)
                                        );
                                    };

                                    scans_restriction.push(referenced_scan);

                                    map_len
                                });

                            *column_index = mapped_index;
                        }
                    }

                    let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanRestrictValues(
                        ColumnScanRestrictValues::new(
                            scan_value,
                            scans_restriction,
                            lower_bounds,
                            upper_bounds,
                            avoid_values,
                        ),
                    ));

                    select_scans[*column_idx_value] =
                        UnsafeCell::new(ColumnScanT::$variant(next_scan));
                }};
            }

            match column_types[*column_idx_value] {
                StorageTypeName::U32 => init_scans_for_datatype!(U32, u32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64, u64),
                StorageTypeName::I64 => init_scans_for_datatype!(I64, i64),
                StorageTypeName::Float => init_scans_for_datatype!(Float, Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double, Double),
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
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::{TrieScanRestrictValues, TrieScanSelectEqual, ValueAssignment};
    use crate::columnar::operations::columnscan_restrict_values::{FilterBound, FilterValue};
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

        let mut dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &mut dict,
            trie_iter,
            &HashMap::from([
                (
                    1,
                    ValueAssignment {
                        lower_bounds: vec![FilterBound::Inclusive(FilterValue::Constant(
                            DataValueT::U64(4),
                        ))],
                        upper_bounds: vec![FilterBound::Inclusive(FilterValue::Constant(
                            DataValueT::U64(4),
                        ))],
                        avoid_values: vec![],
                    },
                ),
                (
                    3,
                    ValueAssignment {
                        lower_bounds: vec![FilterBound::Inclusive(FilterValue::Constant(
                            DataValueT::U64(7),
                        ))],
                        upper_bounds: vec![FilterBound::Inclusive(FilterValue::Constant(
                            DataValueT::U64(7),
                        ))],
                        avoid_values: vec![],
                    },
                ),
            ]),
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

        let mut dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &mut dict,
            trie_iter,
            &HashMap::from([(
                1,
                ValueAssignment {
                    lower_bounds: vec![],
                    upper_bounds: vec![FilterBound::Exclusive(FilterValue::Column(0))],
                    avoid_values: vec![],
                },
            )]),
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

        let mut dict = Dict::default();
        let mut restrict_iter = TrieScanRestrictValues::new(
            &mut dict,
            trie_iter,
            &HashMap::from([(
                1,
                ValueAssignment {
                    lower_bounds: vec![],
                    upper_bounds: vec![],
                    avoid_values: vec![FilterValue::Column(0)],
                },
            )]),
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
