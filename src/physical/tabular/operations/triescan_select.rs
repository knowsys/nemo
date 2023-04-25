use crate::physical::{
    columnar::{
        operations::{ColumnScanEqualColumn, ColumnScanEqualValue, ColumnScanPass},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{DataValueT, StorageTypeName, StorageValueT},
    management::database::Dict,
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
    util::interval::{Interval, IntervalBound},
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

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

impl<'a> TrieScan<'a> for TrieScanSelectEqual<'a> {
    #[allow(clippy::unnecessary_lazy_evaluations)] // not actually
                                                   // unnecessary, as the subtraction might underflow
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());

        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));

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

/// Trie iterator enforcing conditions which state that some columns should have the same value
#[derive(Debug)]
pub struct TrieScanSelectValue<'a> {
    /// Base trie on which the filter is applied
    base_trie: Box<TrieScanEnum<'a>>,

    /// For each layer in the resulting trie, contains a [`ColumnScanEqualValue`]
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`]
    select_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,

    /// Layer we are currently at in the resulting trie
    current_layer: Option<usize>,
}

/// Struct representing the restriction of a column to a certain value
#[derive(Debug, Clone)]
pub struct ValueAssignment {
    /// Index of the column to which the value is assigned
    pub column_idx: usize,
    /// The interval assigned to the column
    pub interval: Interval<DataValueT>,
}

impl<'a> TrieScanSelectValue<'a> {
    /// Construct new TrieScanSelectValue object.
    pub fn new(
        dict: &mut Dict,
        base_trie: TrieScanEnum<'a>,
        assignments: &[ValueAssignment],
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
                StorageTypeName::Float => init_scans_for_datatype!(Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double),
            }
        }

        macro_rules! translate_interval_bound {
            ($variant:ident, $type:ty, $interval_bound:expr, $dict:expr) => {
                match $interval_bound {
                    IntervalBound::Inclusive(bound) => {
                        if let StorageValueT::$variant(value) = bound.to_storage_value(dict) {
                            IntervalBound::Inclusive(value)
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    }
                    IntervalBound::Exclusive(bound) => {
                        if let StorageValueT::$variant(value) = bound.to_storage_value(dict) {
                            IntervalBound::Exclusive(value)
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    }
                    IntervalBound::Unbounded => IntervalBound::Unbounded,
                }
            };
        }

        for assignment in assignments {
            macro_rules! init_scans_for_datatype {
                ($variant:ident,$type:ty) => {{
                    let lower = translate_interval_bound!(
                        $variant,
                        $type,
                        &assignment.interval.lower,
                        dict
                    );
                    let upper = translate_interval_bound!(
                        $variant,
                        $type,
                        &assignment.interval.upper,
                        dict
                    );
                    let interval = Interval::new(lower, upper);

                    let scan_enum = if let ColumnScanT::$variant(scan) =
                        unsafe { &*base_trie.get_scan(assignment.column_idx).unwrap().get() }
                    {
                        scan
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($variant));
                    };

                    let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanEqualValue(
                        ColumnScanEqualValue::new(scan_enum, interval),
                    ));

                    select_scans[assignment.column_idx] =
                        UnsafeCell::new(ColumnScanT::$variant(next_scan));
                }};
            }
            match column_types[assignment.column_idx] {
                StorageTypeName::U32 => init_scans_for_datatype!(U32, u32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64, u64),
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

impl<'a> TrieScan<'a> for TrieScanSelectValue<'a> {
    #[allow(clippy::unnecessary_lazy_evaluations)] // not actually
                                                   // unnecessary, as the subtraction might underflow
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());

        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));

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
    use super::{TrieScanSelectEqual, TrieScanSelectValue, ValueAssignment};
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::datatypes::DataValueT;
    use crate::physical::management::database::Dict;
    use crate::physical::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};
    use crate::physical::util::interval::Interval;
    use crate::physical::util::test_util::make_column_with_intervals_t;
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

    fn select_val_next(scan: &mut TrieScanSelectValue) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = scan.current_scan()? {
            rcs.next()
        } else {
            panic!("Type should be u64");
        }
    }

    fn select_val_current(scan: &mut TrieScanSelectValue) -> Option<u64> {
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
    fn test_select_value() {
        let column_fst = make_column_with_intervals_t(&[1], &[0]);
        let column_snd = make_column_with_intervals_t(&[4, 5], &[0]);
        let column_trd = make_column_with_intervals_t(&[0, 1, 2, 1], &[0, 3]);
        let column_fth = make_column_with_intervals_t(&[7, 5, 7, 3, 4, 6], &[0, 1, 3, 5]);

        let trie = Trie::new(vec![column_fst, column_snd, column_trd, column_fth]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut dict = Dict::default();
        let mut select_iter = TrieScanSelectValue::new(
            &mut dict,
            trie_iter,
            &[
                ValueAssignment {
                    column_idx: 1,
                    interval: Interval::single(DataValueT::U64(4)),
                },
                ValueAssignment {
                    column_idx: 3,
                    interval: Interval::single(DataValueT::U64(7)),
                },
            ],
        );
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), Some(1));
        assert_eq!(select_val_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), Some(4));
        assert_eq!(select_val_current(&mut select_iter), Some(4));
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), Some(0));
        assert_eq!(select_val_current(&mut select_iter), Some(0));
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), Some(7));
        assert_eq!(select_val_current(&mut select_iter), Some(7));
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_val_next(&mut select_iter), Some(1));
        assert_eq!(select_val_current(&mut select_iter), Some(1));
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), Some(7));
        assert_eq!(select_val_current(&mut select_iter), Some(7));
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_val_next(&mut select_iter), Some(2));
        assert_eq!(select_val_current(&mut select_iter), Some(2));
        select_iter.down();
        assert_eq!(select_val_current(&mut select_iter), None);
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
        select_iter.up();
        assert_eq!(select_val_next(&mut select_iter), None);
        assert_eq!(select_val_current(&mut select_iter), None);
    }
}
