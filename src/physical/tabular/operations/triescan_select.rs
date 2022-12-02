use crate::physical::{
    columnar::{
        operations::{ColumnScanEqualColumn, ColumnScanEqualValue, ColumnScanPass},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::{DataTypeName, DataValueT},
    tabular::table_types::trie::TrieSchema,
    tabular::traits::{
        table_schema::TableSchema,
        triescan::{TrieScan, TrieScanEnum},
    },
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

        let target_schema = base_trie.get_schema();
        let arity = target_schema.arity();
        let mut select_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

        for col_index in 0..arity {
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

            match target_schema.get_type(col_index) {
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
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

                match target_schema.get_type(current_member_idx) {
                    DataTypeName::U64 => init_scans_for_datatype!(U64),
                    DataTypeName::Float => init_scans_for_datatype!(Float),
                    DataTypeName::Double => init_scans_for_datatype!(Double),
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
        debug_assert!(self.current_layer.unwrap() < self.get_schema().arity());

        self.base_trie.down();

        self.select_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.select_scans[index])
    }

    fn get_schema(&self) -> &TrieSchema {
        self.base_trie.get_schema()
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
#[derive(Debug, Copy, Clone)]
pub struct ValueAssignment {
    /// Index of the column to which the value is assigned
    pub column_idx: usize,
    /// The value assigned to the column
    pub value: DataValueT,
}

impl<'a> TrieScanSelectValue<'a> {
    /// Construct new TrieScanSelectValue object.
    pub fn new(base_trie: TrieScanEnum<'a>, assignemnts: &[ValueAssignment]) -> Self {
        let target_schema = base_trie.get_schema();
        let arity = target_schema.arity();
        let mut select_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity);

        for col_index in 0..arity {
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

            match target_schema.get_type(col_index) {
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
            }
        }

        for assignemnt in assignemnts {
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        let value = if let DataValueT::$variant(value) = assignemnt.value {
                            value
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        };

                        let scan_enum = if let ColumnScanT::$variant(scan) =
                            &*base_trie.get_scan(assignemnt.column_idx).unwrap().get()
                        {
                            scan
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        };

                        let next_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanEqualValue(
                            ColumnScanEqualValue::new(scan_enum, value),
                        ));

                        select_scans[assignemnt.column_idx] =
                            UnsafeCell::new(ColumnScanT::$variant(next_scan));
                    }
                };
            }
            match target_schema.get_type(assignemnt.column_idx) {
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
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
        debug_assert!(self.current_layer.unwrap() < self.get_schema().arity());

        self.base_trie.down();

        self.select_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.select_scans[index])
    }

    fn get_schema(&self) -> &TrieSchema {
        self.base_trie.get_schema()
    }
}

#[cfg(test)]
mod test {
    use super::{TrieScanSelectEqual, TrieScanSelectValue, ValueAssignment};
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::datatypes::{DataTypeName, DataValueT};
    use crate::physical::tabular::table_types::trie::{
        Trie, TrieScanGeneric, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn select_eq_next(scan: &mut TrieScanSelectEqual) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("Type should be u64");
        }
    }

    fn select_eq_current(scan: &mut TrieScanSelectEqual) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
            rcs.current()
        } else {
            panic!("Type should be u64");
        }
    }

    fn select_val_next(scan: &mut TrieScanSelectValue) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("Type should be u64");
        }
    }

    fn select_val_current(scan: &mut TrieScanSelectValue) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*scan.current_scan()?.get()) } {
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
        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 3,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_fst, column_snd, column_trd, column_fth]);
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
        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 3,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_fst, column_snd, column_trd, column_fth]);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut select_iter = TrieScanSelectValue::new(
            trie_iter,
            &vec![
                ValueAssignment {
                    column_idx: 1,
                    value: DataValueT::U64(4),
                },
                ValueAssignment {
                    column_idx: 3,
                    value: DataValueT::U64(7),
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
