use std::cell::UnsafeCell;

use crate::physical::{
    columnar::{
        operations::{ColumnScanNulls, ColumnScanPass},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::StorageTypeName,
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
};

/// [`TrieScan`] which appends columns with nulls at the end of another [`TrieScan`].
#[derive(Debug)]
pub struct TrieScanNulls<'a> {
    /// Trie scans to which new columns will be appended.
    trie_scan: Box<TrieScanEnum<'a>>,

    /// Layer we are currently at in the resulting trie.
    current_layer: Option<usize>,

    /// Types of the resulting trie.
    target_types: Vec<StorageTypeName>,

    /// [`ColumnScanT`]s which represent each new layer in the resulting trie.
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`].
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanNulls<'a> {
    /// Create new [`TrieScanNulls`] object.
    /// Receives as input the [`TrieScanEnum`] to which the nulls will be appended,
    /// the amount of null-columns as well as the value of the first new null.
    /// Null-columns are always appended to the end.
    pub fn new(trie_scan: TrieScanEnum<'a>, num_nulls: usize, nulls_start: u64) -> Self {
        let mut column_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::new();
        let mut target_types = trie_scan.get_types().clone();

        for scan_index in 0..trie_scan.get_types().len() {
            unsafe {
                let current_scan = &*trie_scan.get_scan(scan_index).unwrap().get();

                macro_rules! add_scan_for_datatype {
                    ($variant:ident, $type:ty) => {{
                        if let ColumnScanT::$variant(cs) = current_scan {
                            column_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                                ColumnScanCell::new(ColumnScanEnum::ColumnScanPass(
                                    ColumnScanPass::new(cs),
                                )),
                            )));
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($type));
                        }
                    }};
                }

                match trie_scan.get_types()[scan_index] {
                    StorageTypeName::U32 => add_scan_for_datatype!(U32, u32),
                    StorageTypeName::U64 => add_scan_for_datatype!(U64, u64),
                    StorageTypeName::Float => add_scan_for_datatype!(Float, Float),
                    StorageTypeName::Double => add_scan_for_datatype!(Double, Double),
                }
            }
        }

        // TODO: Should be revised after type system is completed
        for null_index in 0..num_nulls {
            target_types.push(StorageTypeName::U64);

            // To prevent different [`ColumnScanNulls`] from producing the same null
            // we offset the starting point an increment the null value by the amount of null columns
            column_scans.push(UnsafeCell::new(ColumnScanT::U64(ColumnScanCell::new(
                ColumnScanEnum::ColumnScanNulls(ColumnScanNulls::new(
                    nulls_start + null_index as u64,
                    num_nulls as u64,
                )),
            ))));
        }

        Self {
            trie_scan: Box::new(trie_scan),
            current_layer: None,
            target_types,
            column_scans,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanNulls<'a> {
    fn up(&mut self) {
        if self.current_layer.is_none() {
            return;
        }

        if self.current_layer.unwrap() < self.trie_scan.get_types().len() {
            self.trie_scan.up();
        }

        self.current_layer = self.current_layer.and_then(|l| l.checked_sub(1));
    }

    fn down(&mut self) {
        if self.trie_scan.get_types().len() > 0
            && (self.current_layer.is_none()
                || self.current_layer.unwrap() < self.trie_scan.get_types().len() - 1)
        {
            self.trie_scan.down();
        }

        self.current_layer = Some(self.current_layer.map_or(0, |v| v + 1));

        self.column_scans[self.current_layer.unwrap()]
            .get_mut()
            .reset();
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        self.get_scan(self.current_layer?)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.column_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.target_types
    }
}

#[cfg(test)]
mod test {
    use crate::physical::{
        columnar::traits::columnscan::ColumnScanT,
        tabular::{
            table_types::trie::{Trie, TrieScanGeneric},
            traits::triescan::{TrieScan, TrieScanEnum},
        },
        util::make_column_with_intervals_t,
    };

    use super::TrieScanNulls;

    fn scan_next(int_scan: &mut TrieScanNulls) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*int_scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut TrieScanNulls) -> Option<u64> {
        unsafe {
            if let ColumnScanT::U64(rcs) = &(*int_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_nulls() {
        let column_x = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_y = make_column_with_intervals_t(&[2, 4, 1], &[0, 2]);
        let column_z = make_column_with_intervals_t(&[5, 1, 7, 9], &[0, 1, 3]);

        let trie = Trie::new(vec![column_x, column_y, column_z]);
        let trie_generic = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let mut trie_iter = TrieScanNulls::new(trie_generic, 2, 100);

        assert!(scan_current(&mut trie_iter).is_none());

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
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(102));
        assert_eq!(scan_current(&mut trie_iter), Some(102));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(103));
        assert_eq!(scan_current(&mut trie_iter), Some(103));
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
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(104));
        assert_eq!(scan_current(&mut trie_iter), Some(104));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(105));
        assert_eq!(scan_current(&mut trie_iter), Some(105));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(106));
        assert_eq!(scan_current(&mut trie_iter), Some(106));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(107));
        assert_eq!(scan_current(&mut trie_iter), Some(107));
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
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(108));
        assert_eq!(scan_current(&mut trie_iter), Some(108));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(109));
        assert_eq!(scan_current(&mut trie_iter), Some(109));

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
    }
}
