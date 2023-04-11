use polars::export::arrow::compute::arity;

use crate::physical::{
    columnar::{
        operations::{ColumnScanFollow, ColumnScanMinus, ColumnScanSubtract},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::StorageTypeName,
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
    util::mapping::permutation::{self, Permutation},
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// TODO: Descriptions
#[derive(Debug)]
pub struct TrieScanSubtract<'a> {
    /// [`TrieScan`] from which elements are being subtracted
    trie_left: Box<TrieScanEnum<'a>>,
    /// Elements that are subtracted
    trie_right: Box<TrieScanEnum<'a>>,

    /// Has an entry for each layer in `trie_left`
    /// If `matched_layers[i]` is true
    matched_layers: Vec<bool>,

    /// Current layer of this scan.
    current_layer: Option<usize>,

    /// List of `ColumnScanSubtract` where every entry represents one level of the resulting trie.
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

#[derive(Debug)]
pub struct SubtractInfo {
    skipped_layers: Vec<usize>,
}

impl SubtractInfo {
    /// Create new [`SubtractInfo`].
    pub fn new(mut skipped_layers: Vec<usize>) -> Self {
        skipped_layers.sort();

        Self { skipped_layers }
    }

    /// Applies a permutation representing a reordering of the trie scan that this info is attached to.
    pub fn apply_permutation(&mut self, permutation: Permutation) {
        self.skipped_layers
            .iter_mut()
            .map(|l| permutation.get(l))
            .collect();
        self.skipped_layers.sort();
    }
}

impl<'a> TrieScanSubtract<'a> {
    /// Construct new [`TrieScanSubtract`] object.
    pub fn new(
        trie_left: TrieScanEnum<'a>,
        trie_right: TrieScanEnum<'a>,
        info: SubtractInfo,
    ) -> Self {
        debug_assert!(trie_left.get_types().len() >= trie_right.get_types().len());
        debug_assert!(trie_left.get_types().len() > info.skipped_layers.len());
        debug_assert!(
            trie_left.get_types().len() - info.skipped_layers.len() == trie_right.get_types().len()
        );
        debug_assert!(info.skipped_layers.is_sorted());
        debug_assert!(info
            .skipped_layers
            .iter()
            .all(|&l| l < trie_left.get_types().len()));

        let arity_left = trie_left.get_types().len();

        let mut matched_layers = vec![true; arity_left];
        for layer in info.skipped_layers {
            matched_layers[layer] = false;
        }
        let last_not_skipped_layer = matched_layers
            .iter()
            .rev()
            .position(|&l| l == true)
            .expect("At least one layer should have not been skipped.");

        let mut column_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity_left);
        let last_scan_index: Option<usize> = None;

        for layer_index in 0..arity_left {
            unsafe {
                if let ColumnScanT::U64(left_scan_enum) =
                    &*trie_left.get_scan(layer_index).unwrap().get()
                {
                    if let ColumnScanT::U64(right_scan_enum) =
                        &*trie_right.get_scan(layer_index).unwrap().get()
                    {
                        let new_scan = ColumnScanEnum::ColumnScanSubtract(ColumnScanSubtract::new(
                            left_scan_enum,
                            right_scan_enum,
                            t,
                            i,
                        ));
                    }
                }
            }
        }

        Self {
            trie_left: Box::new(trie_left),
            trie_right: Box::new(trie_right),
            matched_layers,
            current_layer: None,
            column_scans,
        }
    }
}

/// [`TrieScan`] containg all elements from a "left" [`TrieScan`] that are not in the "right" [`TrieScan`]  
#[derive(Debug)]
pub struct TrieScanMinus<'a> {
    /// [`TrieScan`] from which elements are being subtracted
    trie_left: Box<TrieScanEnum<'a>>,

    /// Elements that are subtracted
    trie_right: Box<TrieScanEnum<'a>>,

    /// Current layer of the left trie
    layer_left: Option<usize>,

    /// Current layer of the right trie
    /// This is different from `layer_left` because the right trie might not contain
    /// an element with the same "prefix" as an element in the left trie
    layer_right: Option<usize>,

    /// For the last layer contians a [`ColumnScanMinus`]. All previous layer
    /// contain a [`ColumnScanFollow`]
    /// Note: Reason for using [`UnsafeCell`] is explained for [`TrieScanJoin`]
    minus_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanMinus<'a> {
    /// Construct new [`TrieScanMinus`] object.
    pub fn new(trie_left: TrieScanEnum<'a>, trie_right: TrieScanEnum<'a>) -> TrieScanMinus<'a> {
        debug_assert!(trie_left.get_types().len() == trie_right.get_types().len());

        let target_types = trie_left.get_types();
        let layer_count = target_types.len();

        let mut minus_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(layer_count);

        for (layer_index, target_type) in target_types.iter().enumerate() {
            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {
                    unsafe {
                        if let ColumnScanT::$variant(left_scan_enum) =
                            &*trie_left.get_scan(layer_index).unwrap().get()
                        {
                            if let ColumnScanT::$variant(right_scan_enum) =
                                &*trie_right.get_scan(layer_index).unwrap().get()
                            {
                                // For the minus trie operator we need to check if a path contained
                                // in the "left" trie is also present in the "right" trie
                                let new_scan = if layer_index < layer_count - 1 {
                                    // In the non-leaf layers, [`ColumnScanFollow`] is responsible
                                    // for checking if the paths match up to the last layer
                                    ColumnScanEnum::ColumnScanFollow(ColumnScanFollow::new(
                                        left_scan_enum,
                                        right_scan_enum,
                                    ))
                                } else {
                                    // Only on the last layer the [`ColumnScanMinus`] object
                                    // returns the values contained in the left trie but not in the right
                                    // (assuming the paths match up to this point)
                                    ColumnScanEnum::ColumnScanMinus(ColumnScanMinus::new(
                                        left_scan_enum,
                                        right_scan_enum,
                                    ))
                                };

                                minus_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                                    ColumnScanCell::new(new_scan),
                                )));
                            } else {
                                panic!("Expected a column scan of type {}", stringify!($variant));
                            }
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($variant));
                        }
                    }
                };
            }

            match target_type {
                StorageTypeName::U32 => init_scans_for_datatype!(U32),
                StorageTypeName::U64 => init_scans_for_datatype!(U64),
                StorageTypeName::I64 => init_scans_for_datatype!(I64),
                StorageTypeName::Float => init_scans_for_datatype!(Float),
                StorageTypeName::Double => init_scans_for_datatype!(Double),
            };
        }

        TrieScanMinus {
            trie_left: Box::new(trie_left),
            trie_right: Box::new(trie_right),
            layer_left: None,
            layer_right: None,
            minus_scans,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanMinus<'a> {
    fn up(&mut self) {
        self.layer_left = self
            .layer_left
            .expect("calling up only allowed after calling down")
            .checked_sub(1);

        self.trie_left.up();

        // Only update `layer_right` when it was the same as `layer_left` before calling `up`
        if self.layer_right.is_some() && self.layer_right > self.layer_left {
            self.layer_right = self.layer_left;

            self.trie_right.up();
        }
    }

    fn down(&mut self) {
        let next_layer = self.layer_left.map_or(0, |v| v + 1);
        debug_assert!(next_layer < self.get_types().len());

        if next_layer > 0 {
            let current_layer = next_layer - 1;

            // We only call `down` on the right trie if the path matches with the left trie up to this point
            // and they both point to the same value
            // Note that `down` cannot be called on the last layer
            // hence we know that `minus_scans[current_layer]` contains a [`ColumnScanFollow`] object
            if self.layer_left == self.layer_right
                && self.minus_scans[current_layer].get_mut().is_equal()
            {
                self.trie_right.down();
                self.layer_right = Some(next_layer);
            }
        } else {
            self.layer_right = Some(next_layer);
            self.trie_right.down();
        }

        self.trie_left.down();
        self.layer_left = Some(next_layer);

        // The above down call has changed the sub scans of the current layer
        // Hence, we need to reset its state
        self.minus_scans[next_layer].get_mut().reset();

        if next_layer == self.get_types().len() - 1 {
            self.minus_scans[next_layer]
                .get_mut()
                .minus_enable(self.layer_left == self.layer_right);
        }
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.minus_scans[self.layer_left?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.minus_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        self.trie_left.get_types()
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanMinus;
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::physical::tabular::traits::triescan::{TrieScan, TrieScanEnum};
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn diff_next(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = diff_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn diff_current(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = diff_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_trie_minus() {
        let column_left_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_left_y = make_column_with_intervals_t(&[3, 6, 8, 2, 7, 5], &[0, 3, 5]);
        let column_right_x = make_column_with_intervals_t(&[1, 3, 4], &[0]);
        let column_right_y = make_column_with_intervals_t(&[2, 6, 9, 2, 5, 8], &[0, 3, 5]);

        let trie_left = Trie::new(vec![column_left_x, column_left_y]);
        let trie_right = Trie::new(vec![column_right_x, column_right_y]);

        let trie_left_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_left));
        let trie_right_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_right));

        let mut diff_iter = TrieScanMinus::new(trie_left_iter, trie_right_iter);
        assert!(diff_iter.current_scan().is_none());

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(3));
        assert_eq!(diff_current(&mut diff_iter), Some(3));
        assert_eq!(diff_next(&mut diff_iter), Some(8));
        assert_eq!(diff_current(&mut diff_iter), Some(8));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), Some(7));
        assert_eq!(diff_current(&mut diff_iter), Some(7));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(3));
        assert_eq!(diff_current(&mut diff_iter), Some(3));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);
    }

    #[test]
    fn test_trie_minus_2() {
        let column_left_x = make_column_with_intervals_t(&[4, 7, 8, 9, 10], &[0]);
        let column_left_y = make_column_with_intervals_t(&[1, 1, 2, 1, 2, 1, 2], &[0, 1, 2, 3, 5]);

        let column_right_x = make_column_with_intervals_t(&[2, 3, 4, 5, 7, 8, 9, 10], &[0]);
        let column_right_y = make_column_with_intervals_t(
            &[1, 1, 2, 1, 2, 7, 5, 7, 1, 2, 7],
            &[0, 1, 2, 3, 4, 5, 6, 8],
        );

        let trie_left = Trie::new(vec![column_left_x, column_left_y]);
        let trie_right = Trie::new(vec![column_right_x, column_right_y]);

        let trie_left_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_left));
        let trie_right_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_right));

        let mut diff_iter = TrieScanMinus::new(trie_left_iter, trie_right_iter);
        assert!(diff_iter.current_scan().is_none());

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(4));
        assert_eq!(diff_current(&mut diff_iter), Some(4));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(7));
        assert_eq!(diff_current(&mut diff_iter), Some(7));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(8));
        assert_eq!(diff_current(&mut diff_iter), Some(8));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(9));
        assert_eq!(diff_current(&mut diff_iter), Some(9));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), Some(1));
        assert_eq!(diff_current(&mut diff_iter), Some(1));
        assert_eq!(diff_next(&mut diff_iter), Some(2));
        assert_eq!(diff_current(&mut diff_iter), Some(2));
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), Some(10));
        assert_eq!(diff_current(&mut diff_iter), Some(10));

        diff_iter.down();
        assert_eq!(diff_current(&mut diff_iter), None);
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);

        diff_iter.up();
        assert_eq!(diff_next(&mut diff_iter), None);
        assert_eq!(diff_current(&mut diff_iter), None);
    }
}
