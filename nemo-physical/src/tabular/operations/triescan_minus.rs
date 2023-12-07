use crate::{
    columnar::{
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
        operations::{ColumnScanFollow, ColumnScanMinus, ColumnScanSubtract},
    },
    datatypes::{Double, Float, StorageTypeName},
    tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum},
    util::mapping::permutation::Permutation,
};
use std::cell::UnsafeCell;
use std::fmt::Debug;

/// [`PartialTrieScan`] that subtracts from a "main" [`PartialTrieScan`] a list of "subtract" [`PartialTrieScan`],
/// i.e. the results contains all elements that are in main but not in one of the subtract scans.
/// This can also handle subtracting tables of different arities.
#[derive(Debug)]
pub struct TrieScanSubtract<'a> {
    /// [`PartialTrieScan`] from which elements are being subtracted
    trie_main: Box<TrieScanEnum<'a>>,
    /// Elements that are subtracted
    tries_subtract: Vec<TrieScanEnum<'a>>,

    /// For each [`PartialTrieScan`] in `trie_subtract`, additional information relevant for this scan.
    infos: Vec<SubtractInfo>,

    /// List of `ColumnScanSubtract` where every entry represents one level of the resulting trie.
    column_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

/// Struct containing additional information for the setting up a [`TrieScanSubtract`] iterator.
/// One of these structures should be associated with each trie that is to be subtracted.
#[derive(Debug, Clone)]
pub struct SubtractInfo {
    /// Which layers of the main trie are subtracted.
    /// For example, if we have a main trie with for layers: a(x, y, z, w)
    /// and we want to subtract the trie associated with this struct b(y, w)
    /// this vector would contain: [1, 3]
    pub used_layers: Vec<usize>,
}

impl SubtractInfo {
    /// Create new [`SubtractInfo`].
    pub fn new(mut used_layers: Vec<usize>) -> Self {
        used_layers.sort();

        Self { used_layers }
    }

    /// Applies a permutation representing a reordering of the trie scan that this info is attached to.
    pub fn apply_permutation(&mut self, permutation: Permutation) {
        for layer in &mut self.used_layers {
            *layer = permutation.get(*layer);
        }
        self.used_layers.sort();
    }
}

impl<'a> TrieScanSubtract<'a> {
    /// Construct new [`TrieScanSubtract`] object.
    pub fn new(
        trie_main: TrieScanEnum<'a>,
        tries_subtract: Vec<TrieScanEnum<'a>>,
        infos: Vec<SubtractInfo>,
    ) -> Self {
        debug_assert!(tries_subtract.len() == infos.len());
        debug_assert!(tries_subtract
            .iter()
            .all(|trie_subtract| trie_main.get_types().len() >= trie_subtract.get_types().len()));
        debug_assert!(infos
            .iter()
            .all(|info| trie_main.get_types().len() >= info.used_layers.len()));
        debug_assert!(
            tries_subtract
                .iter()
                .zip(infos.iter())
                .all(|(trie_subtract, info)| info.used_layers.len()
                    == trie_subtract.get_types().len())
        );
        debug_assert!(infos.iter().all(|info| info.used_layers.is_sorted()));
        debug_assert!(infos.iter().all(|info| info
            .used_layers
            .iter()
            .all(|&l| l < trie_main.get_types().len())));

        let arity_main = trie_main.get_types().len();

        let last_used_layers: Vec<usize> = infos
            .iter()
            .map(|info| {
                *info
                    .used_layers
                    .iter()
                    .max()
                    .expect("There should be at least one layer that is used.")
            })
            .collect();
        let mut column_scans = Vec::<UnsafeCell<ColumnScanT<'a>>>::with_capacity(arity_main);

        for layer in 0..arity_main {
            macro_rules! subtract_for_datatype {
                ($variant:ident, $type:ty) => {{
                    if let ColumnScanT::$variant(left_scan_enum) =
                        unsafe { &*trie_main.get_scan(layer).unwrap().get() }
                    {
                        let mut scans_follower = Vec::<Option<&ColumnScanCell<'a, $type>>>::new();

                        let mut subtract_indices = Vec::new();
                        let mut follow_indices = Vec::new();

                        for (subtract_index, (trie_subtract, info)) in
                            tries_subtract.iter().zip(infos.iter()).enumerate()
                        {
                            let used_layer = info.used_layers.iter().position(|&l| l == layer);
                            let is_last = layer == last_used_layers[subtract_index];

                            if let Some(used_layer) = used_layer {
                                if let ColumnScanT::$variant(subtract_scan_enum) =
                                    unsafe { &*trie_subtract.get_scan(used_layer).unwrap().get() }
                                {
                                    scans_follower.push(Some(subtract_scan_enum));
                                } else {
                                    panic!("Expected a column scan of type {}", stringify!($type));
                                }

                                if is_last {
                                    subtract_indices.push(subtract_index);
                                } else {
                                    follow_indices.push(subtract_index);
                                }
                            } else {
                                scans_follower.push(None);
                            }
                        }

                        let new_scan = ColumnScanEnum::ColumnScanSubtract(ColumnScanSubtract::new(
                            left_scan_enum,
                            scans_follower,
                            subtract_indices,
                            follow_indices,
                        ));

                        column_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                            ColumnScanCell::new(new_scan),
                        )));
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($type));
                    }
                }};
            }

            let output_type = trie_main.get_types()[layer];

            match output_type {
                StorageTypeName::Id32 => subtract_for_datatype!(Id32, u32),
                StorageTypeName::Id64 => subtract_for_datatype!(Id64, u64),
                StorageTypeName::Int64 => subtract_for_datatype!(Int64, i64),
                StorageTypeName::Float => subtract_for_datatype!(Float, Float),
                StorageTypeName::Double => subtract_for_datatype!(Double, Double),
            }
        }

        Self {
            trie_main: Box::new(trie_main),
            tries_subtract,
            infos,
            column_scans,
        }
    }
}

impl<'a> PartialTrieScan<'a> for TrieScanSubtract<'a> {
    fn up(&mut self) {
        let current_layer = self.current_layer().unwrap();

        for (subtract_index, trie_subtract) in self.tries_subtract.iter_mut().enumerate() {
            if let Some(current_layer_subtract) = trie_subtract.current_layer() {
                let used_layer = self.infos[subtract_index].used_layers[current_layer_subtract];

                if current_layer == used_layer {
                    trie_subtract.up();
                }
            }
        }

        self.trie_main.up();
    }

    fn down(&mut self) {
        let next_layer = self.current_layer().map_or(0, |v| v + 1);

        debug_assert!(next_layer < self.get_types().len());

        for (subtract_index, trie_subtract) in self.tries_subtract.iter_mut().enumerate() {
            let next_layer_subtract = trie_subtract.current_layer().map_or(0, |v| v + 1);

            if let Some(&next_used_layer) = self.infos[subtract_index]
                .used_layers
                .get(next_layer_subtract)
            {
                if next_layer == next_used_layer {
                    if next_layer > 0 {
                        let current_layer = next_layer - 1;
                        let equal_values = self.column_scans[current_layer]
                            .get_mut()
                            .subtract_get_equal();

                        if !equal_values[subtract_index] {
                            continue;
                        }
                    }

                    trie_subtract.down();
                }
            }
        }

        if next_layer > 0 {
            let current_layer = next_layer - 1;
            let equal_values = self.column_scans[current_layer]
                .get_mut()
                .subtract_get_equal()
                .clone();

            self.column_scans[next_layer]
                .get_mut()
                .subtract_set_active(equal_values);
        }

        self.trie_main.down();

        // The above down call has changed the sub scans of the current layer
        // Hence, we need to reset its state
        self.column_scans[next_layer].get_mut().reset();
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        let current_layer = self.current_layer()?;

        Some(self.column_scans[current_layer].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.column_scans[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        self.trie_main.get_types()
    }

    fn current_layer(&self) -> Option<usize> {
        self.trie_main.current_layer()
    }
}

/// [`PartialTrieScan`] containg all elements from a "left" [`PartialTrieScan`] that are not in the "right" [`PartialTrieScan`]  
#[derive(Debug)]
pub struct TrieScanMinus<'a> {
    /// [`PartialTrieScan`] from which elements are being subtracted
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
                StorageTypeName::Id32 => init_scans_for_datatype!(Id32),
                StorageTypeName::Id64 => init_scans_for_datatype!(Id64),
                StorageTypeName::Int64 => init_scans_for_datatype!(Int64),
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

impl<'a> PartialTrieScan<'a> for TrieScanMinus<'a> {
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

    fn current_layer(&self) -> Option<usize> {
        self.layer_left
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanMinus;
    use crate::columnar::columnscan::ColumnScanT;
    use crate::tabular::operations::triescan_minus::{SubtractInfo, TrieScanSubtract};
    use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::tabular::traits::partial_trie_scan::{PartialTrieScan, TrieScanEnum};
    use crate::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    fn diff_next(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = diff_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn diff_current(diff_scan: &mut TrieScanMinus) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = diff_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    fn sub_next(sub_scan: &mut TrieScanSubtract) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = sub_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn sub_current(sub_scan: &mut TrieScanSubtract) -> Option<u64> {
        if let ColumnScanT::Id64(rcs) = sub_scan.current_scan()? {
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

    #[test]
    fn subtract() {
        let column_main_x = make_column_with_intervals_t(&[2, 4, 6, 8], &[0]);
        let column_main_y =
            make_column_with_intervals_t(&[0, 1, 5, 0, 2, 5, 2, 9, 1, 4, 5], &[0, 3, 6, 8]);
        let column_main_z = make_column_with_intervals_t(
            &[0, 1, 0, 2, 3, 0, 1, 1, 0, 2, 2, 2, 1],
            &[0, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12],
        );

        let column_sub_1_x = make_column_with_intervals_t(&[1, 3, 6], &[0]);

        let column_sub_2_x = make_column_with_intervals_t(&[2, 4], &[0]);
        let column_sub_2_y = make_column_with_intervals_t(&[5, 2], &[0, 1]);

        let column_sub_3_y = make_column_with_intervals_t(&[0, 9], &[0]);
        let column_sub_3_z = make_column_with_intervals_t(&[0, 2], &[0, 1]);

        let column_sub_4_x = make_column_with_intervals_t(&[8], &[0]);
        let column_sub_4_z = make_column_with_intervals_t(&[2], &[0]);

        let trie_main = Trie::new(vec![column_main_x, column_main_y, column_main_z]);
        let trie_sub_1 = Trie::new(vec![column_sub_1_x]);
        let trie_sub_2 = Trie::new(vec![column_sub_2_x, column_sub_2_y]);
        let trie_sub_3 = Trie::new(vec![column_sub_3_y, column_sub_3_z]);
        let trie_sub_4 = Trie::new(vec![column_sub_4_x, column_sub_4_z]);

        let trie_scan_main = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_main));
        let trie_scan_sub_1 = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub_1));
        let trie_scan_sub_2 = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub_2));
        let trie_scan_sub_3 = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub_3));
        let trie_scan_sub_4 = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub_4));

        let mut sub_scan = TrieScanSubtract::new(
            trie_scan_main,
            vec![
                trie_scan_sub_1,
                trie_scan_sub_2,
                trie_scan_sub_3,
                trie_scan_sub_4,
            ],
            vec![
                SubtractInfo::new(vec![0]),
                SubtractInfo::new(vec![0, 1]),
                SubtractInfo::new(vec![1, 2]),
                SubtractInfo::new(vec![0, 2]),
            ],
        );

        assert!(sub_scan.current_scan().is_none());

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(2));
        assert_eq!(sub_current(&mut sub_scan), Some(2));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(0));
        assert_eq!(sub_current(&mut sub_scan), Some(0));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(0));
        assert_eq!(sub_current(&mut sub_scan), Some(0));
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(4));
        assert_eq!(sub_current(&mut sub_scan), Some(4));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(0));
        assert_eq!(sub_current(&mut sub_scan), Some(0));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(5));
        assert_eq!(sub_current(&mut sub_scan), Some(5));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(8));
        assert_eq!(sub_current(&mut sub_scan), Some(8));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(4));
        assert_eq!(sub_current(&mut sub_scan), Some(4));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), Some(5));
        assert_eq!(sub_current(&mut sub_scan), Some(5));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));
    }

    #[test]
    fn subtract_2() {
        let column_main_x = make_column_with_intervals_t(&[4], &[0]);
        let column_main_y = make_column_with_intervals_t(&[0, 2], &[0]);
        let column_main_z = make_column_with_intervals_t(&[0, 1], &[0, 1]);

        let column_sub_x = make_column_with_intervals_t(&[0], &[0]);

        let trie_main = Trie::new(vec![column_main_x, column_main_y, column_main_z]);
        let trie_sub = Trie::new(vec![column_sub_x]);

        let trie_scan_main = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_main));
        let trie_scan_sub = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub));

        let mut sub_scan = TrieScanSubtract::new(
            trie_scan_main,
            vec![trie_scan_sub],
            vec![SubtractInfo::new(vec![1])],
        );

        assert!(sub_scan.current_scan().is_none());

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(4));
        assert_eq!(sub_current(&mut sub_scan), Some(4));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(2));
        assert_eq!(sub_current(&mut sub_scan), Some(2));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(1));
        assert_eq!(sub_current(&mut sub_scan), Some(1));
    }

    #[test]
    fn subtract_3() {
        let column_main_x = make_column_with_intervals_t(&[2, 4, 8], &[0]);
        let column_main_y = make_column_with_intervals_t(&[0, 0, 5], &[0, 1, 2]);
        let column_main_z = make_column_with_intervals_t(&[0, 0, 1], &[0, 1, 2]);

        let column_sub_x = make_column_with_intervals_t(&[2, 8], &[0]);

        let trie_main = Trie::new(vec![column_main_x, column_main_y, column_main_z]);
        let trie_sub = Trie::new(vec![column_sub_x]);

        let trie_scan_main = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_main));
        let trie_scan_sub = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_sub));

        let mut sub_scan = TrieScanSubtract::new(
            trie_scan_main,
            vec![trie_scan_sub],
            vec![SubtractInfo::new(vec![0])],
        );

        assert!(sub_scan.current_scan().is_none());

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(4));
        assert_eq!(sub_current(&mut sub_scan), Some(4));

        sub_scan.down();
        assert_eq!(sub_current(&mut sub_scan), None);
        assert_eq!(sub_next(&mut sub_scan), Some(0));
        assert_eq!(sub_current(&mut sub_scan), Some(0));
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);

        sub_scan.up();
        assert_eq!(sub_next(&mut sub_scan), None);
        assert_eq!(sub_current(&mut sub_scan), None);
    }
}
