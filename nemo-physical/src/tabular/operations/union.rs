//! This module defines [TrieScanUnion] and [GeneratorUnion].

use std::{
    cell::{RefCell, UnsafeCell},
    rc::Rc,
};

use crate::{
    columnar::{
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
        operations::union::ColumnScanUnion,
    },
    datatypes::{storage_type_name::StorageTypeBitSet, Double, Float, StorageTypeName},
    management::database::Dict,
    tabular::triescan::{PartialTrieScan, TrieScanEnum},
};

use super::OperationGenerator;

/// Used to create a [TrieScanUnion]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct GeneratorUnion {}

impl GeneratorUnion {
    /// Create a new [GeneratorUnion].
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl OperationGenerator for GeneratorUnion {
    fn generate<'a>(
        &'_ self,
        trie_scans: Vec<Option<TrieScanEnum<'a>>>,
        _dictionary: &'a RefCell<Dict>,
    ) -> Option<TrieScanEnum<'a>> {
        // We ignore any empy tables
        let trie_scans = trie_scans.into_iter().flatten().collect::<Vec<_>>();

        // We return `None` if there are no input tables left
        if trie_scans.is_empty() {
            return None;
        }

        debug_assert!(trie_scans
            .iter()
            .all(|scan| scan.arity() == trie_scans[0].arity()));

        let arity = trie_scans.first().map_or(0, |s| s.arity());

        let mut active_scans = Vec::with_capacity(arity + 1);
        let mut column_scans = Vec::<UnsafeCell<ColumnScanRainbow<'a>>>::with_capacity(arity);

        active_scans.push(Rc::new(RefCell::new((0..trie_scans.len()).collect())));

        for layer in 0..arity {
            active_scans.push(Rc::new(RefCell::new(Vec::with_capacity(trie_scans.len()))));

            macro_rules! union_scan {
                ($type:ty, $scan:ident) => {{
                    let mut input_scans =
                        Vec::<&'a ColumnScanCell<$type>>::with_capacity(trie_scans.len());

                    for trie_scan in &trie_scans {
                        let input_scan = &unsafe { &*trie_scan.scan(layer).get() }.$scan;
                        input_scans.push(input_scan);
                    }

                    let smallest_scans = active_scans[layer + 1].clone();
                    let active_scans = active_scans[layer].clone();

                    ColumnScanEnum::Union(ColumnScanUnion::new(
                        input_scans,
                        active_scans,
                        smallest_scans,
                    ))
                }};
            }

            let union_scan_id32 = union_scan!(u32, scan_id32);
            let union_scan_id64 = union_scan!(u64, scan_id64);
            let union_scan_i64 = union_scan!(i64, scan_i64);
            let union_scan_float = union_scan!(Float, scan_float);
            let union_scan_double = union_scan!(Double, scan_double);

            let new_scan = ColumnScanRainbow::new(
                union_scan_id32,
                union_scan_id64,
                union_scan_i64,
                union_scan_float,
                union_scan_double,
            );
            column_scans.push(UnsafeCell::new(new_scan));
        }

        Some(TrieScanEnum::Union(TrieScanUnion {
            trie_scans,
            column_scans,
            active_scans,
            current_layer: None,
        }))
    }
}

/// [PartialTrieScan] which represents the union of a list of [PartialTrieScan]s
#[derive(Debug)]
pub(crate) struct TrieScanUnion<'a> {
    /// Input trie scans over which the union is computed
    trie_scans: Vec<TrieScanEnum<'a>>,

    /// For each layer, contains a list of indices into `trie_scans`
    /// inidicating which input scans point to the same partial row of values
    active_scans: Vec<Rc<RefCell<Vec<usize>>>>,
    /// Current layer of this [PartialTrieScan]
    current_layer: Option<usize>,

    /// For each layer in the resulting trie contains a [ColumnScanRainbow]
    /// evaluating the union of the underlying columns of the input trie.
    column_scans: Vec<UnsafeCell<ColumnScanRainbow<'a>>>,
}

impl<'a> PartialTrieScan<'a> for TrieScanUnion<'a> {
    fn up(&mut self) {
        let previous_layer = self.current_layer();

        for trie_scan in &mut self.trie_scans {
            if trie_scan.current_layer() == previous_layer {
                trie_scan.up();
            }
        }

        self.current_layer = self
            .current_layer
            .expect("Cannot call PartialTrieScan::up when in starting position")
            .checked_sub(1);
    }

    fn down(&mut self, next_type: StorageTypeName) {
        let next_layer = self.current_layer.map_or(0, |layer| layer + 1);

        for &scan_index in self.active_scans[next_layer].borrow().iter() {
            self.trie_scans[scan_index].down(next_type);
        }

        self.column_scans[next_layer].get_mut().reset(next_type);
        self.current_layer = Some(next_layer);
    }

    fn arity(&self) -> usize {
        self.trie_scans[0].arity()
    }

    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>> {
        &self.column_scans[layer]
    }

    fn possible_types(&self, layer: usize) -> StorageTypeBitSet {
        self.trie_scans
            .iter()
            .fold(StorageTypeBitSet::empty(), |result, scan| {
                result.union(scan.possible_types(layer))
            })
    }

    fn current_layer(&self) -> Option<usize> {
        self.current_layer
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{
        datatypes::{StorageTypeName, StorageValueT},
        dictionary::meta_dv_dict::MetaDvDictionary,
        tabular::{
            operations::{join::GeneratorJoin, OperationGenerator, OperationTableGenerator},
            triescan::TrieScanEnum,
        },
        util::test_util::test::{trie_dfs, trie_id32},
    };

    use super::GeneratorUnion;

    #[test]
    fn basic_union() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![&[1, 2], &[2, 5], &[4, 4]]);
        let trie_b = trie_id32(vec![&[1, 2], &[1, 4], &[2, 4], &[7, 8], &[7, 9]]);
        let trie_c = trie_id32(vec![&[1, 1], &[1, 4], &[1, 6], &[3, 3], &[7, 7], &[7, 8]]);

        let trie_a_scan = TrieScanEnum::Generic(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::Generic(trie_b.partial_iterator());
        let trie_c_scan = TrieScanEnum::Generic(trie_c.partial_iterator());

        let union_generator = GeneratorUnion::new();
        let mut union_scan = union_generator
            .generate(
                vec![Some(trie_a_scan), Some(trie_b_scan), Some(trie_c_scan)],
                &dictionary,
            )
            .unwrap();

        trie_dfs(
            &mut union_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
                StorageValueT::Id32(6),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(5),
                StorageValueT::Id32(3), // x = 3
                StorageValueT::Id32(3),
                StorageValueT::Id32(4), // x = 4
                StorageValueT::Id32(4),
                StorageValueT::Id32(7), // x = 7
                StorageValueT::Id32(7),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
            ],
        );
    }

    #[test]
    fn union_2() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![
            &[1, 2],
            &[1, 3],
            &[1, 5],
            &[1, 10],
            &[2, 4],
            &[2, 7],
            &[2, 10],
            &[5, 9],
            &[7, 8],
            &[7, 9],
            &[7, 10],
        ]);
        let trie_b = trie_id32(vec![
            &[4, 1],
            &[7, 1],
            &[8, 2],
            &[9, 1],
            &[9, 2],
            &[10, 1],
            &[10, 2],
        ]);

        let trie_a_scan = TrieScanEnum::Generic(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::Generic(trie_b.partial_iterator());

        let union_generator = GeneratorUnion::new();
        let mut union_scan = union_generator
            .generate(vec![Some(trie_a_scan), Some(trie_b_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut union_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(2),
                StorageValueT::Id32(3),
                StorageValueT::Id32(5),
                StorageValueT::Id32(10),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(7),
                StorageValueT::Id32(10),
                StorageValueT::Id32(4), // x = 4
                StorageValueT::Id32(1),
                StorageValueT::Id32(5), // x = 5
                StorageValueT::Id32(9),
                StorageValueT::Id32(7), // x = 7
                StorageValueT::Id32(1),
                StorageValueT::Id32(8),
                StorageValueT::Id32(9),
                StorageValueT::Id32(10),
                StorageValueT::Id32(8), // x = 8
                StorageValueT::Id32(2),
                StorageValueT::Id32(9), // x = 9
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(10), // x = 10
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
            ],
        );
    }

    #[test]
    fn union_3() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![&[4, 1, 2]]);
        let trie_b = trie_id32(vec![&[1, 4, 1], &[2, 4, 1], &[4, 1, 4]]);

        let trie_a_scan = TrieScanEnum::Generic(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::Generic(trie_b.partial_iterator());

        let union_generator = GeneratorUnion::new();
        let mut union_scan = union_generator
            .generate(vec![Some(trie_a_scan), Some(trie_b_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut union_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(4),
                StorageValueT::Id32(1),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(1),
                StorageValueT::Id32(4), // x = 4
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
            ],
        );
    }

    #[test]
    fn union_close() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![&[1, 3], &[1, 4], &[2, 5]]);
        let trie_b = trie_id32(vec![&[2, 5], &[2, 6]]);

        let trie_a_scan = TrieScanEnum::Generic(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::Generic(trie_b.partial_iterator());

        let union_generator = GeneratorUnion::new();
        let mut union_scan = union_generator
            .generate(vec![Some(trie_a_scan), Some(trie_b_scan)], &dictionary)
            .unwrap();

        trie_dfs(
            &mut union_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(3),
                StorageValueT::Id32(4),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(5),
                StorageValueT::Id32(6),
            ],
        );
    }

    #[test]
    fn union_of_join() {
        let dictionary = RefCell::new(MetaDvDictionary::default());

        let trie_a = trie_id32(vec![&[1, 4], &[4, 1]]);
        let trie_b = trie_id32(vec![&[1, 2], &[2, 4]]);
        let trie_c = trie_id32(vec![&[1, 2], &[1, 4], &[2, 4], &[4, 1]]);
        let trie_d = trie_id32(vec![&[1, 4], &[4, 1]]);

        let trie_a_scan = TrieScanEnum::Generic(trie_a.partial_iterator());
        let trie_b_scan = TrieScanEnum::Generic(trie_b.partial_iterator());
        let trie_c_scan = TrieScanEnum::Generic(trie_c.partial_iterator());
        let trie_d_scan = TrieScanEnum::Generic(trie_d.partial_iterator());

        let mut marker_generator = OperationTableGenerator::new();
        marker_generator.add_marker("x");
        marker_generator.add_marker("y");
        marker_generator.add_marker("z");

        let markers_result_ab = marker_generator.operation_table(["x", "y", "z"].iter());
        let markers_a = marker_generator.operation_table(["x", "y"].iter());
        let markers_b = marker_generator.operation_table(["y", "z"].iter());

        let markers_result_cd = marker_generator.operation_table(["x", "y", "z"].iter());
        let markers_c = marker_generator.operation_table(["x", "y"].iter());
        let markers_d = marker_generator.operation_table(["y", "z"].iter());

        let join_ab_generator = GeneratorJoin::new(markers_result_ab, vec![markers_a, markers_b]);
        let join_cd_generator = GeneratorJoin::new(markers_result_cd, vec![markers_c, markers_d]);

        let join_ab_scan =
            join_ab_generator.generate(vec![Some(trie_a_scan), Some(trie_b_scan)], &dictionary);
        let join_cd_scan =
            join_cd_generator.generate(vec![Some(trie_c_scan), Some(trie_d_scan)], &dictionary);

        let union_generator = GeneratorUnion::new();
        let mut union_scan = union_generator
            .generate(vec![join_ab_scan, join_cd_scan], &dictionary)
            .unwrap();

        trie_dfs(
            &mut union_scan,
            &[StorageTypeName::Id32],
            &[
                StorageValueT::Id32(1), // x = 1
                StorageValueT::Id32(4),
                StorageValueT::Id32(1),
                StorageValueT::Id32(2), // x = 2
                StorageValueT::Id32(4),
                StorageValueT::Id32(1),
                StorageValueT::Id32(4), // x = 4
                StorageValueT::Id32(1),
                StorageValueT::Id32(2),
                StorageValueT::Id32(4),
            ],
        );
    }
}
