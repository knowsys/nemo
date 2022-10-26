use super::{Table, TableSchema, Trie, TrieScan, TrieSchema, TrieSchemaEntry};
use crate::physical::columns::{
    Column, ColumnScan, IntervalColumn, IntervalColumnEnum, IntervalColumnT, RangedColumnScan,
    RangedColumnScanCell, RangedColumnScanEnum, RangedColumnScanT, ReorderScan,
};
use crate::physical::datatypes::{ColumnDataType, DataTypeName};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::Range;

/// Helper function which, given a continous range, expands it in such a way
/// that all of the child nodes are covered as well
pub fn expand_range(column: &IntervalColumnT, range: Range<usize>) -> Range<usize> {
    let start = column.int_bounds(range.start).start;
    let end = if range.end >= column.int_len() {
        column.len()
    } else {
        column.int_bounds(range.end).start
    };

    log::debug!("{range:?} {start}..{end}");

    start..end
}

fn shrink_position(column: &IntervalColumnT, pos: usize) -> usize {
    fn shrink_position_t<T>(column: &IntervalColumnEnum<T>, pos: usize) -> usize
    where
        T: ColumnDataType,
    {
        let mut column_iter = column.get_int_column().iter();
        column_iter.seek(pos + 1);
        let block_position = column_iter.pos();

        block_position.map_or(column.get_int_column().len() - 1, |i| i - 1)
    }

    match column {
        IntervalColumnT::U64(col) => shrink_position_t(col, pos),
        IntervalColumnT::Float(col) => shrink_position_t(col, pos),
        IntervalColumnT::Double(col) => shrink_position_t(col, pos),
    }
}

/// Iterator which can reorder and project away colums of a trie
#[derive(Debug)]
pub struct TrieProject<'a> {
    trie: &'a Trie,
    current_layer: Option<usize>,
    target_schema: TrieSchema,
    picked_columns: Vec<usize>,
    reorder_scans: Vec<UnsafeCell<RangedColumnScanT<'a>>>,
}

impl<'a> TrieProject<'a> {
    /// Create new TrieProject object
    pub fn new(trie: &'a Trie, picked_columns: Vec<usize>) -> Self {
        let input_schema = trie.schema();

        let mut reorder_scans =
            Vec::<UnsafeCell<RangedColumnScanT>>::with_capacity(picked_columns.len());

        let mut target_attributes = Vec::<TrieSchemaEntry>::with_capacity(picked_columns.len());
        for &col_index in &picked_columns {
            let current_label = input_schema.get_label(col_index);
            let current_datatype = input_schema.get_type(col_index);

            target_attributes.push(TrieSchemaEntry {
                label: current_label,
                datatype: current_datatype,
            });

            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {{
                    let current_column =
                        if let IntervalColumnT::$variant(col) = trie.get_column(col_index) {
                            col
                        } else {
                            panic!("Do other cases later")
                        };

                    reorder_scans.push(UnsafeCell::new(RangedColumnScanT::$variant(
                        RangedColumnScanCell::new(RangedColumnScanEnum::ReorderScan(
                            ReorderScan::new(current_column.get_data_column()),
                        )),
                    )));
                }};
            }

            match current_datatype {
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
            }
        }
        let target_schema = TrieSchema::new(target_attributes);

        Self {
            trie,
            current_layer: None,
            target_schema,
            picked_columns,
            reorder_scans,
        }
    }
}

impl<'a> TrieScan<'a> for TrieProject<'a> {
    fn up(&mut self) {
        debug_assert!(self.current_layer.is_some());
        let current_layer = self.current_layer.unwrap();

        self.current_layer = if current_layer == 0 {
            None
        } else {
            Some(current_layer - 1)
        };
    }

    fn down(&mut self) {
        let next_layer = self.current_layer.map_or(0, |v| v + 1);

        debug_assert!(next_layer < self.target_schema.arity());

        let next_column = self.trie.get_column(self.picked_columns[next_layer]);

        macro_rules! down_for_datatype {
            ($variant:ident) => {{
                let next_column = if let IntervalColumnT::U64(col) = next_column
                {
                    col
                } else {
                    panic!("Do other cases later")
                };

                let next_ranges = if self.current_layer.is_none() {
                    vec![0..next_column.len()]
                } else {
                    let layer_for_comparison = self.picked_columns[0..self.current_layer.unwrap()]
                        .iter()
                        .copied()
                        .enumerate()
                        .filter(|(_, col_i)| *col_i >= self.picked_columns[self.current_layer.unwrap()]
                        //        && *col_i <= self.picked_columns[next_layer]
                        )
                        .max_by_key(|(_, col_i)| *col_i)
                        .map(|(i, _)| i)
                        .unwrap_or(self.current_layer.unwrap());

                    let current_cursors = self.reorder_scans[layer_for_comparison]
                        .get_mut()
                        .pos_multiple()
                        .expect("Should not call down when not on an element");

                    log::debug!("CURRENT_CURSORS {:?}", current_cursors);
                    log::debug!("PICKED_COLUMNS {:?} {:?} {:?}", layer_for_comparison, next_layer, self.picked_columns);

                    if self.picked_columns[layer_for_comparison] < self.picked_columns[next_layer] {
                        let mut ranges = Vec::<Range<usize>>::new();
                        for current_cursor in current_cursors {
                            let mut range = current_cursor..(current_cursor + 1);

                            for expand_index in (self.picked_columns[layer_for_comparison] + 1)
                                ..=self.picked_columns[next_layer]
                            {
                                log::debug!("EXPAND_INDEX {:?}", expand_index);
                                range = expand_range(self.trie.get_column(expand_index), range);
                            }

                            ranges.push(range);
                        }

                        ranges
                    } else {
                        let mut ranges = Vec::<Range<usize>>::new();
                        for current_cursor in current_cursors {
                            let mut value = current_cursor;
                            for shrink_index in (self.picked_columns[next_layer] + 1
                                ..=(self.picked_columns[layer_for_comparison]))
                                .rev()
                            {
                                log::debug!("VALUE {value:?}");

                                value = shrink_position(self.trie.get_column(shrink_index), value);
                            }

                            // This assumes that current_cursors is sorted
                            // which should be the case since the sort method used in permutator is stable
                            if let Some(last_range) = ranges.last() {
                                if value != last_range.start {
                                    ranges.push(value..(value + 1));
                                }
                            } else {
                                ranges.push(value..(value + 1));
                            }
                        }

                        ranges
                    }
                };

                log::debug!("NEXT_RANGES {next_layer:?} {next_ranges:?}");

                self.reorder_scans[next_layer]
                    .get_mut()
                    .narrow_ranges(next_ranges);
            }};
        }

        match next_column {
            IntervalColumnT::U64(_) => down_for_datatype!(U64),
            IntervalColumnT::Float(_) => down_for_datatype!(Float),
            IntervalColumnT::Double(_) => down_for_datatype!(Double),
        }

        self.current_layer = Some(next_layer);
    }

    fn current_scan(&self) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        debug_assert!(self.current_layer.is_some());

        Some(&self.reorder_scans[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<RangedColumnScanT<'a>>> {
        Some(&self.reorder_scans[index])
    }

    fn get_schema(&self) -> &dyn TableSchema {
        &self.target_schema
    }
}

#[cfg(test)]
mod test {
    use super::TrieProject;
    use crate::physical::columns::{Column, IntervalColumnT};
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};
    use crate::physical::tables::{
        materialize, Table, Trie, TrieScanEnum, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::util::test_util::make_gict;
    use test_log::test;

    #[test]
    fn single_small_hole() {
        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_gict(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

        let column_vec = vec![column_fst, column_snd, column_trd];

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
        ]);

        let trie = Trie::new(schema, column_vec);

        let trie_no_first = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![1, 2],
        )));
        let trie_no_middle = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![0, 2],
        )));
        let trie_no_last = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![0, 1],
        )));

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_no_first.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_no_first.get_column(1) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 3, 4, 5]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![4, 6, 7, 9, 2, 3, 5, 8]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 4, 6]
        );

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_no_middle.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_no_middle.get_column(1) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![5, 7, 8, 9, 2, 3, 4, 6]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 4]
        );

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_no_last.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_no_last.get_column(1) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![3, 5, 2, 4]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn multiple_big_holes() {
        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_gict(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);
        let column_fth = make_gict(
            &[5, 10, 15, 2, 4, 6, 1, 3, 7, 9, 11, 13, 17],
            &[0, 3, 5, 6, 7, 8, 10, 11],
        );
        let column_vth = make_gict(
            &[
                1, 2, 20, 3, 4, 21, 5, 6, 22, 7, 8, 23, 9, 10, 24, 11, 12, 25, 13, 14,
            ],
            &[0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18],
        );
        let column_sth = make_gict(
            &[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ],
            &[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ],
        );

        let column_vec = vec![
            column_fst, column_snd, column_trd, column_fth, column_vth, column_sth,
        ];

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
            TrieSchemaEntry {
                label: 4,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 5,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![0, 3, 5],
        )));

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_projected.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_middle = if let IntervalColumnT::U64(col) = trie_projected.get_column(1) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_projected.get_column(2) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_middle
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2, 4, 5, 6, 10, 15, 3, 7, 9, 11, 13, 17]
        );

        assert_eq!(
            proj_column_middle
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 7]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![9, 10, 5, 6, 7, 0, 1, 8, 2, 3, 4, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 3, 5, 7, 8, 9, 11, 12, 14, 15, 17, 18]
        );
    }

    #[test]
    fn reorder() {
        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_gict(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

        let column_vec = vec![column_fst, column_snd, column_trd];

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
        ]);

        let trie = Trie::new(schema, column_vec);

        let trie_reordered = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![2, 0, 1],
        )));

        log::debug!(
            "\n{}\n\n{}",
            trie.debug(&PrefixedStringDictionary::default()),
            trie_reordered.debug(&PrefixedStringDictionary::default())
        );

        assert_eq!(trie.row_num(), trie_reordered.row_num());

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_reordered.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_middle = if let IntervalColumnT::U64(col) = trie_reordered.get_column(1) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_reordered.get_column(2) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 3, 4, 5, 6, 7, 8, 9]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_middle
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 2, 2, 1, 2, 1, 1, 1]
        );

        assert_eq!(
            proj_column_middle
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![4, 4, 2, 5, 2, 3, 5, 3]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn reorder_and_project() {
        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_gict(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

        let column_vec = vec![column_fst, column_snd, column_trd];

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
        ]);

        let trie = Trie::new(schema, column_vec);

        let trie_reordered = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![2, 0],
        )));

        let proj_column_upper = if let IntervalColumnT::U64(col) = trie_reordered.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_lower = if let IntervalColumnT::U64(col) = trie_reordered.get_column(1) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_upper
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 3, 4, 5, 6, 7, 8, 9]
        );

        assert_eq!(
            proj_column_upper
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_lower
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 2, 2, 1, 2, 1, 1, 1]
        );

        assert_eq!(
            proj_column_lower
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn test_duplicates() {
        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[3, 4, 4, 5], &[0, 2]);
        let column_trd = make_gict(&[4, 6, 6, 7, 3, 4, 3, 4], &[0, 2, 4, 6]);
        let column_fth = make_gict(&[0, 0, 0, 0, 0, 0, 0, 0], &[0, 1, 2, 3, 4, 5, 6, 7]);
        let column_vth = make_gict(
            &[1, 2, 3, 4, 3, 4, 1, 2, 3, 4, 3, 4, 1, 2, 3, 4],
            &[
                0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32,
            ],
        );

        let column_vec = vec![column_fst, column_snd, column_trd, column_fth, column_vth];

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
            TrieSchemaEntry {
                label: 4,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieProject(TrieProject::new(
            &trie,
            vec![0, 2, 4, 1],
        )));

        let proj_column_fst = if let IntervalColumnT::U64(col) = trie_projected.get_column(0) {
            col
        } else {
            panic!("...")
        };

        let proj_column_snd = if let IntervalColumnT::U64(col) = trie_projected.get_column(1) {
            col
        } else {
            panic!("...")
        };

        let proj_column_trd = if let IntervalColumnT::U64(col) = trie_projected.get_column(2) {
            col
        } else {
            panic!("...")
        };

        let proj_column_fth = if let IntervalColumnT::U64(col) = trie_projected.get_column(3) {
            col
        } else {
            panic!("...")
        };

        assert_eq!(
            proj_column_fst
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2]
        );

        assert_eq!(
            proj_column_fst
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );

        assert_eq!(
            proj_column_snd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![4, 6, 7, 3, 4]
        );

        assert_eq!(
            proj_column_snd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 3]
        );

        assert_eq!(
            proj_column_trd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 2, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4]
        );

        assert_eq!(
            proj_column_trd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 4, 6, 10]
        );

        assert_eq!(
            proj_column_fth
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![3, 3, 3, 4, 3, 4, 4, 4, 5, 5, 4, 4, 4, 5, 4, 5]
        );

        assert_eq!(
            proj_column_fth
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 2, 4, 6, 7, 8, 9, 10, 11, 12, 14]
        );
    }

    /// This is a test case for a bug first encountered while
    /// classifying `smallmed` using the EL calculus rules, where
    /// reordering a table with 2 rows results in a table with 4 rows.
    #[test]
    fn spurious_tuples_in_reorder_bug() {
        let schema_entry = TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        };
        let schema = TrieSchema::new(vec![schema_entry, schema_entry, schema_entry]);

        let mut dict = PrefixedStringDictionary::default();
        let x = dict.add("72".to_owned()).try_into().unwrap();
        let a = dict.add("139".to_owned()).try_into().unwrap();
        let b = dict.add("141".to_owned()).try_into().unwrap();
        let u = dict.add("140".to_owned()).try_into().unwrap();
        let v = dict.add("134".to_owned()).try_into().unwrap();

        let fst = vec![x];
        let snd = vec![a, b];
        let trd = vec![u, v];

        let first = make_gict(&fst, &[0]);
        let second = make_gict(&snd, &[0]);
        let third = make_gict(&trd, &[0, 1]);

        let columns = vec![first, second, third];

        let picked_columns = vec![1, 0, 2];
        let base_trie = Trie::new(schema, columns);
        let mut project = TrieScanEnum::TrieProject(TrieProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project);
        log::debug!("{}", reordered_trie.debug(&dict));
        log::debug!("{reordered_trie:#?}");

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());

        // assert_eq!(
        //     base_trie.get_column(0).iter().collect::<Vec<_>>(),
        //     reordered_trie.get_column(1).iter().collect::<Vec<_>>()
        // );

        // assert_eq!(
        //     base_trie.get_column(1).iter().collect::<Vec<_>>(),
        //     reordered_trie.get_column(0).iter().collect::<Vec<_>>()
        // );

        // assert_eq!(
        //     base_trie.get_column(2).iter().collect::<Vec<_>>(),
        //     reordered_trie.get_column(2).iter().collect::<Vec<_>>()
        // );
    }
}
