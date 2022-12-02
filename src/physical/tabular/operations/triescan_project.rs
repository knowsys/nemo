use crate::physical::{
    columnar::{
        column_types::interval::{ColumnWithIntervals, ColumnWithIntervalsT},
        operations::columnscan_reorder::ColumnScanReorder,
        traits::{
            column::Column,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
        },
    },
    datatypes::{ColumnDataType, DataTypeName},
    tabular::table_types::trie::{Trie, TrieSchema, TrieSchemaEntry},
    tabular::traits::{table::Table, table_schema::TableSchema, triescan::TrieScan},
};

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::Range;

/// Type that represents a reordering of columns
/// So `perm[i]` is the new index of the ith column
pub type ColumnPermutation = Vec<usize>;

/// Helper function which, given a continous range, expands it in such a way
/// that all of the child nodes are covered as well
pub fn expand_range(column: &ColumnWithIntervalsT, range: Range<usize>) -> Range<usize> {
    let start = column.int_bounds(range.start).start;
    let end = if range.end >= column.int_len() {
        column.len()
    } else {
        column.int_bounds(range.end).start
    };

    log::debug!("{range:?} {start}..{end}");

    start..end
}

fn shrink_position(column: &ColumnWithIntervalsT, pos: usize) -> usize {
    fn shrink_position_t<T>(column: &ColumnWithIntervals<T>, pos: usize) -> usize
    where
        T: ColumnDataType,
    {
        let mut column_iter = column.get_int_column().iter();
        column_iter.seek(pos + 1);
        let block_position = column_iter.pos();

        block_position.map_or(column.get_int_column().len() - 1, |i| i - 1)
    }

    match column {
        ColumnWithIntervalsT::U64(col) => shrink_position_t(col, pos),
        ColumnWithIntervalsT::Float(col) => shrink_position_t(col, pos),
        ColumnWithIntervalsT::Double(col) => shrink_position_t(col, pos),
    }
}

/// Iterator which can reorder and project away colums of a trie
#[derive(Debug)]
pub struct TrieScanProject<'a> {
    trie: &'a Trie,
    current_layer: Option<usize>,
    target_schema: TrieSchema,
    picked_columns: ColumnPermutation,
    reorder_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

impl<'a> TrieScanProject<'a> {
    /// Create new TrieScanProject object
    pub fn new(trie: &'a Trie, picked_columns: ColumnPermutation) -> Self {
        let input_schema = trie.schema();

        let mut reorder_scans = Vec::<UnsafeCell<ColumnScanT>>::with_capacity(picked_columns.len());

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
                        if let ColumnWithIntervalsT::$variant(col) = trie.get_column(col_index) {
                            col
                        } else {
                            panic!("Do other cases later")
                        };

                    reorder_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                        ColumnScanCell::new(ColumnScanEnum::ColumnScanReorder(
                            ColumnScanReorder::new(current_column.get_data_column()),
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

impl<'a> TrieScan<'a> for TrieScanProject<'a> {
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
                let next_column = if let ColumnWithIntervalsT::U64(col) = next_column
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
                        .filter(|(_, col_i)| *col_i >= self.picked_columns[self.current_layer.unwrap()])
                        .max_by_key(|(_, col_i)| *col_i)
                        .map(|(i, _)| i)
                        .unwrap_or(self.current_layer.unwrap());

                    let current_cursors = self.reorder_scans[self.current_layer.unwrap()]
                        .get_mut()
                        .pos_multiple()
                        .expect("Should not call down when not on an element");

                    log::debug!("CURRENT_CURSORS before filtering {:?}", current_cursors);

                    // keep only cursor positions for the comparison layer that are in line with the positions in the current layer
                    // we check this by "shrinking" the cursor positions from the comparison layer
                    let current_cursors: Vec<usize> = if self.current_layer.unwrap() == layer_for_comparison { current_cursors } else {
                        let comparison_cursors = self.reorder_scans[layer_for_comparison]
                            .get_mut()
                            .pos_multiple()
                            .expect("Should not call down when not on an element");

                        current_cursors
                            .into_iter()
                            .flat_map(|c| {
                                let mut cursor_range = c..(c+1);
                                for expand_index in ((self.picked_columns[self.current_layer.unwrap()] + 1)..=self.picked_columns[layer_for_comparison]) {
                                    cursor_range = expand_range(self.trie.get_column(expand_index), cursor_range);
                                }

                                comparison_cursors.iter().copied().filter(move |c| cursor_range.contains(c))
                            })
                            .collect()
                    };


                    log::debug!("CURRENT_CURSORS after filtering {:?}", current_cursors);
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
            ColumnWithIntervalsT::U64(_) => down_for_datatype!(U64),
            ColumnWithIntervalsT::Float(_) => down_for_datatype!(Float),
            ColumnWithIntervalsT::Double(_) => down_for_datatype!(Double),
        }

        self.current_layer = Some(next_layer);
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        debug_assert!(self.current_layer.is_some());

        Some(&self.reorder_scans[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.reorder_scans[index])
    }

    fn get_schema(&self) -> &TrieSchema {
        &self.target_schema
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanProject;
    use crate::physical::columnar::traits::column::Column;
    use crate::physical::datatypes::{DataTypeName, DataValueT};
    use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};
    use crate::physical::tabular::operations::materialize;
    use crate::physical::tabular::table_types::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::tabular::traits::{table::Table, triescan::TrieScanEnum};
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    #[test]
    fn single_small_hole() {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

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

        let trie_no_first = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![1, 2],
        )))
        .unwrap();
        let trie_no_middle = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![0, 2],
        )))
        .unwrap();
        let trie_no_last = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![0, 1],
        )))
        .unwrap();

        let proj_column_upper = trie_no_first.get_column(0).as_u64().unwrap();
        let proj_column_lower = trie_no_first.get_column(1).as_u64().unwrap();
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

        let proj_column_upper = trie_no_middle.get_column(0).as_u64().unwrap();
        let proj_column_lower = trie_no_middle.get_column(1).as_u64().unwrap();
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

        let proj_column_upper = trie_no_last.get_column(0).as_u64().unwrap();
        let proj_column_lower = trie_no_last.get_column(1).as_u64().unwrap();
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
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);
        let column_fth = make_column_with_intervals_t(
            &[5, 10, 15, 2, 4, 6, 1, 3, 7, 9, 11, 13, 17],
            &[0, 3, 5, 6, 7, 8, 10, 11],
        );
        let column_vth = make_column_with_intervals_t(
            &[
                1, 2, 20, 3, 4, 21, 5, 6, 22, 7, 8, 23, 9, 10, 24, 11, 12, 25, 13, 14,
            ],
            &[0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18],
        );
        let column_sth = make_column_with_intervals_t(
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

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![0, 3, 5],
        )))
        .unwrap();

        let proj_column_upper = trie_projected.get_column(0).as_u64().unwrap();
        let proj_column_middle = trie_projected.get_column(1).as_u64().unwrap();
        let proj_column_lower = trie_projected.get_column(2).as_u64().unwrap();
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
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

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

        let trie_reordered = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![2, 0, 1],
        )))
        .unwrap();

        log::debug!(
            "\n{}\n\n{}",
            trie.debug(&PrefixedStringDictionary::default()),
            trie_reordered.debug(&PrefixedStringDictionary::default())
        );

        assert_eq!(trie.row_num(), trie_reordered.row_num());

        let proj_column_upper = trie_reordered.get_column(0).as_u64().unwrap();
        let proj_column_middle = trie_reordered.get_column(1).as_u64().unwrap();
        let proj_column_lower = trie_reordered.get_column(2).as_u64().unwrap();
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
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

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

        let trie_reordered = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![2, 0],
        )))
        .unwrap();

        let proj_column_upper = trie_reordered.get_column(0).as_u64().unwrap();
        let proj_column_lower = trie_reordered.get_column(1).as_u64().unwrap();
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
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 4, 4, 5], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[4, 6, 6, 7, 3, 4, 3, 4], &[0, 2, 4, 6]);
        let column_fth =
            make_column_with_intervals_t(&[0, 0, 0, 0, 0, 0, 0, 0], &[0, 1, 2, 3, 4, 5, 6, 7]);
        let column_vth = make_column_with_intervals_t(
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

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![0, 2, 4, 1],
        )))
        .unwrap();

        let proj_column_fst = trie_projected.get_column(0).as_u64().unwrap();
        let proj_column_snd = trie_projected.get_column(1).as_u64().unwrap();
        let proj_column_trd = trie_projected.get_column(2).as_u64().unwrap();
        let proj_column_fth = trie_projected.get_column(3).as_u64().unwrap();
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

    #[test]
    fn complex_reorder() {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[10, 11, 9, 10], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[4, 5, 5, 6, 3, 4, 4, 5], &[0, 2, 4, 6]);
        let column_fth = make_column_with_intervals_t(
            &[6, 7, 6, 8, 7, 8, 6, 7, 7, 8, 6, 8, 5, 6, 7, 8],
            &[0, 2, 4, 6, 8, 10, 12, 14],
        );

        let column_vec = vec![column_fst, column_snd, column_trd, column_fth];

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

        let trie = Trie::new(schema, column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanProject(TrieScanProject::new(
            &trie,
            vec![2, 0, 3],
        )))
        .unwrap();

        let proj_column_fst = trie_projected.get_column(0).as_u64().unwrap();
        let proj_column_snd = trie_projected.get_column(1).as_u64().unwrap();
        let proj_column_trd = trie_projected.get_column(2).as_u64().unwrap();
        assert_eq!(
            proj_column_fst
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![3, 4, 5, 6]
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
            vec![2, 1, 2, 1, 2, 1]
        );

        assert_eq!(
            proj_column_snd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 1, 3, 5]
        );

        assert_eq!(
            proj_column_trd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![7, 8, 6, 7, 5, 6, 8, 6, 7, 8, 7, 8, 6, 7]
        );

        assert_eq!(
            proj_column_trd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 4, 7, 10, 12]
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

        let first = make_column_with_intervals_t(&fst, &[0]);
        let second = make_column_with_intervals_t(&snd, &[0]);
        let third = make_column_with_intervals_t(&trd, &[0, 1]);

        let columns = vec![first, second, third];

        let picked_columns = vec![1, 0, 2];
        let base_trie = Trie::new(schema, columns);
        let mut project =
            TrieScanEnum::TrieScanProject(TrieScanProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    /// This is another test case for a bug first encountered while
    /// classifying `smallmed` using the EL calculus rules, where
    /// reordering a table with 10 rows results in a table with 30 rows.
    #[test]
    fn spurious_tuples_in_reorder_mk2_bug() {
        let schema_entry = TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        };
        let schema = TrieSchema::new(vec![schema_entry, schema_entry, schema_entry]);

        let mut dict = PrefixedStringDictionary::default();
        let mut intern =
            |term: &str| DataValueT::U64(dict.add(term.to_owned()).try_into().unwrap());

        let a = intern("genid:cc18ce3a-be8a-3445-8b68-2027a2e1b1be");
        let b = intern("genid:0f18d187-7a4f-35c6-b645-c57ee51d277d");
        let c = intern("genid:c43dcdc4-5c45-307f-b734-76485822be3a");
        let d = intern("genid:8ab183c6-7491-3ae9-946c-f26084087292");
        let e = intern("genid:31241d92-1fbf-3bac-8027-82e028b346a6");
        let r = intern("i:RoleGroup");
        let u = intern("genid:cbf5c5b6-f56e-362d-a793-bcfd30c264f4");
        let v = intern("genid:6ecad60a-3cbd-3db0-b4c4-ee456ebcb64c");
        let w = intern("genid:ce19480b-4d0d-3bb0-b2e5-8fd877f29514");
        let x = intern("genid:69b6b533-19ac-35e1-9fe5-1fece1653d7b");
        let y = intern("genid:4034e8ac-9994-35bb-9836-ea484a2cc3d9");
        let z = intern("genid:7af2f7a6-3f3c-3694-83b8-b630f9e4c02a");

        let rows = vec![
            vec![a, r, u],
            vec![b, r, v],
            vec![b, r, w],
            vec![c, r, v],
            vec![c, r, w],
            vec![d, r, x],
            vec![d, r, v],
            vec![d, r, w],
            vec![d, r, y],
            vec![e, r, z],
        ];

        let picked_columns = vec![1, 0, 2];
        let base_trie = Trie::from_rows(schema, rows);

        let mut project =
            TrieScanEnum::TrieScanProject(TrieScanProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    /// This is derived from [`spurious_tuples_in_reorder_mk2_bug`],
    /// but minimised to a table of 2 rows resulting in four rows.
    #[test]
    fn spurious_tuples_in_reorder_mk2_minimised_bug() {
        let schema_entry = TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        };
        let schema = TrieSchema::new(vec![schema_entry, schema_entry, schema_entry]);

        let mut dict = PrefixedStringDictionary::default();
        let mut intern =
            |term: &str| DataValueT::U64(dict.add(term.to_owned()).try_into().unwrap());

        let a = intern("genid:cc18ce3a-be8a-3445-8b68-2027a2e1b1be");
        let b = intern("genid:0f18d187-7a4f-35c6-b645-c57ee51d277d");
        let r = intern("i:RoleGroup");
        let x = intern("genid:cbf5c5b6-f56e-362d-a793-bcfd30c264f4");
        let y = intern("genid:6ecad60a-3cbd-3db0-b4c4-ee456ebcb64c");

        let rows = vec![vec![a, r, x], vec![b, r, y]];

        let picked_columns = vec![1, 0, 2];
        let base_trie = Trie::from_rows(schema, rows);

        let mut project =
            TrieScanEnum::TrieScanProject(TrieScanProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    #[test]
    fn spurious_tuples_in_reorder_bug2() {
        let schema_entry = TrieSchemaEntry {
            label: 0,
            datatype: DataTypeName::U64,
        };
        let schema = TrieSchema::new(vec![schema_entry, schema_entry, schema_entry]);

        let mut dict = PrefixedStringDictionary::default();

        let rg = dict.add("RoleGroup".to_owned()).try_into().unwrap();
        let a = dict.add("4_1_6".to_owned()).try_into().unwrap();
        let b = dict.add("4_1_21".to_owned()).try_into().unwrap();
        let c = dict.add("4_1_22".to_owned()).try_into().unwrap();

        let u = dict.add("32_1_58".to_owned()).try_into().unwrap();
        let v = dict.add("32_1_72".to_owned()).try_into().unwrap();
        let w = dict.add("32_1_81".to_owned()).try_into().unwrap();
        let x = dict.add("32_1_74".to_owned()).try_into().unwrap();
        let y = dict.add("32_1_83".to_owned()).try_into().unwrap();
        let z = dict.add("32_1_60".to_owned()).try_into().unwrap();

        let fst = vec![a, b, c];
        let snd = vec![rg, rg, rg];
        let trd = vec![u, v, w, x, y, z];

        let first = make_column_with_intervals_t(&fst, &[0]);
        let second = make_column_with_intervals_t(&snd, &[0, 1, 2]);
        let third = make_column_with_intervals_t(&trd, &[0, 4, 5]);

        let columns = vec![first, second, third];

        let picked_columns = vec![1, 0, 2];
        let base_trie = Trie::new(schema, columns);
        let mut project =
            TrieScanEnum::TrieScanProject(TrieScanProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }
}
