use crate::physical::{
    columnar::{
        column_types::interval::ColumnWithIntervalsT,
        operations::columnscan_multiple_ranges::ColumnScanMultipleRanges,
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum, ColumnScanT},
    },
    datatypes::DataTypeName,
    tabular::traits::{table::Table, triescan::TrieScan},
    tabular::{table_types::trie::Trie, traits::table_schema::TableColumnTypes},
    util::Reordering,
};

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::Range;


/// Iterator which can reorder and project away colums of a trie
#[derive(Debug)]
pub struct TrieScanReorderProject<'a> {
    trie: &'a Trie,
    column_reordering: Reordering,
    data_types: TableColumnTypes,
    current_layer: Option<usize>,
    deepest_layer: Option<usize>,
    trie_scans: Vec<UnsafeCell<ColumnScanT<'a>>>,
}

fn get_pos_towards_root(trie: &Trie, layer: usize, upwards_layer: usize, pos: usize) -> usize {
    (upwards_layer..layer).rev().fold(pos, |mut_pos, next| {
        // println!("move up: {}@{}", mut_pos, next + 1);
        let go_to = trie.get_column(next + 1).int_idx(mut_pos).unwrap();
        // println!("...to: {}@{}", go_to, next);
        go_to
    })
}

impl<'a> TrieScanReorderProject<'a> {
    /// Create new TrieScanReorderProject object
    pub fn new(trie: &'a Trie, column_reordering: Reordering) -> Self {
        let input_types = trie.get_types();

        let mut trie_scans =
            Vec::<UnsafeCell<ColumnScanT>>::with_capacity(column_reordering.len_target());


        let mut target_types = Vec::<DataTypeName>::with_capacity(column_reordering.len_target());
        for &col_index in column_reordering.iter() {
            let current_datatype = input_types[col_index];

            target_types.push(current_datatype);

            macro_rules! init_scans_for_datatype {
                ($variant:ident) => {{
                    let current_column =
                        if let ColumnWithIntervalsT::$variant(col) = trie.get_column(col_index) {
                            col
                        } else {
                            panic!("Do other cases later")
                        };

                    trie_scans.push(UnsafeCell::new(ColumnScanT::$variant(
                        ColumnScanCell::new(ColumnScanEnum::ColumnScanMultipleRanges(
                            ColumnScanMultipleRanges::new(current_column.get_data_column(), vec![]),
                        )),
                    )));
                }};
            }

            match current_datatype {
                DataTypeName::U32 => init_scans_for_datatype!(U32),
                DataTypeName::U64 => init_scans_for_datatype!(U64),
                DataTypeName::Float => init_scans_for_datatype!(Float),
                DataTypeName::Double => init_scans_for_datatype!(Double),
            }
        }

        println!("{:?}", column_reordering);

        Self {
            trie,
            column_reordering,
            current_layer: None,
            deepest_layer: None,
            data_types: target_types,
            trie_scans
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanReorderProject<'a> {
    fn up(&mut self) {
        println!("Top-level up");
        debug_assert!(self.current_layer.is_some());
        let up_layer = if self.current_layer.unwrap() == 0 {
            None
        } else {
            Some(self.current_layer.unwrap() - 1)
        };

        let deepest_layer = if up_layer.is_none() {
            None
        } else {
            let deepest_underlying_layer = (0..up_layer.unwrap() + 1).map(|layer| self.column_reordering[layer]).max().unwrap();
            self.column_reordering.iter().position(|layer| *layer == deepest_underlying_layer)
        };

        self.trie_scans[self.current_layer.unwrap()].get_mut().reset();
        self.current_layer = up_layer;
        self.deepest_layer = deepest_layer;
    }

    fn down(&mut self) {
        println!("Top-level down");
        let down_layer = self.current_layer.map_or(0, |v| v + 1);
        debug_assert!(down_layer < self.data_types.len());

        let ranges: Vec<Range<usize>>;
        if down_layer == 0 {
            // the first layer uses all ranges of the target layer in underlying trie
            let column = self.trie.get_column(self.column_reordering[0]);
            ranges = (0..column.int_len())
                .map(|int_idx| column.int_bounds(int_idx))
                .collect();

            /*
            ranges = Vec<Range<usize>>::with_capacity(column.int_len());
            let mut start: Option<usize> = None;
            for idx in 0..intervals. {
                start = match start {
                    None => Some(column.get_int_column().get(idx)),
                    Some<int_start> => {
                        let int_next = column.get_int_column().get(idx);
                        ranges.push(int_start..int_next-1);
                        Some(int_next)
                    }
                };
            }
            ranges.push(start.unwrap()..column.len()-1);
            */

        } else if self.column_reordering[down_layer] < self.column_reordering[self.current_layer.unwrap()] {
            println!("up");
            // towards the root in underlying trie
            ranges = self.trie_scans[self.current_layer.unwrap()]
                .get_mut().pos_multiple().unwrap().iter() // borrow suffices probably
                .map(|pos| get_pos_towards_root(
                    self.trie,
                    self.column_reordering[self.current_layer.unwrap()],
                    self.column_reordering[down_layer],
                    *pos))
                .map(|pos| pos..pos+1)
                // .map(|pos| {
                    //let mut layer = self.column_reordering[self.current_layer.unwrap()];
                    //let mut mut_pos = *pos;
                    //while layer > self.column_reordering[down_layer] {
                        //mut_pos = self.trie.get_column(layer).int_idx(mut_pos).unwrap();
                        // layer -= 1;
                        // let mut int_iter = self.trie.get_column(layer).get_int_column().iter();
                        // mut_pos = match int_iter.seek(mut_pos) {
                            // Some(idx) => if idx == mut_pos { idx } else { idx - 1 },
                            // None => self.trie.get_column(layer).int_len() - 1
                        // };
                    // }
                    // mut_pos..mut_pos+1
                // })
                .collect();

        } else {
            // towards the leafs in underlying trie
            if self.deepest_layer.unwrap() < self.current_layer.unwrap() && self.column_reordering[down_layer] < self.column_reordering[self.deepest_layer.unwrap()] {
                println!("down - in-between");
                ranges = self.trie_scans[self.deepest_layer.unwrap()]
                    .get_mut().pos_multiple().unwrap().iter() // borrow suffices probably
                    .map(|pos| get_pos_towards_root(
                        self.trie,
                        self.column_reordering[self.deepest_layer.unwrap()],
                        self.column_reordering[down_layer],
                        *pos))
                    .map(|pos| pos..pos+1)
                    .collect();

            } else if self.deepest_layer.unwrap() < self.current_layer.unwrap() {
                println!("down after up");
                let active_positions = self.trie_scans[self.current_layer.unwrap()].get_mut().pos_multiple().unwrap();
                ranges = self.trie_scans[self.deepest_layer.unwrap()]
                    .get_mut().pos_multiple().expect("Well...").iter() // borrow suffices probably
                    .filter(|pos| active_positions.contains(&get_pos_towards_root(
                        self.trie,
                        self.column_reordering[self.deepest_layer.unwrap()],
                        self.column_reordering[self.current_layer.unwrap()],
                    **pos)))
                    .flat_map(|pos| {
                        let mut layer = self.column_reordering[self.deepest_layer.unwrap()];
                        let mut lower_bound = *pos;
                        let mut upper_bound = *pos;

                        while layer < self.column_reordering[down_layer] {
                            layer += 1;
                            lower_bound = self.trie.get_column(layer).int_bounds(lower_bound).min().expect("Intervals are non empty");
                            upper_bound = self.trie.get_column(layer).int_bounds(upper_bound).max().expect("Intervals are non empty");
                        }

                        let column = self.trie.get_column(layer);
                        (column.int_idx(lower_bound).unwrap()..column.int_idx(upper_bound).unwrap()+1)
                            .map(|int_idx| column.int_bounds(int_idx))
                    })
                    .collect();
                println!("{:?}", ranges);

            } else {
                println!("further down");
                ranges = self.trie_scans[self.current_layer.unwrap()]
                    .get_mut().pos_multiple().unwrap().iter() // borrow suffices probably
                    .flat_map(|pos| {
                        let mut layer = self.column_reordering[self.current_layer.unwrap()];
                        let mut lower_bound = *pos;
                        let mut upper_bound = *pos;
 
                        while layer < self.column_reordering[down_layer] {
                            layer += 1;
                            lower_bound = self.trie.get_column(layer).int_bounds(lower_bound).min().expect("Intervals are non empty");
                            upper_bound = self.trie.get_column(layer).int_bounds(upper_bound).max().expect("Intervals are non empty");
                        }
 
                        let column = self.trie.get_column(layer);
                        (column.int_idx(lower_bound).unwrap()..column.int_idx(upper_bound).unwrap()+1)
                            .map(|int_idx| column.int_bounds(int_idx))
                    })
                    .collect();
            }
        }

        self.trie_scans[down_layer].get_mut().narrow_ranges(ranges);
        self.current_layer = Some(down_layer);

        if self.deepest_layer.is_none() || self.column_reordering[self.deepest_layer.unwrap()] < self.column_reordering[down_layer] {
            self.deepest_layer = self.current_layer;
        }
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        debug_assert!(self.current_layer.is_some());

        Some(&self.trie_scans[self.current_layer?])
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.trie_scans[index])
    }

    fn get_types(&self) -> &TableColumnTypes {
        &self.data_types
    }
}

#[cfg(test)]
mod test {
    use super::TrieScanReorderProject;
    use crate::physical::columnar::traits::column::Column;
    use crate::physical::datatypes::DataValueT;
    use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};
    use crate::physical::tabular::operations::materialize;
    use crate::physical::tabular::table_types::trie::Trie;
    use crate::physical::tabular::traits::{table::Table, triescan::TrieScanEnum};
    use crate::physical::util::test_util::make_column_with_intervals_t;
    use crate::physical::util::Reordering;
    use test_log::test;

    #[test]
    fn single_small_hole() {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[3, 5, 2, 4], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 9, 5, 8, 4, 6, 2, 3], &[0, 2, 4, 6]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);

        let trie_no_first = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![1, 2], 3),
        )))
        .unwrap();
        let trie_no_middle = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![0, 2], 3),
        )))
        .unwrap();
        let trie_no_last = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![0, 1], 3),
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

        let trie = Trie::new(column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![0, 3, 5], 6),
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

        let trie = Trie::new(column_vec);

        let trie_reordered = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![2, 0, 1], 3),
        )))
        .unwrap();

        log::debug!(
            "\n{}\n\n{}",
            trie.debug(&PrefixedStringDictionary::default()),
            trie_reordered.debug(&PrefixedStringDictionary::default())
        );

        assert_eq!(trie.row_num(), trie_reordered.row_num());

        let proj_column_upper = trie_reordered.get_column(0).as_u64().unwrap();
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

        let proj_column_middle = trie_reordered.get_column(1).as_u64().unwrap();
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

        let proj_column_lower = trie_reordered.get_column(2).as_u64().unwrap();
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

        let trie = Trie::new(column_vec);

        let trie_reordered = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![2, 0], 3),
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

        let trie = Trie::new(column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![0, 2, 4, 1], 5),
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

        let trie = Trie::new(column_vec);

        let trie_projected = materialize(&mut TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(
            &trie,
            Reordering::new(vec![2, 0, 3], 4),
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

        let picked_columns = Reordering::new(vec![1, 0, 2], 3);
        let base_trie = Trie::new(columns);
        let mut project =
            TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    /// This is another test case for a bug first encountered while
    /// classifying `smallmed` using the EL calculus rules, where
    /// reordering a table with 10 rows results in a table with 30 rows.
    #[test]
    fn spurious_tuples_in_reorder_mk2_bug() {
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

        let picked_columns = Reordering::new(vec![1, 0, 2], 3);
        let base_trie = Trie::from_rows(rows);

        let mut project =
            TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    /// This is derived from [`spurious_tuples_in_reorder_mk2_bug`],
    /// but minimised to a table of 2 rows resulting in four rows.
    #[test]
    fn spurious_tuples_in_reorder_mk2_minimised_bug() {
        let mut dict = PrefixedStringDictionary::default();
        let mut intern =
            |term: &str| DataValueT::U64(dict.add(term.to_owned()).try_into().unwrap());

        let a = intern("genid:cc18ce3a-be8a-3445-8b68-2027a2e1b1be");
        let b = intern("genid:0f18d187-7a4f-35c6-b645-c57ee51d277d");
        let r = intern("i:RoleGroup");
        let x = intern("genid:cbf5c5b6-f56e-362d-a793-bcfd30c264f4");
        let y = intern("genid:6ecad60a-3cbd-3db0-b4c4-ee456ebcb64c");

        let rows = vec![vec![a, r, x], vec![b, r, y]];

        let picked_columns = Reordering::new(vec![1, 0, 2], 3);
        let base_trie = Trie::from_rows(rows);

        let mut project =
            TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }

    #[test]
    fn spurious_tuples_in_reorder_bug2() {
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

        let picked_columns = Reordering::new(vec![1, 0, 2], 3);
        let base_trie = Trie::new(columns);
        let mut project =
            TrieScanEnum::TrieScanReorderProject(TrieScanReorderProject::new(&base_trie, picked_columns));

        let reordered_trie = materialize(&mut project).unwrap();

        assert_eq!(base_trie.row_num(), reordered_trie.row_num());
    }
}
