use crate::{
    columnar::column_builders::{
        columnbuilder::ColumnBuilder,
        columnbuilder_adaptive::{ColumnBuilderAdaptive, ColumnBuilderAdaptiveT},
    },
    datatypes::{StorageTypeName, StorageValueT},
    tabular::{
        table_types::trie::Trie,
        traits::{table::TableRow, trie_scan::TrieScan},
    },
};

/// Given a TrieScan iterator, materialize its content into a trie
/// For the last `cut` layers only checks the existence of a value and does not materialize it fully.
pub fn materialize_up_to(trie_scan: &mut impl TrieScan, cut: usize) -> Option<Trie> {
    if trie_scan.column_types().is_empty() {
        return None;
    }

    let num_columns = trie_scan.column_types().len() - cut;

    let mut data_column_builders: Vec<_> = trie_scan
        .column_types()
        .iter()
        .take(num_columns)
        .map(|column_type| {
            macro_rules! init_builder_for_datatype {
                ($variant:ident) => {{
                    ColumnBuilderAdaptiveT::$variant(ColumnBuilderAdaptive::default())
                }};
            }

            match column_type {
                StorageTypeName::Id32 => init_builder_for_datatype!(Id32),
                StorageTypeName::Id64 => init_builder_for_datatype!(Id64),
                StorageTypeName::Int64 => init_builder_for_datatype!(Int64),
                StorageTypeName::Float => init_builder_for_datatype!(Float),
                StorageTypeName::Double => init_builder_for_datatype!(Double),
            }
        })
        .collect();

    let mut intervals_column_builder = Vec::with_capacity(num_columns);
    intervals_column_builder.resize_with(num_columns, ColumnBuilderAdaptive::<usize>::default);

    let mut changed_layer = trie_scan.advance_on_layer(num_columns - 1)?;
    debug_assert_eq!(changed_layer, 0);
    intervals_column_builder[0].add(0);

    loop {
        for i in changed_layer..num_columns {
            if i < num_columns - 1 {
                intervals_column_builder[i + 1].add(data_column_builders[i + 1].count())
            }
            data_column_builders[i].add(trie_scan.current(i));
        }

        if let Some(next_changed_layer) = trie_scan.advance_on_layer(num_columns - 1) {
            changed_layer = next_changed_layer;
        } else {
            break;
        }
    }

    let columns = data_column_builders
        .into_iter()
        .zip(intervals_column_builder)
        .map(|(d, i)| d.finalize(i));

    Some(columns.collect())
}

/// Given a TrieScan iterator, materialize its content into a trie
pub fn materialize(trie_scan: &mut impl TrieScan) -> Option<Trie> {
    materialize_up_to(trie_scan, 0)
}

/// Tests whether an iterator is empty by materializing it until the first element.
/// Returns the first row of the result or `None` if it is empty.
pub fn scan_first_match(trie_scan: &mut impl TrieScan) -> Option<TableRow> {
    if trie_scan.column_types().is_empty() {
        return None;
    }

    trie_scan.advance_on_layer(trie_scan.column_types().len() - 1)?;

    let mut result = Vec::<StorageValueT>::new();
    for layer in 0..trie_scan.column_types().len() {
        result.push(trie_scan.current(layer));
    }

    Some(result)
}

#[cfg(test)]
mod test {
    use super::materialize;
    use crate::columnar::column_storage::column::Column;
    use crate::datatypes::StorageValueT;
    use crate::tabular::operations::materialize::scan_first_match;
    use crate::tabular::operations::{JoinBindings, TrieScanJoin, TrieScanPrune};
    use crate::tabular::table_types::trie::{Trie, TrieScanGeneric};
    use crate::tabular::traits::partial_trie_scan::TrieScanEnum;
    use crate::util::test_util::make_column_with_intervals_t;
    use test_log::test;

    #[test]
    fn complete() {
        let column_fst_data = [1, 2, 3];
        let column_fst_int = [0];
        let column_snd_data = [2, 3, 4, 1, 2];
        let column_snd_int = [0, 2, 3];
        let column_trd_data = [3, 4, 5, 7, 2, 1];
        let column_trd_int = [0, 2, 3, 4, 5];

        let column_fst = make_column_with_intervals_t(&column_fst_data, &column_fst_int);
        let column_snd = make_column_with_intervals_t(&column_snd_data, &column_snd_int);
        let column_trd = make_column_with_intervals_t(&column_trd_data, &column_trd_int);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);
        let trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let materialized_trie = materialize(&mut TrieScanPrune::new(trie_iter)).unwrap();

        let mat_in_col_fst = materialized_trie.get_column(0).as_u64().unwrap();
        let mat_in_col_snd = materialized_trie.get_column(1).as_u64().unwrap();
        let mat_in_col_trd = materialized_trie.get_column(2).as_u64().unwrap();

        assert_eq!(
            mat_in_col_fst
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            column_fst_data
        );
        assert_eq!(
            mat_in_col_fst
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            column_fst_int
        );
        assert_eq!(
            mat_in_col_snd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            column_snd_data
        );
        assert_eq!(
            mat_in_col_snd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            column_snd_int
        );
        assert_eq!(
            mat_in_col_trd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            column_trd_data
        );
        assert_eq!(
            mat_in_col_trd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            column_trd_int
        );
    }

    #[test]
    fn partial() {
        // Same setup as in test_trie_join
        let column_a_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_a_y = make_column_with_intervals_t(&[2, 3, 4, 5, 6, 7], &[0, 3, 4]);
        let column_b_y = make_column_with_intervals_t(&[1, 2, 3, 6], &[0]);
        let column_b_z = make_column_with_intervals_t(&[1, 8, 9, 10, 11, 12], &[0, 1, 3, 4]);

        let trie_a = Trie::new(vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(vec![column_b_y, column_b_z]);

        let join_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        ));

        let materialized_join = materialize(&mut TrieScanPrune::new(join_iter)).unwrap();

        let mat_in_col_fst = materialized_join.get_column(0).as_u64().unwrap();
        let mat_in_col_snd = materialized_join.get_column(1).as_u64().unwrap();
        let mat_in_col_trd = materialized_join.get_column(2).as_u64().unwrap();

        assert_eq!(
            mat_in_col_fst
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![1, 3]
        );
        assert_eq!(
            mat_in_col_fst
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0]
        );
        assert_eq!(
            mat_in_col_snd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![2, 3, 6]
        );
        assert_eq!(
            mat_in_col_snd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2]
        );
        assert_eq!(
            mat_in_col_trd
                .get_data_column()
                .iter()
                .collect::<Vec<u64>>(),
            vec![8, 9, 10, 11, 12]
        );
        assert_eq!(
            mat_in_col_trd
                .get_int_column()
                .iter()
                .collect::<Vec<usize>>(),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn trie_scan_empty() {
        let column_a_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_a_y = make_column_with_intervals_t(&[2, 3, 4, 5, 6, 7], &[0, 3, 4]);
        let column_b_y = make_column_with_intervals_t(&[1, 2, 3, 6], &[0]);
        let column_b_z = make_column_with_intervals_t(&[1, 8, 9, 10, 11, 12], &[0, 1, 3, 4]);

        let trie_a = Trie::new(vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(vec![column_b_y, column_b_z]);

        let join_iter = TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        );

        let first_result = scan_first_match(&mut TrieScanPrune::new(TrieScanEnum::TrieScanJoin(
            join_iter,
        )))
        .unwrap();

        let expected_result = vec![
            StorageValueT::Id64(1),
            StorageValueT::Id64(2),
            StorageValueT::Id64(8),
        ];

        assert_eq!(first_result, expected_result);
    }
}
