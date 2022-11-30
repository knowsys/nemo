use num::ToPrimitive;

use crate::physical::{
    columnar::{
        adaptive_column_builder::{ColumnBuilderAdaptive, ColumnBuilderAdaptiveT},
        column_types::interval::{ColumnWithIntervals, ColumnWithIntervalsT},
        traits::{columnbuilder::ColumnBuilder, columnscan::ColumnScan},
    },
    datatypes::{DataTypeName, Double, Float},
    tabular::{
        table_types::trie::Trie,
        traits::{
            table_schema::TableSchema,
            triescan::{TrieScan, TrieScanEnum},
        },
    },
};

/// Given a TrieScan iterator, materialize its content into a trie
/// If `picked_columns` is provided we will only store values for columns
/// for which the resepective entry in this vector is set to true
/// If `check_empty` is set to true it will only try to find the first entry and then abort
pub fn materialize_inner(
    trie_scan: &mut TrieScanEnum,
    picked_columns: Option<Vec<bool>>,
    check_empty: bool,
) -> Option<Trie> {
    // Keep track of the number of next calls; used for logging
    let mut next_count: usize = 0;

    // Clone the schema of the trie
    let schema = trie_scan.get_schema().clone();
    debug_assert!(schema.arity() > 0);

    // Setup column builders
    let mut result_columns = Vec::<ColumnWithIntervalsT>::with_capacity(schema.arity());
    let mut data_column_builders = Vec::<ColumnBuilderAdaptiveT>::new();
    let mut intervals_column_builders = Vec::<ColumnBuilderAdaptive<usize>>::new();

    for var in 0..schema.arity() {
        intervals_column_builders.push(ColumnBuilderAdaptive::default());

        macro_rules! init_builder_for_datatype {
            ($variant:ident) => {{
                data_column_builders.push(ColumnBuilderAdaptiveT::$variant(
                    ColumnBuilderAdaptive::default(),
                ))
            }};
        }

        match schema.get_type(var) {
            DataTypeName::U64 => init_builder_for_datatype!(U64),
            DataTypeName::Float => init_builder_for_datatype!(Float),
            DataTypeName::Double => init_builder_for_datatype!(Double),
        }
    }

    // Consider the path we currently span in the depth-first search.
    // current_row[i] = true means that for the ith value in the current path
    // we have already reached the bottom starting from that value (by an alternative path)
    // meaning that this value has to be added before leaving it
    let mut current_row: Vec<bool> = vec![false; schema.arity() - 1];

    // Contains for each layer the current number of entries in the data vector
    let mut current_num_elements: Vec<usize> = vec![0usize; schema.arity()];

    // Contains for each layer the number of entries in the data vector at the last time the interval column has been updated
    let mut prev_num_elements: Vec<usize> = vec![0usize; schema.arity()];

    // Current layer in the depth-first search
    let mut current_layer: usize = 0;

    // Whether the result contians at least one element
    let mut is_empty = true;

    // Iterate through the trie_scan in a depth-first-search manner
    trie_scan.down();
    loop {
        // It is important to know when we reached the bottom as only those values will be added to the result
        let is_last_layer = current_layer >= schema.arity() - 1;

        // In each loop iteration we perform a sideways step, then if not on the last layer go down
        // (unless a sideways step is impossible in which case we go up)
        // next_value is the value on which the down operation will be performed
        // current_value is the value we are leaving behind
        // If there exists a path from current_value to the bottom (which we keep track of with the current_row variable)
        // then it has to be added to a column builder
        let current_value = unsafe { (*trie_scan.current_scan().unwrap().get()).current() };
        let next_value = unsafe { (*trie_scan.current_scan().unwrap().get()).next() };
        next_count += 1;

        if let Some(val) = current_value {
            if is_last_layer || current_row[current_layer] {
                let is_picked = if let Some(picked_columns) = &picked_columns {
                    picked_columns[current_layer]
                } else {
                    true
                };

                current_num_elements[current_layer] += 1;

                if is_picked {
                    data_column_builders[current_layer].add(val);
                }
            }

            if !is_last_layer {
                current_row[current_layer] = false;
            }
        } else if is_last_layer && next_value.is_some() {
            current_row = vec![true; schema.arity() - 1];
            is_empty = false;

            // At this point we know that the result will contain at least one variable
            if check_empty {
                break;
            }
        }

        if next_value.is_none() {
            // Next value being None means that the currently built sorted interval of values is finished
            // and we therefore know the starting point of the next interval
            let current_data_len = current_num_elements[current_layer];
            let prev_data_len = &mut prev_num_elements[current_layer];

            // Check if new values have been added since last time
            if current_data_len > *prev_data_len {
                intervals_column_builders[current_layer].add(*prev_data_len);
                *prev_data_len = current_data_len;
            }

            // Since the next value is None we need to go up
            // If we are in the first layer then this means that the computation is finished
            if current_layer == 0 {
                break;
            }

            trie_scan.up();
            current_layer -= 1;

            // So that we don't go up and down in the same loop iteration
            continue;
        }

        if !is_last_layer {
            trie_scan.down();
            current_layer += 1;
        }
    }

    if !is_empty {
        // Collect data from column builders
        for column_index in 0..schema.arity() {
            macro_rules! finalize_for_datatype {
                ($variant:ident, $type:ty) => {{
                    let current_data_builder: ColumnBuilderAdaptive<$type> =
                        if let ColumnBuilderAdaptiveT::$variant(cb) = data_column_builders.remove(0)
                        {
                            cb
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($type));
                        };
                    let current_interval_builder = intervals_column_builders.remove(0);

                    let next_interval_column =
                        ColumnWithIntervalsT::$variant(ColumnWithIntervals::new(
                            current_data_builder.finalize(),
                            current_interval_builder.finalize(),
                        ));

                    result_columns.push(next_interval_column);
                }};
            }

            match schema.get_type(column_index) {
                DataTypeName::U64 => finalize_for_datatype!(U64, u64),
                DataTypeName::Float => finalize_for_datatype!(Float, Float),
                DataTypeName::Double => finalize_for_datatype!(Double, Double),
            }
        }

        let result = Trie::new(schema, result_columns);
        log::info!(
            "Materialize: Next: {next_count}, Elements: {}, Quotient: {}",
            result.num_elements(),
            next_count.to_f64().unwrap() / result.num_elements().to_f64().unwrap()
        );

        return Some(result);
    }

    None
}

/// Given a TrieScan iterator, materialize its content into a trie
pub fn materialize(trie_scan: &mut TrieScanEnum) -> Option<Trie> {
    materialize_inner(trie_scan, None, false)
}

/// Tests whether an iterator is empty by materializing it until the first element
pub fn scan_is_empty(trie_scan: &mut TrieScanEnum) -> bool {
    materialize_inner(trie_scan, None, true).is_some()
}

/// Given a TrieScan iterator, materialize its content into a trie
/// Setting picked_columns[i] to false means that the ith column will have an empty data vector
/// Passing None is equivalent to passing a vector containing only true
pub fn materialize_subset(trie_scan: &mut TrieScanEnum, picked_columns: Vec<bool>) -> Option<Trie> {
    materialize_inner(trie_scan, Some(picked_columns), false)
}

#[cfg(test)]
mod test {
    use super::materialize;
    use crate::physical::columnar::traits::column::Column;
    use crate::physical::datatypes::DataTypeName;
    use crate::physical::tabular::operations::TrieScanJoin;
    use crate::physical::tabular::table_types::trie::{
        Trie, TrieScanGeneric, TrieSchema, TrieSchemaEntry,
    };
    use crate::physical::tabular::traits::triescan::TrieScanEnum;
    use crate::physical::util::test_util::make_column_with_intervals_t;
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
        let mut trie_iter = TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie));

        let materialized_trie = materialize(&mut trie_iter).unwrap();

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

        let schema_a = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
        ]);
        let schema_b = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
        ]);

        let schema_target = TrieSchema::new(vec![
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

        let trie_a = Trie::new(schema_a, vec![column_a_x, column_a_y]);
        let trie_b = Trie::new(schema_b, vec![column_b_y, column_b_z]);

        let mut join_iter = TrieScanEnum::TrieScanJoin(TrieScanJoin::new(
            vec![
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_a)),
                TrieScanEnum::TrieScanGeneric(TrieScanGeneric::new(&trie_b)),
            ],
            &[vec![0, 1], vec![1, 2]],
            schema_target,
        ));

        let materialized_join = materialize(&mut join_iter).unwrap();

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

    // TODO: Tets scan_is_empty
}
