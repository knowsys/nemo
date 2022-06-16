use super::{TableSchema, Trie, TrieScan, TrieScanEnum, TrieSchema, TrieSchemaEntry};
use crate::physical::columns::{
    AdaptiveColumnBuilder, AdaptiveColumnBuilderT, ColumnBuilder, ColumnScan,
    GenericIntervalColumn, IntervalColumnEnum, IntervalColumnT,
};
use crate::physical::datatypes::{DataTypeName, DataValueT};

/// Given a TrieScan iterator, materialize its content into a trie
pub fn materialize(trie_scan: &mut TrieScanEnum) -> Trie {
    // Compute target schema (which is the same as the input schema...)
    // TODO: There should be a better way to clone something like this...
    let input_schema = trie_scan.get_schema();
    let mut target_attributes = Vec::<TrieSchemaEntry>::with_capacity(input_schema.arity());
    for var in 0..input_schema.arity() {
        target_attributes.push(TrieSchemaEntry {
            label: input_schema.get_label(var),
            datatype: input_schema.get_type(var),
        });
    }
    let target_schema = TrieSchema::new(target_attributes);

    // Setup column builders
    let mut result_columns = Vec::<IntervalColumnT>::with_capacity(target_schema.arity());
    let mut data_column_builders = Vec::<AdaptiveColumnBuilderT>::new();
    let mut intervals_column_builders = Vec::<AdaptiveColumnBuilder<usize>>::new();

    for var in 0..target_schema.arity() {
        intervals_column_builders.push(AdaptiveColumnBuilder::new());
        match input_schema.get_type(var) {
            DataTypeName::U64 => {
                data_column_builders.push(AdaptiveColumnBuilderT::U64(AdaptiveColumnBuilder::new()))
            }
            DataTypeName::Float => data_column_builders
                .push(AdaptiveColumnBuilderT::Float(AdaptiveColumnBuilder::new())),
            DataTypeName::Double => data_column_builders
                .push(AdaptiveColumnBuilderT::Double(AdaptiveColumnBuilder::new())),
        }
    }

    // Iterate through the trie_scan in a dfs manner
    let mut current_row = Vec::<DataValueT>::with_capacity(target_schema.arity());
    let mut current_int_starts: Vec<usize> = vec![0usize; target_schema.arity()];
    let mut current_layer: usize = 0;
    trie_scan.down();
    loop {
        let is_last_layer = current_layer < target_schema.arity() - 1;
        let is_first_value = trie_scan.current_scan().unwrap().current().is_none();
        let next_value = trie_scan.current_scan().unwrap().next();

        if next_value.is_some() {
            if is_last_layer {
                if is_first_value {
                    for var in 0..(target_schema.arity() - 1) {
                        data_column_builders[var].add(current_row[var]);
                    }
                }

                data_column_builders
                    .last_mut()
                    .unwrap()
                    .add(next_value.unwrap());
            } else {
                current_row[current_layer] = next_value.unwrap();
            }
        } else {
            if current_layer == 0 {
                break;
            }

            let current_data_len = data_column_builders[current_layer].count();
            let prev_data_len = &mut current_int_starts[current_layer];

            if current_data_len > *prev_data_len {
                intervals_column_builders[current_layer].add(*prev_data_len);
                *prev_data_len = current_data_len;
            }

            trie_scan.up();
            current_layer -= 1;
            continue;
        }

        if !is_last_layer {
            trie_scan.down();
            current_layer += 1;
        }
    }

    // Collect data from column builders
    for _ in 0..target_schema.arity() {
        let current_data_builder: AdaptiveColumnBuilder<u64> =
            if let AdaptiveColumnBuilderT::U64(cb) = data_column_builders.remove(0) {
                cb
            } else {
                panic!("Only covering u64 for now");
            };
        let current_interval_builder = intervals_column_builders.remove(0);

        let next_interval_column = IntervalColumnT::U64(IntervalColumnEnum::GenericIntervalColumn(
            GenericIntervalColumn::new(
                current_data_builder.finalize(),
                current_interval_builder.finalize(),
            ),
        ));

        result_columns.push(next_interval_column);
    }

    // Finally, return finished trie
    Trie::new(target_schema, result_columns)
}
