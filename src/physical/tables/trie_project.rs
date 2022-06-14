use super::{Table, TableSchema, Trie, TrieSchema, TrieSchemaEntry};
use crate::logical::Permutator;
use crate::physical::columns::{
    AdaptiveColumnBuilder, AdaptiveColumnBuilderT, Column, ColumnBuilder, GenericIntervalColumn,
    IntervalColumn, IntervalColumnEnum, IntervalColumnT,
};
use crate::physical::datatypes::DataTypeName;
use std::ops::Range;

fn expand_range(column: &IntervalColumnT, range: Range<usize>) -> Range<usize> {
    let start = column.int_bounds(range.start).start;
    let end = if range.end >= column.int_len() {
        column.len()
    } else {
        column.int_bounds(range.end).start
    };

    start..end
}

/// Given an input trie and an ordered list of variables to keep
/// compute a trie projected to the given variables
pub fn trie_project<'a>(input_trie: &Trie, projected_vars: Vec<usize>) -> Trie {
    debug_assert!(projected_vars.len() > 0);
    let input_schema = input_trie.schema();

    let mut target_attributes = Vec::<TrieSchemaEntry>::with_capacity(projected_vars.len());
    for &var in &projected_vars {
        target_attributes.push(TrieSchemaEntry {
            label: input_schema.get_label(var),
            datatype: input_schema.get_type(var),
        });
    }
    let target_schema = TrieSchema::new(target_attributes);

    let mut result_columns = Vec::<IntervalColumnT>::with_capacity(target_schema.arity());
    let mut last_stable_input_index: usize = 0;
    for var_index in 0..projected_vars.len() {
        let current_var = projected_vars[var_index];

        if var_index == 0 {
            if current_var == 0 {
                last_stable_input_index = 0;
            } else {
                last_stable_input_index = projected_vars[0];
                break;
            }
        } else {
            let prev_var = projected_vars[var_index - 1];

            if current_var - prev_var == 1 {
                result_columns.push(input_trie.get_column(prev_var).clone());
                last_stable_input_index = var_index;
            } else {
                break;
            }
        }
    }

    let mut data_column_builders = Vec::<AdaptiveColumnBuilderT>::new();
    let mut intervals_column_builders = Vec::<AdaptiveColumnBuilder<usize>>::new();

    for &var in &projected_vars {
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

    struct PermutatorStackElement {
        pub permutator: Option<Permutator>,
        pub value_pointer: usize,
        pub range: Range<usize>,
    }

    let mut permutator_stack = Vec::<PermutatorStackElement>::new();
    permutator_stack.push(PermutatorStackElement {
        permutator: None,
        value_pointer: 0,
        range: 0..input_trie.get_column(last_stable_input_index).len(),
    });

    while permutator_stack.len() > 0 {
        let stack_index = permutator_stack.len() - 1;
        let output_column_index = last_stable_input_index + stack_index;
        let input_column_index = projected_vars[stack_index];
        let mut current_element = &mut permutator_stack.last_mut().unwrap();

        let current_read_column_t = input_trie.get_column(input_column_index);
        let current_read_column: &IntervalColumnEnum<u64> =
            if let IntervalColumnT::U64(col) = current_read_column_t {
                &col
            } else {
                panic!("Only covering u64 for now");
            };

        let mut current_data_builder_t = &mut data_column_builders[stack_index];
        let current_data_builder: &mut AdaptiveColumnBuilder<u64> =
            if let AdaptiveColumnBuilderT::U64(cb) = &mut current_data_builder_t {
                cb
            } else {
                panic!("Only covering u64 for now");
            };
        let current_interval_builder = &mut intervals_column_builders[stack_index];

        if current_element.value_pointer >= current_element.range.end {
            if current_read_column.len() != current_element.range.end {
                current_interval_builder.add(current_element.range.end);
            }

            permutator_stack.pop();
            continue;
        }

        if current_element.permutator.is_some() {
            let relative_pointer = current_element.value_pointer - current_element.range.start;
            let permutator = current_element.permutator.as_ref().unwrap();

            current_data_builder
                .add(current_read_column.get(permutator.get_sort_vec()[relative_pointer]));
        } else {
            current_data_builder.add(current_read_column.get(current_element.value_pointer));
        }

        current_element.value_pointer += 1;

        if output_column_index < projected_vars.len() - 1 {
            let next_input_index = projected_vars[output_column_index + 1];
            let next_read_column_t = input_trie.get_column(next_input_index);
            let next_read_column: &IntervalColumnEnum<u64> =
                if let IntervalColumnT::U64(col) = next_read_column_t {
                    &col
                } else {
                    panic!("Only covering u64 for now");
                };

            let mut next_range = current_element.value_pointer..(current_element.value_pointer + 1);
            let mut is_sorted = true;
            for expand_index in (input_column_index + 1)..next_input_index {
                next_range = expand_range(input_trie.get_column(expand_index), next_range);
                if next_range.end - next_range.start > 1 && expand_index < next_input_index {
                    is_sorted = false;
                }
            }

            let next_permutator_opt = if is_sorted {
                None
            } else {
                let next_permutator = Permutator::sort_from_column_range(
                    next_read_column.get_data_column(),
                    &next_range,
                );
                Some(next_permutator)
            };

            permutator_stack.push(PermutatorStackElement {
                permutator: next_permutator_opt,
                value_pointer: next_range.start,
                range: next_range,
            });
        }
    }

    for _ in 0..data_column_builders.len() {
        let current_data_builder: AdaptiveColumnBuilder<u64> =
            if let AdaptiveColumnBuilderT::U64(cb) = data_column_builders.pop().unwrap() {
                cb
            } else {
                panic!("Only covering u64 for now");
            };
        let current_interval_builder = intervals_column_builders.pop().unwrap();

        let next_interval_column = IntervalColumnT::U64(IntervalColumnEnum::GenericIntervalColumn(
            GenericIntervalColumn::new(
                current_data_builder.finalize(),
                current_interval_builder.finalize(),
            ),
        ));

        result_columns.push(next_interval_column);
    }

    Trie::new(target_schema, result_columns)
}
