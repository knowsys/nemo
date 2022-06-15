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
    let mut last_stable_input_index: Option<usize> = None;
    for var_index in 0..projected_vars.len() {
        let current_var = projected_vars[var_index];

        if var_index == 0 {
            if current_var == 0 {
                last_stable_input_index = Some(0);
            } else {
                last_stable_input_index = None;
                break;
            }
        } else {
            let prev_var = projected_vars[var_index - 1];

            if current_var - prev_var == 1 {
                last_stable_input_index = Some(var_index);
            } else {
                break;
            }
        }

        result_columns.push(input_trie.get_column(current_var).clone());
    }

    let mut data_column_builders = Vec::<AdaptiveColumnBuilderT>::new();
    let mut intervals_column_builders = Vec::<AdaptiveColumnBuilder<usize>>::new();

    for &var in &projected_vars[last_stable_input_index.map_or(0, |i| i)..] {
        intervals_column_builders.push(AdaptiveColumnBuilder::new());
        intervals_column_builders.last_mut().unwrap().add(0);
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

    if last_stable_input_index.is_none() {
        let starting_column_t = input_trie.get_column(projected_vars[0]);
        let starting_column: &IntervalColumnEnum<u64> =
            if let IntervalColumnT::U64(col) = starting_column_t {
                &col
            } else {
                panic!("Only covering u64 for now");
            };
        let range = 0..starting_column.len();

        let permutator =
            Permutator::sort_from_column_range(starting_column.get_data_column(), &range);

        permutator_stack.push(PermutatorStackElement {
            permutator: Some(permutator),
            value_pointer: 0,
            range: range,
        })
    } else {
        permutator_stack.push(PermutatorStackElement {
            permutator: None,
            value_pointer: 0,
            range: 0..input_trie
                .get_column(last_stable_input_index.unwrap())
                .len(),
        });
    }

    while permutator_stack.len() > 0 {
        let stack_index = permutator_stack.len() - 1;
        let build_index = if stack_index == 0 {
            0
        } else {
            stack_index - last_stable_input_index.map_or(0, |_| 1)
        };
        let add_to_builder = stack_index > 0 || last_stable_input_index.is_none();

        let output_column_index = last_stable_input_index.map_or(0, |i| i) + stack_index;
        let input_column_index = projected_vars[output_column_index];
        let mut current_element = &mut permutator_stack.last_mut().unwrap();
        let current_value_pointer = current_element.value_pointer;

        let current_read_column_t = input_trie.get_column(input_column_index);
        let current_read_column: &IntervalColumnEnum<u64> =
            if let IntervalColumnT::U64(col) = current_read_column_t {
                &col
            } else {
                panic!("Only covering u64 for now");
            };

        let mut current_data_builder_t = &mut data_column_builders[build_index];
        let current_data_builder: &mut AdaptiveColumnBuilder<u64> =
            if let AdaptiveColumnBuilderT::U64(cb) = &mut current_data_builder_t {
                cb
            } else {
                panic!("Only covering u64 for now");
            };
        let current_interval_builder = &mut intervals_column_builders[build_index];

        if current_value_pointer >= current_element.range.end {
            if add_to_builder && current_data_builder.count() < current_read_column.len() {
                current_interval_builder.add(current_data_builder.count());
            }

            permutator_stack.pop();
            continue;
        }

        let next_element_original_index: usize;

        if current_element.permutator.is_some() {
            let relative_pointer = current_value_pointer - current_element.range.start;
            let permutator = current_element.permutator.as_ref().unwrap();

            next_element_original_index = permutator.get_sort_vec()[relative_pointer];

            if add_to_builder {
                current_data_builder.add(current_read_column.get(next_element_original_index));
            }
        } else {
            if add_to_builder {
                current_data_builder.add(current_read_column.get(current_value_pointer));
            }
            next_element_original_index = current_value_pointer;
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

            let mut next_range = next_element_original_index..(next_element_original_index + 1);
            let mut is_sorted = true;
            for expand_index in (input_column_index + 1)..=next_input_index {
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

    for _ in last_stable_input_index.map_or(0, |_| 1)..data_column_builders.len() {
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

    Trie::new(target_schema, result_columns)
}

#[cfg(test)]
mod test {
    use super::super::trie::{Trie, TrieSchema, TrieSchemaEntry};
    use super::trie_project;
    use crate::physical::columns::{Column, IntervalColumnT};
    use crate::physical::datatypes::DataTypeName;
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

        let trie_no_first = trie_project(&trie, vec![1, 2]);
        let trie_no_middle = trie_project(&trie, vec![0, 2]);
        let trie_no_last = trie_project(&trie, vec![0, 1]);

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
}
