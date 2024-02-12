use std::time::Instant;

use nemo_physical::tabular::operations::{
    projectreorder::{GeneratorProjectReorder, ProjectReordering},
    OperationTableGenerator,
};

use crate::{
    gen_trie::{random_integer_trie, random_integer_trie_old},
    old::{project_reorder::project_and_reorder, trie::Table},
};

pub fn time_new_projectreorder(length: usize, output: Vec<&str>, input: Vec<&str>) {
    let trie = random_integer_trie(length, input.len());
    let trie_scan = trie.full_iterator();

    let mut marker_generator = OperationTableGenerator::new();
    for &marker in input.iter().chain(output.iter()) {
        marker_generator.add_marker(marker);
    }

    let markers_input = marker_generator.operation_table(input.iter());
    let markers_output = marker_generator.operation_table(output.iter());

    let project_generator = GeneratorProjectReorder::new(markers_output, markers_input);

    let start_time = Instant::now();
    let trie_result = project_generator.apply_operation(trie_scan);
    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();

    println!(
        "Project New: {}ms, ({} rows)",
        duration,
        trie_result.num_rows()
    );
}

pub fn time_old_proectreorder(length: usize, output: Vec<&str>, input: Vec<&str>) {
    let trie = random_integer_trie_old(length, input.len());
    let reordering = ProjectReordering::from_transformation(&input, &output);

    let start_time = Instant::now();
    let trie_result = project_and_reorder(&trie, &reordering);
    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();

    println!(
        "Project Old: {}ms, ({} rows)",
        duration,
        trie_result.row_num()
    );
}
