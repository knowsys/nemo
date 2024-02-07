use std::time::{Duration, Instant};

use nemo_physical::{
    function::tree::FunctionTree,
    management::{
        database::DatabaseInstance,
        execution_plan::{ColumnOrder, ExecutionPlan},
    },
    tabular::operations::{function::FunctionAssignment, OperationTableGenerator},
};
use rand::Rng;

use crate::gen_trie::random_integer_trie;

fn random_number_vec(length: usize) -> Vec<i64> {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| rng.gen_range(1..=(length as i64)))
        .collect()
}

fn time_vector_addition(length: usize) -> Duration {
    let list_a = random_number_vec(length);
    let list_b = random_number_vec(length);

    let start_time = Instant::now();

    let mut latest: i64 = 0;
    for (a, b) in list_a.into_iter().zip(list_b.into_iter()) {
        latest = a + b;
    }
    let end_time = Instant::now();

    println!("latest: {}", latest);

    end_time - start_time
}

fn time_vector_filter(length: usize) -> Duration {
    let list = random_number_vec(length);
    let constant = rand::thread_rng().gen_range(1..=(length as i64));

    let start_time = Instant::now();

    let mut seen = false;
    for x in list.into_iter() {
        if x == constant {
            seen = true
        }
    }

    let end_time = Instant::now();

    println!("seen: {}", seen);

    end_time - start_time
}

fn time_nemo_addition(length: usize) -> Duration {
    let mut database = DatabaseInstance::default();
    let trie = random_integer_trie(length, 2);
    let id = database.register_add_trie("Trie", ColumnOrder::default(), trie);

    let mut marker_generator = OperationTableGenerator::new();
    marker_generator.add_marker("a");
    marker_generator.add_marker("b");
    marker_generator.add_marker("r");

    let markers_result = marker_generator.operation_table(["a", "b", "r"].iter());
    let markers = marker_generator.operation_table(["a", "b"].iter());

    let mut function = FunctionAssignment::new();
    function.insert(
        *marker_generator.get(&"r").unwrap(),
        FunctionTree::numeric_addition(
            FunctionTree::reference(*marker_generator.get(&"a").unwrap()),
            FunctionTree::reference(*marker_generator.get(&"b").unwrap()),
        ),
    );

    let mut plan = ExecutionPlan::default();
    let load_table = plan.fetch_table(markers, id);
    let node_result = plan.function(markers_result, load_table, function);
    plan.write_permanent(node_result, "Addition", "Result");

    let start_time = Instant::now();
    let result = database.execute_plan(plan).unwrap();
    let end_time = Instant::now();

    println!("Result: {}", result.len());

    end_time - start_time
}

fn time_vector(length: usize) {
    let duration_vector_addition = time_vector_addition(length);
    let duration_vector_filter = time_vector_filter(length);

    println!(
        "Vector addition: {} ms",
        duration_vector_addition.as_millis()
    );
    println!("Vector filter: {} ms", duration_vector_filter.as_millis());
}

fn time_nemo(length: usize) {
    let duration_nemo_addition = time_nemo_addition(length);
    println!("Nemo addition: {} ms", duration_nemo_addition.as_millis());
}

pub fn time_addition(length: usize) {
    time_vector(length);
    time_nemo(length);
}
