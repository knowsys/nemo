use std::{
    cell::RefCell,
    time::{Duration, Instant},
};

use bitvec::vec::BitVec;
use nemo_physical::{
    datasources::tuple_writer::TupleWriter,
    datavalues::AnyDataValue,
    dictionary::meta_dv_dict::MetaDvDictionary,
    function::tree::FunctionTree,
    management::{
        database::DatabaseInstance,
        execution_plan::{ColumnOrder, ExecutionPlan},
    },
    tabular::{
        operations::{function::FunctionAssignment, OperationTableGenerator},
        trie::Trie,
    },
};
use rand::Rng;

fn random_number_vec(length: usize) -> Vec<i64> {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| rng.gen_range(1..=(length as i64)))
        .collect()
}

fn random_bitvector(length: usize) -> Vec<bool> {
    let mut rng = rand::thread_rng();
    (0..length).map(|_| rng.gen_range(1..100) > 50).collect()
}

fn random_trie(length: usize, arity: usize) -> Trie {
    let mut dicationary = RefCell::new(MetaDvDictionary::new());
    let mut tuple_writer = TupleWriter::new(&dicationary, arity);

    let mut rng = rand::thread_rng();
    for _ in 0..(arity * length) {
        tuple_writer.add_tuple_value(AnyDataValue::new_integer_from_i64(
            rng.gen_range(1..(length as i64)),
        ));
    }

    Trie::from_tuple_writer(tuple_writer)
}

fn bitvec_from_bitvector(vec: &[bool]) -> BitVec {
    let mut result = BitVec::new();
    for value in vec {
        result.push(*value);
    }

    result
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
    let trie = random_trie(length, 2);
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

fn time_vector() {
    let duration_vector_addition = time_vector_addition(10_000_000);
    let duration_vector_filter = time_vector_filter(10_000_000);

    println!(
        "Vector addition: {} ms",
        duration_vector_addition.as_millis()
    );
    println!("Vector filter: {} ms", duration_vector_filter.as_millis());
}

fn time_nemo() {
    let duration_nemo_addition = time_nemo_addition(10_000_000);
    println!("Nemo addition: {} ms", duration_nemo_addition.as_millis());
}

fn bitvector_copy_and_check(copy: Vec<bool>) -> usize {
    let mut result: usize = 0;
    for value in copy {
        if value {
            result += 1;
        }
    }

    result
}

fn bitvec_copy_and_check(copy: BitVec) -> usize {
    let mut result: usize = 0;
    for value in copy {
        if value {
            result += 1;
        }
    }

    result
}

fn time_bitvec(operation_count: usize, bitvec_length: usize) {
    let bitvector = random_bitvector(bitvec_length);
    let bitvec = bitvec_from_bitvector(&bitvector);

    let start_time = Instant::now();
    let mut latest: usize = 0;
    for _ in 0..operation_count {
        latest = bitvector_copy_and_check(bitvector.clone());
    }
    let end_time = Instant::now();
    let duration = end_time - start_time;

    println!("latest: {latest}");
    println!("Bitvector: {} ms", duration.as_millis());

    let start_time = Instant::now();
    for _ in 0..operation_count {
        latest = bitvec_copy_and_check(bitvec.clone());
    }
    let end_time = Instant::now();
    let duration = end_time - start_time;

    println!("latest: {latest}");
    println!("Bitvec: {} ms", duration.as_millis());
}

fn main() {
    // time_bitvec(1_000_000, 6);
    time_vector()
    // time_nemo();
}
