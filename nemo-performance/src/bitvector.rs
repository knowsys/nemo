use std::time::Instant;

use bitvec::vec::BitVec;
use rand::Rng;

fn random_bitvector(length: usize) -> Vec<bool> {
    let mut rng = rand::thread_rng();
    (0..length).map(|_| rng.gen_range(1..100) > 50).collect()
}

fn bitvec_from_bitvector(vec: &[bool]) -> BitVec {
    let mut result = BitVec::new();
    for value in vec {
        result.push(*value);
    }

    result
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

pub fn time_bitvec(operation_count: usize, bitvec_length: usize) {
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
