use std::time::Instant;

use nemo_physical::{
    datatypes::{StorageTypeName, StorageValueT},
    tabular::{
        rowscan::RowScan,
        triescan::{PartialTrieScan, TrieScan, TrieScanEnum},
    },
};
use streaming_iterator::StreamingIterator;

use crate::{
    gen_trie::{random_integer_trie, random_integer_trie_old},
    old::{self},
};

/// Return the current value of the [PartialTrieScan].
fn partial_scan_current<'a, Scan: PartialTrieScan<'a>>(
    scan: &mut Scan,
    storage_type: StorageTypeName,
) -> Option<StorageValueT> {
    let column_scan = unsafe { &mut *scan.current_scan()?.get() };

    column_scan.current(storage_type)
}

/// Move to the next value on the current layer of the [PartialTrieScan].
fn partial_scan_next<'a, Scan: PartialTrieScan<'a>>(
    scan: &Scan,
    storage_type: StorageTypeName,
) -> Option<StorageValueT> {
    let column_scan = unsafe { &mut *scan.current_scan()?.get() };

    column_scan.next(storage_type)
}

fn trie_dfs<'a, Scan: PartialTrieScan<'a>>(scan: &mut Scan) -> i64 {
    let mut result: i64 = 0;

    let possible_types = (0..scan.arity())
        .map(|layer| scan.possible_types(layer).storage_types())
        .collect::<Vec<_>>();

    // let possible_types = (0..scan.arity())
    //     .map(|layer| StorageTypeBitSet::full().storage_types())
    //     .collect::<Vec<_>>();

    let mut type_indices = Vec::with_capacity(scan.arity());
    type_indices.push(0);

    let first_type = possible_types[0][0];

    scan.down(first_type);

    while let Some(current_type) = scan.current_layer().map(|layer| scan.path_types()[layer]) {
        if let Some(next_value) = partial_scan_next(scan, current_type) {
            if let StorageValueT::Int64(value) = next_value {
                result += value;
            }

            if scan.current_layer().unwrap() < scan.arity() - 1 {
                type_indices.push(0);
                scan.down(possible_types[scan.current_layer().unwrap() + 1][0]);
            }
        } else {
            let current_layer = scan.current_layer().unwrap();

            let current_type_index = &mut type_indices[current_layer];
            *current_type_index += 1;

            scan.up();

            if let Some(next_type) = possible_types[current_layer].get(*current_type_index) {
                scan.down(*next_type);
            } else {
                type_indices.pop();
            }
        }
    }

    result
}

pub fn time_navigation_simple(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    for _ in 0..1 {
        let start_time = Instant::now();

        let mut iterator = trie.full_iterator();
        let mut sum: i64 = 0;
        while let Some(changed) = TrieScan::advance_on_layer(&mut iterator, arity - 1) {
            for layer in changed..arity {
                if let StorageValueT::Int64(value) = iterator.current_value(layer) {
                    sum += value;
                }
            }
        }

        let end_time = Instant::now();

        let duration = (end_time - start_time).as_millis();

        println!("New Time: {}ms, Sum: {}", duration, sum);
    }
}

pub fn time_navigation_row(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    for _ in 0..1 {
        let start_time = Instant::now();

        let iterator = RowScan::new(TrieScanEnum::TrieScanGeneric(trie.partial_iterator()), 0);
        let mut sum: i64 = 0;
        for row in iterator {
            for value in row {
                if let StorageValueT::Int64(value) = value {
                    sum += value;
                }
            }
        }

        let end_time = Instant::now();

        let duration = (end_time - start_time).as_millis();

        println!("Row Time: {}ms, Sum: {}", duration, sum);
    }
}

pub fn time_navigation_simple_dfs(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    let start_time = Instant::now();

    let mut iterator = trie.partial_iterator();
    // let possible_types = iterator.possible_types(0).storage_types();
    // let possible_types = [StorageTypeName::Int64];

    let sum = trie_dfs(&mut iterator);

    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();

    println!("Dfs Time: {}ms, Sum: {}", duration, sum);
}

pub fn time_navigation_simple_dfs_all(num_rows: usize, arity: usize) {
    // let trie = random_integer_trie(num_rows, arity);

    // let start_time = Instant::now();

    // let mut iterator = trie.partial_iterator();
    // let sum = trie_dfs(
    //     &mut iterator,
    //     &[
    //         StorageTypeName::Id32,
    //         StorageTypeName::Id64,
    //         StorageTypeName::Int64,
    //         StorageTypeName::Float,
    //         StorageTypeName::Double,
    //     ],
    // );

    // let end_time = Instant::now();

    // let duration = (end_time - start_time).as_millis();

    // println!("Dfs All Time: {}ms, Sum: {}", duration, sum);
}

pub fn time_navigation_simple_old(num_rows: usize, arity: usize) {
    let trie = random_integer_trie_old(num_rows, arity);

    for _ in 0..1 {
        let start_time = Instant::now();

        let mut iterator = trie.scan();
        let mut sum: i64 = 0;
        while let Some(changed) =
            old::trie_scan_prune::TrieScan::advance_on_layer(&mut iterator, arity - 1)
        {
            for layer in changed..arity {
                if let StorageValueT::Int64(value) =
                    old::trie_scan_prune::TrieScan::current(&mut iterator, layer)
                {
                    sum += value;
                }
            }
        }

        let end_time = Instant::now();

        let duration = (end_time - start_time).as_millis();

        println!("Old Time: {}ms, Sum: {}", duration, sum);
    }
}
