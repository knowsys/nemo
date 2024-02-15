use std::{collections::HashMap, time::Instant};

use nemo_physical::{
    datatypes::{StorageTypeName, StorageValueT},
    tabular::triescan::{PartialTrieScan, TrieScan},
};

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

fn trie_dfs<'a, Scan: PartialTrieScan<'a>>(scan: &mut Scan, types: &[StorageTypeName]) -> i64 {
    let mut result: i64 = 0;

    let mut next_type_map = HashMap::<StorageTypeName, StorageTypeName>::new();
    for adjacent_types in types.windows(2) {
        next_type_map.insert(adjacent_types[0].clone(), adjacent_types[1].clone());
    }

    let first_type = types.first().unwrap().clone();
    scan.down(first_type);

    while let Some(current_type) = scan.current_layer().map(|layer| scan.path_types()[layer]) {
        if let Some(next_value) = partial_scan_next(scan, current_type) {
            if let StorageValueT::Int64(value) = next_value {
                result += value;
            }

            if scan.current_layer().unwrap() < scan.arity() - 1 {
                scan.down(first_type);
            }
        } else {
            scan.up();

            if let Some(next_type) = next_type_map.get(&current_type).cloned() {
                scan.down(next_type);
            }
        }
    }

    result
}

pub fn time_navigation_simple(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    for _ in 0..100 {
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

pub fn time_navigation_simple_dfs(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    let start_time = Instant::now();

    let mut iterator = trie.partial_iterator();
    let sum = trie_dfs(&mut iterator, &[StorageTypeName::Int64]);

    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();

    println!("Dfs Time: {}ms, Sum: {}", duration, sum);
}

pub fn time_navigation_simple_dfs_all(num_rows: usize, arity: usize) {
    let trie = random_integer_trie(num_rows, arity);

    let start_time = Instant::now();

    let mut iterator = trie.partial_iterator();
    let sum = trie_dfs(
        &mut iterator,
        &[
            StorageTypeName::Id32,
            StorageTypeName::Id64,
            StorageTypeName::Int64,
            StorageTypeName::Float,
            StorageTypeName::Double,
        ],
    );

    let end_time = Instant::now();

    let duration = (end_time - start_time).as_millis();

    println!("Dfs All Time: {}ms, Sum: {}", duration, sum);
}

pub fn time_navigation_simple_old(num_rows: usize, arity: usize) {
    let trie = random_integer_trie_old(num_rows, arity);

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
